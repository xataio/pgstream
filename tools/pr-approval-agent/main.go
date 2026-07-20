// SPDX-License-Identifier: Apache-2.0

// Command pr-approval-agent is a lean, Go-native reimplementation of PostHog's
// stamphog PR reviewer, retuned for pgstream.
//
// Pipeline: fetch PR -> deterministic gates -> (conditionally) LLM review -> post
// verdict. Gates are authoritative; the LLM can tighten but never loosen them.
//
// Modes:
//
//	review   (default) classify + gate + review, then post the verdict.
//	dismiss  on a new push, dismiss a stale bot approval unless the delta is trivial.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Trigger label and bot identity. Override via flags when the app is named
// differently. The bot login is the GitHub App slug suffixed with "[bot]".
const (
	defaultLabel    = "ai-review"
	defaultBotLogin = "xata-bot"
	diffMaxBytes    = 200_000
)

type options struct {
	prNumber   int
	repo       string
	mode       string
	label      string
	botLogin   string
	repoRoot   string
	agentDir   string
	dryRun     bool
	noPost     bool
	outputJSON string
	verbose    bool
}

func main() {
	os.Exit(run())
}

func run() int {
	exe, _ := os.Executable()
	defaultAgentDir := filepath.Dir(exe)

	var o options
	flag.StringVar(&o.repo, "repo", "xataio/pgstream", "target repository (owner/name)")
	flag.StringVar(&o.mode, "mode", "review", "review | dismiss")
	flag.StringVar(&o.label, "label", defaultLabel, "trigger label to remove on non-approval")
	flag.StringVar(&o.botLogin, "bot-login", defaultBotLogin, "login of the agent bot")
	flag.StringVar(&o.repoRoot, "repo-root", ".", "path to the PR's checked-out code the reviewer reads")
	flag.StringVar(&o.agentDir, "agent-dir", defaultAgentDir, "directory holding review-guidance.md")
	flag.BoolVar(&o.dryRun, "dry-run", false, "gates only: no LLM, no posting")
	flag.BoolVar(&o.noPost, "no-post", false, "compute the verdict but do not touch the PR")
	flag.StringVar(&o.outputJSON, "output-json", "", "write the full result JSON to this path")
	flag.BoolVar(&o.verbose, "v", false, "log agent tool calls to stderr")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: pr-approval-agent [flags] <pr-number>")
		return 2
	}
	n, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid PR number %q\n", flag.Arg(0))
		return 2
	}
	o.prNumber = n

	var result map[string]any
	switch o.mode {
	case "dismiss":
		result, err = runDismiss(o)
	default:
		result, err = runReview(context.Background(), o)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	payload, _ := json.MarshalIndent(result, "", "  ")
	if o.outputJSON != "" {
		if werr := os.WriteFile(o.outputJSON, payload, 0o644); werr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not write %s: %v\n", o.outputJSON, werr)
		}
	}
	fmt.Println(string(payload))
	return 0
}

func labelNames(pr *pullRequest) map[string]bool {
	set := map[string]bool{}
	for _, l := range pr.Labels {
		set[l.Name] = true
	}
	return set
}

func runReview(ctx context.Context, o options) (map[string]any, error) {
	pr, err := fetchPR(o.repo, o.prNumber)
	if err != nil {
		return nil, err
	}

	result := map[string]any{
		"pr":     o.prNumber,
		"repo":   o.repo,
		"head":   pr.HeadRefOID,
		"author": pr.Author.Login,
	}

	// The agent only reviews open, ready-for-review PRs. The workflow already
	// guards this, but keep the guarantee (and a clear message) for manual/local
	// runs too.
	if pr.State != "OPEN" {
		result["final"] = "SKIP"
		result["message"] = "the agent only reviews open PRs; this one is " + pr.State
		return result, nil
	}
	if pr.IsDraft {
		result["final"] = "SKIP"
		result["message"] = "the agent only reviews open, ready-for-review PRs; this one is a draft"
		return result, nil
	}

	if pr.Author.IsBot || hasBotSuffix(pr.Author.Login) {
		result["final"] = "SKIP"
		result["message"] = "the agent does not review bot-authored PRs"
		return result, nil
	}

	c := classify(pr.Files)
	gate := runGates(c, pr.Mergeable)
	result["classification"] = c
	result["gate"] = gate

	// Gate escalation is authoritative — never call the LLM to override it.
	if gate.Verdict == "ESCALATE" {
		result["final"] = "ESCALATE"
		body := renderNonApproval(c, gate, nil, "ESCALATE", o.label)
		result["body"] = body
		if !o.dryRun && !o.noPost {
			if err := upsertStickyComment(o.repo, o.prNumber, body); err != nil {
				return nil, err
			}
			if err := removeLabel(o.repo, o.prNumber, o.label); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// Pure docs/tests changes: auto-approve without spending an LLM call.
	if c.Tier == "T0-trivial" {
		v := verdict{Verdict: "APPROVE", Reasoning: "Trivial change (docs/tests only).", Risk: "low", Issues: []string{}}
		result["verdict"] = v
		result["final"] = "APPROVE"
		if !o.dryRun && !o.noPost {
			if err := postApproval(o.repo, o.prNumber, renderApproval(c, v)); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	if o.dryRun {
		result["final"] = "DRY-RUN"
		return result, nil
	}

	diff, truncated, err := fetchDiff(o.repo, o.prNumber, diffMaxBytes)
	if err != nil {
		return nil, err
	}
	v, err := review(ctx, pr, c, diff, truncated, o.repoRoot, o.agentDir, o.verbose)
	if err != nil {
		return nil, err
	}
	result["verdict"] = v
	result["final"] = v.Verdict

	if v.Verdict == "APPROVE" {
		body := renderApproval(c, v)
		result["body"] = body
		if !o.noPost {
			if err := postApproval(o.repo, o.prNumber, body); err != nil {
				return nil, err
			}
		}
	} else {
		body := renderNonApproval(c, gate, &v, v.Verdict, o.label)
		result["body"] = body
		if !o.noPost {
			if err := upsertStickyComment(o.repo, o.prNumber, body); err != nil {
				return nil, err
			}
			if err := removeLabel(o.repo, o.prNumber, o.label); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// runDismiss dismisses a stale approval on push unless the delta since it is
// trivial (docs/tests only).
func runDismiss(o options) (map[string]any, error) {
	pr, err := fetchPR(o.repo, o.prNumber)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"pr": o.prNumber, "repo": o.repo, "head": pr.HeadRefOID, "mode": "dismiss"}

	if pr.State != "OPEN" {
		result["final"] = "SKIP"
		result["message"] = "PR is not open (state " + pr.State + ")"
		return result, nil
	}

	reviews, err := fetchReviews(o.repo, o.prNumber)
	if err != nil {
		return nil, err
	}
	approvals := botApprovals(reviews, o.botLogin)
	if len(approvals) == 0 {
		result["final"] = "NOOP"
		result["message"] = "no bot approval to dismiss"
		return result, nil
	}

	latest := approvals[len(approvals)-1]
	approvedSHA := latest.CommitID

	if approvedSHA == pr.HeadRefOID {
		result["final"] = "KEEP"
		result["message"] = "approval already matches head"
		return result, nil
	}

	trivial := false
	if approvedSHA != "" {
		if changed, cerr := compareFiles(o.repo, approvedSHA, pr.HeadRefOID); cerr == nil && len(changed) > 0 {
			trivial = true
			for _, p := range changed {
				if !isTrivial(normPath(p)) {
					trivial = false
					break
				}
			}
		}
	}

	if trivial {
		result["final"] = "KEEP"
		result["message"] = "delta since approval is trivial (docs/tests only)"
		return result, nil
	}

	result["final"] = "DISMISSED"
	if !o.noPost {
		if err := dismissReview(o.repo, o.prNumber, latest.ID,
			"New commits changed reviewable code; re-review required."); err != nil {
			return nil, err
		}
		if err := upsertStickyComment(o.repo, o.prNumber,
			"♻️ Approval dismissed: new commits changed reviewable code. Re-add the `"+
				o.label+"` label to re-run the review."); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func hasBotSuffix(login string) bool {
	return len(login) >= 5 && login[len(login)-5:] == "[bot]"
}
