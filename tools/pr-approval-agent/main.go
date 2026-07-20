// SPDX-License-Identifier: Apache-2.0

// Command pr-approval-agent is a lean, Go-native AI PR reviewer for pgstream.
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
	"strconv"
	"strings"
)

// Trigger label and bot identity. Override via flags. Reviews are posted as this
// login (the xata-bot machine user); its own PRs are skipped since it cannot
// approve them.
const (
	defaultLabel    = "ai-review"
	defaultBotLogin = "xata-bot"
	diffMaxBytes    = 200_000
	// compareFileCap is GitHub's per-response cap on the compare endpoint; a
	// delta at or above it is incompletely reported, so it is never "inert".
	compareFileCap = 300
	// Config lives under .github/ (already deny-listed, so a PR can't weaken its
	// own gate). Paths are resolved relative to the working dir (the repo root).
	defaultConfigPath   = ".github/pr-approval-agent/policy.yml"
	defaultGuidancePath = ".github/pr-approval-agent/review-guidance.md"
)

type options struct {
	prNumber   int
	repo       string
	mode       string
	label      string
	botLogin   string
	repoRoot   string
	configPath string
	guidance   string
	dryRun     bool
	noPost     bool
	outputJSON string
	verbose    bool
	pol        *policy
}

func main() {
	os.Exit(run())
}

func run() int {
	var o options
	flag.StringVar(&o.repo, "repo", "xataio/pgstream", "target repository (owner/name)")
	flag.StringVar(&o.mode, "mode", "review", "review | dismiss")
	flag.StringVar(&o.label, "label", defaultLabel, "trigger label to remove on non-approval")
	flag.StringVar(&o.botLogin, "bot-login", defaultBotLogin, "login of the agent bot")
	flag.StringVar(&o.repoRoot, "repo-root", ".", "path to the PR's checked-out code the reviewer reads")
	flag.StringVar(&o.configPath, "config", defaultConfigPath, "path to the YAML gate-policy file")
	flag.StringVar(&o.guidance, "guidance", defaultGuidancePath, "path to the reviewer guidance markdown")
	flag.BoolVar(&o.dryRun, "dry-run", false, "gates only: no LLM, no posting")
	flag.BoolVar(&o.noPost, "no-post", false, "compute the verdict but do not touch the PR")
	flag.StringVar(&o.outputJSON, "output-json", "", "write the full result JSON to this path")
	flag.BoolVar(&o.verbose, "v", false, "log agent tool calls to stderr")
	flag.Parse()

	// Go's flag package stops at the first positional, so flags placed after the
	// PR number are silently ignored — dangerous when e.g. --no-post is dropped.
	// Require exactly one positional and reject trailing args.
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: pr-approval-agent [flags] <pr-number>")
		if flag.NArg() > 1 {
			fmt.Fprintf(os.Stderr, "unexpected arguments after the PR number: %v (flags must precede it)\n", flag.Args()[1:])
		}
		return 2
	}
	n, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid PR number %q\n", flag.Arg(0))
		return 2
	}
	o.prNumber = n

	o.pol, err = loadPolicy(o.configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 2
	}

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

	if pr.Author.IsBot || strings.HasSuffix(pr.Author.Login, "[bot]") || pr.Author.Login == o.botLogin {
		result["final"] = "SKIP"
		result["message"] = "the agent does not review bot-authored PRs"
		return result, nil
	}

	// If the file list hit the REST cap it is incomplete, so the gates cannot be
	// trusted — escalate rather than risk classifying a partial change.
	if pr.FilesTruncated {
		gate := gateResult{
			Allow:   false,
			Verdict: "ESCALATE",
			Reasons: []string{fmt.Sprintf("PR changes at least %d files; too large to classify reliably", restFileCap)},
		}
		c := o.pol.classify(pr.Files)
		result["classification"] = c
		result["gate"] = gate
		result["final"] = "ESCALATE"
		body := renderNonApproval(c, gate, nil, "ESCALATE", o.label)
		result["body"] = body
		if !o.dryRun && !o.noPost {
			if err := postNonApproval(o, pr, body); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	c := o.pol.classify(pr.Files)
	gate := o.pol.runGates(c, pr.Mergeable)
	result["classification"] = c
	result["gate"] = gate

	// Gate escalation is authoritative — never call the LLM to override it.
	if gate.Verdict == "ESCALATE" {
		result["final"] = "ESCALATE"
		body := renderNonApproval(c, gate, nil, "ESCALATE", o.label)
		result["body"] = body
		if !o.dryRun && !o.noPost {
			if err := postNonApproval(o, pr, body); err != nil {
				return nil, err
			}
		}
		return result, nil
	}

	// Inert changes (docs/markdown only) auto-approve without spending an LLM
	// call. Note test files are deliberately NOT inert — CI compiles and runs
	// them, so they get a review.
	if c.Tier == "T0-trivial" {
		v := verdict{Verdict: "APPROVE", Reasoning: "Inert change (docs only).", Risk: "low", Issues: []string{}}
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

	// A missing key is a deployment misconfiguration, not a review outcome —
	// fail loudly (exit 1) instead of silently escalating every PR.
	if strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")) == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY is not set; cannot run the LLM review")
	}

	diff, truncated, err := fetchDiff(o.repo, o.prNumber, diffMaxBytes)
	if err != nil {
		return nil, err
	}
	v, err := review(ctx, pr, c, diff, truncated, o.repoRoot, o.guidance, o.verbose)
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
			if err := postNonApproval(o, pr, body); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// postNonApproval posts the sticky comment for a REFUSE/ESCALATE, dismisses any
// prior bot approval that a re-review has now superseded (so a stale APPROVE
// cannot keep satisfying branch protection), and drops the trigger label.
// Label/dismiss failures are logged, not fatal — the verdict is already recorded.
func postNonApproval(o options, pr *pullRequest, body string) error {
	if err := upsertStickyComment(o.repo, o.prNumber, body); err != nil {
		return err
	}
	if err := dismissBotApprovals(o); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not dismiss prior bot approvals: %v\n", err)
	}
	if hasLabel(pr, o.label) {
		if err := removeLabel(o.repo, o.prNumber, o.label); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not remove label %q: %v\n", o.label, err)
		}
	}
	return nil
}

func dismissBotApprovals(o options) error {
	reviews, err := fetchReviews(o.repo, o.prNumber)
	if err != nil {
		return err
	}
	for _, r := range botApprovals(reviews, o.botLogin) {
		if err := dismissReview(o.repo, o.prNumber, r.ID, "Superseded by a non-approving re-review."); err != nil {
			return err
		}
	}
	return nil
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

	// Keep the approval only when EVERY file in the delta is inert (docs only).
	// Test files are executable and excluded; a >=300-file delta is incompletely
	// reported by the compare API, so it is never treated as inert.
	inert := false
	if approvedSHA != "" {
		if changed, cerr := compareFiles(o.repo, approvedSHA, pr.HeadRefOID); cerr == nil && len(changed) > 0 && len(changed) < compareFileCap {
			inert = true
			for _, p := range changed {
				if !o.pol.isInert(normPath(p)) {
					inert = false
					break
				}
			}
		}
	}

	if inert {
		result["final"] = "KEEP"
		result["message"] = "delta since approval is inert (docs only)"
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
