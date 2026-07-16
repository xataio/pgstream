// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// Thin wrappers over the `gh` CLI. Every call uses whatever token gh/GH_TOKEN is
// configured with; in CI that is the GitHub App installation token, so reviews
// are attributed to the app and can satisfy branch protection.

const stickyMarker = "<!-- pgstream-review-agent -->"

const prFields = "number,title,body,author,isDraft,mergeable,headRefOid,baseRefName,files,labels,reviews"

type ghUser struct {
	Login string `json:"login"`
	IsBot bool   `json:"is_bot"`
}

type ghReview struct {
	ID     int64  `json:"id"`
	State  string `json:"state"`
	Author ghUser `json:"author"`
	Commit struct {
		OID string `json:"oid"`
	} `json:"commit"`
}

type ghLabel struct {
	Name string `json:"name"`
}

type pullRequest struct {
	Number      int           `json:"number"`
	Title       string        `json:"title"`
	Body        string        `json:"body"`
	Author      ghUser        `json:"author"`
	IsDraft     bool          `json:"isDraft"`
	Mergeable   string        `json:"mergeable"`
	HeadRefOID  string        `json:"headRefOid"`
	BaseRefName string        `json:"baseRefName"`
	Files       []changedFile `json:"files"`
	Labels      []ghLabel     `json:"labels"`
	Reviews     []ghReview    `json:"reviews"`
}

func runGH(args ...string) (string, error) {
	cmd := exec.Command("gh", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("gh %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func fetchPR(repo string, number int) (*pullRequest, error) {
	out, err := runGH("pr", "view", fmt.Sprint(number), "--repo", repo, "--json", prFields)
	if err != nil {
		return nil, err
	}
	var pr pullRequest
	if err := json.Unmarshal([]byte(out), &pr); err != nil {
		return nil, fmt.Errorf("parsing PR json: %w", err)
	}
	return &pr, nil
}

// fetchDiff returns the PR diff, clipped so the prompt stays bounded.
func fetchDiff(repo string, number, maxBytes int) (diff string, truncated bool, err error) {
	out, err := runGH("pr", "diff", fmt.Sprint(number), "--repo", repo)
	if err != nil {
		return "", false, err
	}
	if len(out) > maxBytes {
		return out[:maxBytes], true, nil
	}
	return out, false, nil
}

func postApproval(repo string, number int, body string) error {
	_, err := runGH("pr", "review", fmt.Sprint(number), "--repo", repo, "--approve", "--body", body)
	return err
}

func upsertStickyComment(repo string, number int, body string) error {
	full := stickyMarker + "\n" + body
	id, err := findStickyComment(repo, number)
	if err != nil {
		return err
	}
	if id == 0 {
		_, err = runGH("pr", "comment", fmt.Sprint(number), "--repo", repo, "--body", full)
		return err
	}
	_, err = runGH("api", "--method", "PATCH",
		fmt.Sprintf("repos/%s/issues/comments/%d", repo, id),
		"-f", "body="+full)
	return err
}

func findStickyComment(repo string, number int) (int64, error) {
	out, err := runGH("api", fmt.Sprintf("repos/%s/issues/%d/comments", repo, number), "--paginate")
	if err != nil {
		return 0, err
	}
	var comments []struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
	}
	if err := json.Unmarshal([]byte(out), &comments); err != nil {
		return 0, fmt.Errorf("parsing comments json: %w", err)
	}
	for _, c := range comments {
		if strings.Contains(c.Body, stickyMarker) {
			return c.ID, nil
		}
	}
	return 0, nil
}

func removeLabel(repo string, number int, label string) error {
	// gh errors if the label is absent; treat that as a no-op.
	if _, err := runGH("pr", "edit", fmt.Sprint(number), "--repo", repo, "--remove-label", label); err != nil {
		return nil
	}
	return nil
}

// botApprovals returns APPROVED reviews left by the agent bot, in API order.
func botApprovals(pr *pullRequest, botLogin string) []ghReview {
	var out []ghReview
	for _, r := range pr.Reviews {
		if r.Author.Login == botLogin && r.State == "APPROVED" {
			out = append(out, r)
		}
	}
	return out
}

func dismissReview(repo string, number int, reviewID int64, message string) error {
	_, err := runGH("api", "--method", "PUT",
		fmt.Sprintf("repos/%s/pulls/%d/reviews/%d/dismissals", repo, number, reviewID),
		"-f", "message="+message,
		"-f", "event=DISMISS")
	return err
}

// compareFiles returns files changed between two commits via the GitHub compare
// API, so it works without a checkout and across fork boundaries.
func compareFiles(repo, baseSHA, headSHA string) ([]string, error) {
	out, err := runGH("api",
		fmt.Sprintf("repos/%s/compare/%s...%s", repo, baseSHA, headSHA),
		"--jq", ".files[].filename")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			files = append(files, line)
		}
	}
	return files, nil
}
