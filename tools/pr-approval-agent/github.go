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
// configured with; in CI that is the xata-bot PAT, so reviews are attributed to
// xata-bot and its approvals can satisfy branch protection.

const stickyMarker = "<!-- pgstream-review-agent -->"

// Note: neither files nor reviews are fetched here.
//   - `gh pr view --json files` is a GraphQL call capped at the first 100 files
//     with no pagination, so a large PR would hide files 101+ from the gates —
//     a classification bypass. Files come from the paginated REST endpoint
//     instead (fetchFiles), which also reports rename origins.
//   - `gh pr view --json reviews` gives a string node id and omits the reviewed
//     commit, both of which dismiss needs; reviews come from REST (fetchReviews).
const prFields = "number,title,body,author,isDraft,state,mergeable,headRefOid,baseRefName,labels"

// restFileCap is GitHub's hard cap on the PR files REST endpoint. Beyond this the
// file list is incomplete, so the PR must be escalated rather than classified.
const restFileCap = 3000

type ghUser struct {
	Login string `json:"login"`
	IsBot bool   `json:"is_bot"`
}

// restReview matches the REST reviews endpoint, which gives a numeric id (usable
// with the REST dismissal endpoint) and commit_id (the SHA reviewed).
type restReview struct {
	ID       int64  `json:"id"`
	State    string `json:"state"`
	CommitID string `json:"commit_id"`
	User     ghUser `json:"user"`
}

type ghLabel struct {
	Name string `json:"name"`
}

type pullRequest struct {
	Number         int           `json:"number"`
	Title          string        `json:"title"`
	Body           string        `json:"body"`
	Author         ghUser        `json:"author"`
	IsDraft        bool          `json:"isDraft"`
	State          string        `json:"state"` // OPEN | CLOSED | MERGED
	Mergeable      string        `json:"mergeable"`
	HeadRefOID     string        `json:"headRefOid"`
	BaseRefName    string        `json:"baseRefName"`
	Labels         []ghLabel     `json:"labels"`
	Files          []changedFile `json:"-"` // populated from REST (fetchFiles)
	FilesTruncated bool          `json:"-"` // file list hit the REST cap; classification unsafe
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
	files, truncated, err := fetchFiles(repo, number)
	if err != nil {
		return nil, err
	}
	pr.Files = files
	pr.FilesTruncated = truncated
	return &pr, nil
}

type restFile struct {
	Filename         string `json:"filename"`
	PreviousFilename string `json:"previous_filename"`
	Additions        int    `json:"additions"`
	Deletions        int    `json:"deletions"`
}

// fetchFiles lists a PR's changed files via the paginated REST endpoint (up to
// restFileCap). Returns truncated=true when the cap is hit, meaning the list is
// incomplete and the PR must not be classified from it.
func fetchFiles(repo string, number int) (files []changedFile, truncated bool, err error) {
	out, err := runGH("api", fmt.Sprintf("repos/%s/pulls/%d/files?per_page=100", repo, number), "--paginate")
	if err != nil {
		return nil, false, err
	}
	var rf []restFile
	if err := json.Unmarshal([]byte(out), &rf); err != nil {
		return nil, false, fmt.Errorf("parsing files json: %w", err)
	}
	files = make([]changedFile, len(rf))
	for i, f := range rf {
		files[i] = changedFile{
			Path:      f.Filename,
			Prev:      f.PreviousFilename,
			Additions: f.Additions,
			Deletions: f.Deletions,
		}
	}
	return files, len(rf) >= restFileCap, nil
}

// hasLabel reports whether the PR currently carries the given label.
func hasLabel(pr *pullRequest, label string) bool {
	for _, l := range pr.Labels {
		if l.Name == label {
			return true
		}
	}
	return false
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
	_, err := runGH("pr", "edit", fmt.Sprint(number), "--repo", repo, "--remove-label", label)
	if err != nil && strings.Contains(err.Error(), "not found") {
		// Label already absent (e.g. a concurrent run removed it) — a genuine
		// no-op. Any other error (auth, network, permissions) is propagated.
		return nil
	}
	return err
}

// fetchReviews lists a PR's reviews via the REST API (numeric id + commit_id).
func fetchReviews(repo string, number int) ([]restReview, error) {
	out, err := runGH("api", fmt.Sprintf("repos/%s/pulls/%d/reviews", repo, number), "--paginate")
	if err != nil {
		return nil, err
	}
	var reviews []restReview
	if err := json.Unmarshal([]byte(out), &reviews); err != nil {
		return nil, fmt.Errorf("parsing reviews json: %w", err)
	}
	return reviews, nil
}

// botApprovals returns APPROVED reviews left by the agent bot, in API order.
func botApprovals(reviews []restReview, botLogin string) []restReview {
	var out []restReview
	for _, r := range reviews {
		if r.User.Login == botLogin && r.State == "APPROVED" {
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
