# pgstream PR approval agent

An AI-assisted PR reviewer that pairs **deterministic safety gates** with a Claude
review pass. It is a lean, Go-native reimplementation of
[PostHog's stamphog agent](https://github.com/PostHog/posthog/tree/master/tools/pr-approval-agent),
retuned for pgstream's Go + SQL layout — no ownership model, no Django migration
heuristics, no analytics.

It lives in its own nested Go module (`tools/pr-approval-agent/go.mod`) so the
Anthropic SDK dependency stays out of pgstream's core `go.mod`.

## How it works

```
label "ai-review" added ─▶ deterministic gates ─▶ (conditionally) Claude review ─▶ verdict
                             │                        │
                             │ deny-list / size       │ read_file + grep + glob,
                             │ ceiling / draft         │ no code execution, submit_verdict
                             ▼                        ▼
                          ESCALATE (human)      APPROVE · REFUSE · ESCALATE
```

**Gates are authoritative: the LLM can tighten their verdict but never loosen it.**
A PR touching a deny-listed area is escalated to a human regardless of what the
model thinks of the code.

- **APPROVE** → the agent posts a real approving PR review (as the GitHub App, so it
  can count toward branch protection) and keeps the label.
- **REFUSE / ESCALATE** → the agent posts a sticky comment explaining why and
  removes the label, so it can be re-added after a fix.
- **New push (`synchronize`)** → a stale approval is dismissed unless the delta
  since it is trivial (docs/tests only).

### Gate policy (edit `gates.go` to tune)

| Area | Behaviour |
| --- | --- |
| `migrations/`, `.github/`, `.goreleaser*.yaml`, `Dockerfile`, `go.mod/go.sum`, `tools/pr-approval-agent/`, `LICENSE` | **Deny** → always ESCALATE to a human |
| `pkg/wal/replication/`, `pkg/wal/checkpointer/`, `pkg/wal/listener/`, `pkg/snapshot/`, `pkg/wal/processor/postgres/` | **Scrutiny flag** → reviewed, but the model is told to lean toward ESCALATE |
| `*.md`, `docs/`, `*_test.go`, `testdata/`, `mocks/`, generated `*-definition.json` | **Trivial** → excluded from the size ceiling; docs-only PRs auto-approve without an LLM call |
| > 800 substantive lines or > 30 substantive files | **Size ceiling** → ESCALATE (too big to auto-review) |
| Draft / conflicting / bot-authored | ESCALATE / skip |

The trusted review criteria the model follows live in
[`review-guidance.md`](./review-guidance.md). Both that file and the agent binary
are always loaded/built **from the base branch**, never from the PR under review.

## Files

| File | Role |
| --- | --- |
| `main.go` | Orchestrator + CLI (`review` / `dismiss` modes), verdict posting |
| `gates.go` | Deterministic classification, deny-list, size ceiling |
| `reviewer.go` | Anthropic Go SDK tool-use loop (read-only tools, `submit_verdict`) |
| `github.go` | `gh` CLI wrappers (fetch PR, post review, sticky comment, dismiss) |
| `render.go` | GitHub comment rendering for verdicts |
| `review-guidance.md` | Trusted review criteria injected into the system prompt |
| `gates_test.go` | Unit tests for the gate and glob logic |
| `../../.github/workflows/pr-approval-agent.yml` | The workflow that runs it |

## One-time setup

### 1. Register a GitHub App

A dedicated app is required so the agent's approvals are attributed to a bot and
can satisfy required-review branch protection (the built-in `GITHUB_TOKEN` cannot
post approvals that count).

1. **Org/user → Settings → Developer settings → GitHub Apps → New GitHub App.**
   - Name e.g. `pgstream-review-agent` (its bot login becomes
     `pgstream-review-agent[bot]` — keep this in sync with `defaultBotLogin` in
     `main.go` and `REVIEW_LABEL` in the workflow if you rename).
   - Permissions (repository): **Pull requests: Read & write**, **Contents: Read-only**,
     **Metadata: Read-only**.
   - Subscribe to events: none needed (the workflow triggers, not a webhook).
2. **Generate a private key** and note the **App ID**.
3. **Install the app** on the `xataio/pgstream` repo.

### 2. Add repo secrets

In `xataio/pgstream → Settings → Secrets and variables → Actions`:

| Secret | Value |
| --- | --- |
| `PR_APPROVAL_AGENT_APP_ID` | the App ID |
| `PR_APPROVAL_AGENT_PRIVATE_KEY` | the app's private key (full PEM) |
| `REVIEW_ANTHROPIC_API_KEY` | an Anthropic API key for the review LLM |

### 3. Create the trigger label

Create a label named **`ai-review`** in the repo. Adding it to a non-draft PR runs
the agent.

### 4. (Optional) require the bot in branch protection

To make an approval meaningful, add the app as a required reviewer, or rely on it
as an advisory approval alongside human review. The agent **removes the label on
REFUSE/ESCALATE** and dismisses its own approval on substantive pushes — it is an
assistant, not a merge gate on its own.

## Local testing

Requires Go and an authenticated `gh`. For a real LLM review set
`ANTHROPIC_API_KEY`. Note: because Go's `flag` package stops at the first
positional argument, **the PR number comes last, after all flags.**

```bash
cd tools/pr-approval-agent

# Unit-test the gate + glob logic (no network):
go test ./...

# Gates only — no LLM, no posting:
go run . --repo xataio/pgstream --dry-run 1002

# Full review against a local checkout, compute the verdict but DO NOT post:
ANTHROPIC_API_KEY=sk-... go run . \
  --repo xataio/pgstream --repo-root /path/to/pr/checkout --no-post -v 1002

# Dismiss check for a pushed PR (no LLM):
go run . --repo xataio/pgstream --mode dismiss 1002
```

`--repo-root` is the working tree the reviewer reads with read_file/grep/glob;
`--agent-dir` (default: the binary's directory) is where `review-guidance.md` is
loaded from.

## Safety model

- The agent binary is **built from the base** checkout, so a PR cannot alter the
  reviewer that judges it.
- The reviewer has **read-only tools** (read_file/grep/glob) scoped to the PR
  checkout and cannot run code, so PR contents are never executed.
- Any change under `.github/` or `tools/pr-approval-agent/` is **deny-listed**, so a
  PR that modifies the agent is never auto-approved.
- The workflow uses `pull_request_target` (so it has secrets on fork PRs) but reads
  PR code only from a separate `_pr/` directory.
- Any tooling or API failure yields **ESCALATE**, never a false APPROVE.
