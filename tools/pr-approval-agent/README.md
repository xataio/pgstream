# pgstream PR approval agent

An AI-assisted PR reviewer that pairs **deterministic safety gates** with a Claude
review pass. It is deliberately lean and tuned for pgstream's Go + SQL layout — no
ownership model, no analytics — just path-based gating plus an LLM review.

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

- **APPROVE** → the agent posts a real approving PR review (as xata-bot, so it can
  count toward branch protection) and keeps the label.
- **REFUSE / ESCALATE** → the agent posts a sticky comment explaining why and
  removes the label, so it can be re-added after a fix.
- **New push (`synchronize`)** → a stale approval is dismissed unless the delta
  since it is inert (docs/markdown only).

### Gate policy (`gates.go` is the authoritative source)

| Area | Behaviour |
| --- | --- |
| `migrations/`, `.github/`, `.goreleaser*.yaml`, `Dockerfile`, `go.mod/go.sum`, `tools/pr-approval-agent/`, `LICENSE` | **Deny** → always ESCALATE to a human (checked on both the new and pre-rename path) |
| `pkg/wal/replication/`, `pkg/wal/checkpointer/`, `pkg/wal/listener/`, `pkg/snapshot/`, `pkg/wal/processor/postgres/` | **Scrutiny flag** → reviewed, but the model is told to lean toward ESCALATE |
| `*.md`, `docs/`, `coverage/` | **Inert** → auto-approve without an LLM call; dismiss keeps an approval only across inert deltas |
| `*_test.go`, `testdata/`, `mocks/`, generated `*-definition.json` | **Size-exempt but reviewed** → excluded from the size ceiling, but still get an LLM review (CI compiles/runs test files, so they are not auto-approved) |
| > 300 substantive lines or > 15 substantive files, or > 3000 changed files | **Size ceiling** → ESCALATE (too big to classify/auto-review) |
| Draft / merged / closed / bot-authored (incl. xata-bot) | **Skipped** → the agent only reviews open, ready-for-review PRs |
| Merge-conflicted | **ESCALATE** → rebase before review |

The exact patterns and thresholds live in `gates.go`; the table above is a summary.

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

### 1. Bot identity (xata-bot)

Reviews are posted as the **xata-bot** machine user, authenticated with a PAT.
A real user's approving review counts toward required-review branch protection,
unlike the built-in `GITHUB_TOKEN`.

Requirements:
- xata-bot must have **write access** to the repo (to post reviews and manage the
  label).
- Its PAT needs **pull-requests: read & write** (a fine-grained PAT scoped to
  `xataio/pgstream`, or a classic `repo`-scoped token). PATs expire — rotate
  before expiry or the agent will start failing.
- The bot login is `xata-bot` — kept in sync via `defaultBotLogin` in `main.go`
  and `REVIEW_BOT_LOGIN` in the workflow. (To use a GitHub App instead, swap the
  workflow's `GH_TOKEN` for an `actions/create-github-app-token` step and set the
  login to `<app-name>[bot]`.)

### 2. Add repo secrets

In `xataio/pgstream → Settings → Secrets and variables → Actions`:

| Secret | Value |
| --- | --- |
| `GIT_TOKEN` | xata-bot's PAT (already present — reused from the release workflow) |
| `REVIEW_ANTHROPIC_API_KEY` | an Anthropic API key for the review LLM (new) |

### 3. Create the trigger label

Create a label named **`ai-review`** in the repo. Adding it to a non-draft PR runs
the agent.

### 4. (Optional) require the bot in branch protection

To make an approval meaningful, require a review from xata-bot, or rely on it as
an advisory approval alongside human review. The agent **removes the label on
REFUSE/ESCALATE** and dismisses its own approval on substantive pushes — it is an
assistant, not a merge gate on its own. Note xata-bot cannot approve its own PRs,
so bot-authored PRs are skipped.

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

### Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `ANTHROPIC_API_KEY` | — | required for the LLM review call |
| `REVIEW_MODEL` | `claude-sonnet-5` | reviewer model; set to `claude-opus-4-8` for higher-confidence reviews |

`REVIEW_MODEL` is wired as a workflow-level `env` in `pr-approval-agent.yml`, so you
can change the model there without touching code.

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
