// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// LLM review stage, built on the Anthropic Go SDK.
//
// The model is given read-only tools only (read_file, grep, glob) scoped to the
// PR's checked-out working tree, plus a submit_verdict tool that ends the loop.
// It has no ability to run code. The reviewer's own guidance is loaded from the
// base branch (see the workflow), so a PR cannot rewrite the reviewer that judges
// it.

const model = "claude-sonnet-5"

const (
	maxTurns       = 20
	maxToolBytes   = 12_000
	maxGrepMatches = 100
	maxGlobResults = 200
	maxFileBytes   = 60_000
)

type verdict struct {
	Verdict   string   `json:"verdict"`
	Reasoning string   `json:"reasoning"`
	Risk      string   `json:"risk"`
	Issues    []string `json:"issues"`
}

const scaffoldTail = `

## Output contract

Investigate with read_file/grep/glob as needed, then call the submit_verdict tool
exactly once with:
- verdict: exactly one of "APPROVE", "REFUSE", "ESCALATE".
- reasoning: 1-2 sentences justifying the verdict.
- risk: "low", "medium", or "high".
- issues: a list of specific problems (empty for APPROVE).

Do not narrate outside tool calls. Decide, then submit.`

func loadGuidance(agentDir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(agentDir, "review-guidance.md"))
	if err != nil {
		return "", fmt.Errorf("reading review-guidance.md: %w", err)
	}
	return string(b) + scaffoldTail, nil
}

func buildPrompt(pr *pullRequest, c classification, diff string, truncated bool) string {
	var files strings.Builder
	for _, f := range pr.Files {
		fmt.Fprintf(&files, "  - %s (+%d/-%d)\n", f.Path, f.Additions, f.Deletions)
	}
	scrutiny := ""
	if len(c.ScrutinyFlags) > 0 {
		scrutiny = fmt.Sprintf(
			"\nDeterministic gate flagged these correctness-critical areas for extra "+
				"scrutiny: %s. Lean toward ESCALATE if your confidence in these files is "+
				"not high.\n", strings.Join(c.ScrutinyFlags, ", "),
		)
	}
	truncNote := ""
	if truncated {
		truncNote = "\n(The diff below was truncated; use read_file for the full picture.)\n"
	}
	body := strings.TrimSpace(pr.Body)
	if body == "" {
		body = "(none)"
	}
	return fmt.Sprintf(`Review this pull request against pgstream.

Title: %s
Author: %s
Base branch: %s
Tier: %s
%s
Changed files:
%s
PR description:
%s
%s
--- DIFF ---
%s
--- END DIFF ---

Investigate as needed, then call submit_verdict.`,
		pr.Title, pr.Author.Login, pr.BaseRefName, c.Tier, scrutiny, files.String(), body, truncNote, diff)
}

func reviewTools() []anthropic.ToolUnionParam {
	strProp := func(desc string) map[string]any {
		return map[string]any{"type": "string", "description": desc}
	}
	return []anthropic.ToolUnionParam{
		{OfTool: &anthropic.ToolParam{
			Name:        "read_file",
			Description: anthropic.String("Read a UTF-8 text file from the repository, relative to the repo root."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{"path": strProp("Repo-relative file path.")},
				Required:   []string{"path"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "grep",
			Description: anthropic.String("Search the repository for a Go-syntax regular expression. Returns matching path:line: text."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{
					"pattern":   strProp("Go regular expression to search for."),
					"path_glob": strProp("Optional glob to limit which files are searched, e.g. 'pkg/wal/**/*.go'."),
				},
				Required: []string{"pattern"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "glob",
			Description: anthropic.String("List repository files matching a glob pattern (supports ** for any depth)."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{"pattern": strProp("Glob pattern, e.g. '**/*_test.go'.")},
				Required:   []string{"pattern"},
			},
		}},
		{OfTool: &anthropic.ToolParam{
			Name:        "submit_verdict",
			Description: anthropic.String("Submit the final review verdict. Call exactly once when done."),
			InputSchema: anthropic.ToolInputSchemaParam{
				Properties: map[string]any{
					"verdict":   map[string]any{"type": "string", "enum": []string{"APPROVE", "REFUSE", "ESCALATE"}},
					"reasoning": strProp("1-2 sentences justifying the verdict."),
					"risk":      map[string]any{"type": "string", "enum": []string{"low", "medium", "high"}},
					"issues":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				},
				Required: []string{"verdict", "reasoning", "risk", "issues"},
			},
		}},
	}
}

func escalateVerdict(reason string) verdict {
	return verdict{Verdict: "ESCALATE", Reasoning: reason, Risk: "high", Issues: []string{reason}}
}

// review runs the tool loop and returns a verdict. Any tooling/API failure yields
// ESCALATE rather than a false APPROVE.
func review(ctx context.Context, pr *pullRequest, c classification, diff string, truncated bool, repoRoot, agentDir string, verbose bool) (verdict, error) {
	guidance, err := loadGuidance(agentDir)
	if err != nil {
		return verdict{}, err
	}

	client := anthropic.NewClient(option.WithAPIKey(os.Getenv("ANTHROPIC_API_KEY")))
	tools := reviewTools()
	messages := []anthropic.MessageParam{
		anthropic.NewUserMessage(anthropic.NewTextBlock(buildPrompt(pr, c, diff, truncated))),
	}

	for turn := 0; turn < maxTurns; turn++ {
		msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
			Model:     model,
			MaxTokens: 4096,
			System:    []anthropic.TextBlockParam{{Text: guidance}},
			Messages:  messages,
			Tools:     tools,
		})
		if err != nil {
			return escalateVerdict("reviewer LLM call failed; escalating for human review"), nil
		}
		messages = append(messages, msg.ToParam())

		var toolResults []anthropic.ContentBlockParamUnion
		for _, block := range msg.Content {
			tu, ok := block.AsAny().(anthropic.ToolUseBlock)
			if !ok {
				continue
			}
			if tu.Name == "submit_verdict" {
				v := parseVerdict(tu.Input)
				return v, nil
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "  [tool] %s %s\n", tu.Name, string(tu.Input))
			}
			out := execTool(tu.Name, tu.Input, repoRoot)
			toolResults = append(toolResults, anthropic.NewToolResultBlock(tu.ID, out, false))
		}

		if len(toolResults) == 0 {
			// Model produced no tool call and did not submit; nudge once.
			messages = append(messages, anthropic.NewUserMessage(
				anthropic.NewTextBlock("Call submit_verdict now with your final verdict."),
			))
			continue
		}
		messages = append(messages, anthropic.NewUserMessage(toolResults...))
	}

	return escalateVerdict("reviewer did not reach a verdict within its turn budget"), nil
}

func parseVerdict(raw json.RawMessage) verdict {
	var v verdict
	if err := json.Unmarshal(raw, &v); err != nil {
		return escalateVerdict("reviewer returned an unpar-seable verdict")
	}
	v.Verdict = strings.ToUpper(strings.TrimSpace(v.Verdict))
	switch v.Verdict {
	case "APPROVE", "REFUSE", "ESCALATE":
	default:
		return escalateVerdict(fmt.Sprintf("reviewer returned an unrecognised verdict %q", v.Verdict))
	}
	if strings.TrimSpace(v.Reasoning) == "" {
		v.Reasoning = "(no reasoning provided)"
	}
	if v.Risk == "" {
		v.Risk = "medium"
	}
	if v.Issues == nil {
		v.Issues = []string{}
	}
	return v
}

// --- read-only tool implementations, all sandboxed to repoRoot ---------------

func execTool(name string, input json.RawMessage, repoRoot string) string {
	switch name {
	case "read_file":
		var args struct {
			Path string `json:"path"`
		}
		_ = json.Unmarshal(input, &args)
		return execRead(repoRoot, args.Path)
	case "grep":
		var args struct {
			Pattern  string `json:"pattern"`
			PathGlob string `json:"path_glob"`
		}
		_ = json.Unmarshal(input, &args)
		return execGrep(repoRoot, args.Pattern, args.PathGlob)
	case "glob":
		var args struct {
			Pattern string `json:"pattern"`
		}
		_ = json.Unmarshal(input, &args)
		return execGlob(repoRoot, args.Pattern)
	default:
		return "error: unknown tool " + name
	}
}

// safePath resolves rel against root and rejects escapes outside root.
func safePath(root, rel string) (string, bool) {
	clean := filepath.Clean("/" + strings.TrimSpace(rel))
	abs := filepath.Join(root, clean)
	rp, err := filepath.Abs(root)
	if err != nil {
		return "", false
	}
	ap, err := filepath.Abs(abs)
	if err != nil {
		return "", false
	}
	if ap != rp && !strings.HasPrefix(ap, rp+string(os.PathSeparator)) {
		return "", false
	}
	return ap, true
}

func execRead(root, rel string) string {
	p, ok := safePath(root, rel)
	if !ok {
		return "error: path outside repository"
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return "error: " + err.Error()
	}
	if len(b) > maxFileBytes {
		return string(b[:maxFileBytes]) + "\n... [truncated]"
	}
	return string(b)
}

func execGrep(root, pattern, pathGlob string) string {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "error: invalid regexp: " + err.Error()
	}
	var out strings.Builder
	matches := 0
	walk(root, func(rel string) bool {
		if pathGlob != "" && !matchGlob(pathGlob, rel) {
			return true
		}
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil || isBinary(b) {
			return true
		}
		for i, line := range strings.Split(string(b), "\n") {
			if re.MatchString(line) {
				fmt.Fprintf(&out, "%s:%d: %s\n", rel, i+1, strings.TrimSpace(line))
				matches++
				if matches >= maxGrepMatches || out.Len() > maxToolBytes {
					return false
				}
			}
		}
		return true
	})
	if matches == 0 {
		return "(no matches)"
	}
	if matches >= maxGrepMatches || out.Len() > maxToolBytes {
		out.WriteString("... [truncated; refine your pattern or path_glob]\n")
	}
	return out.String()
}

func execGlob(root, pattern string) string {
	var out strings.Builder
	count := 0
	walk(root, func(rel string) bool {
		if matchGlob(pattern, rel) {
			out.WriteString(rel + "\n")
			count++
			if count >= maxGlobResults {
				return false
			}
		}
		return true
	})
	if count == 0 {
		return "(no files match)"
	}
	if count >= maxGlobResults {
		out.WriteString("... [truncated]\n")
	}
	return out.String()
}

// walk visits repo-relative file paths, skipping VCS/vendor noise. The callback
// returns false to stop the walk.
func walk(root string, fn func(rel string) bool) {
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if !fn(rel) {
			return filepath.SkipAll
		}
		return nil
	})
}

// matchGlob supports ** (any depth, including none) plus the usual * and ?.
func matchGlob(pattern, name string) bool {
	re := globToRegexp(pattern)
	return re.MatchString(name)
}

var globCache = map[string]*regexp.Regexp{}

func globToRegexp(pattern string) *regexp.Regexp {
	if re, ok := globCache[pattern]; ok {
		return re
	}
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				b.WriteString(".*")
				i++
				if i+1 < len(pattern) && pattern[i+1] == '/' {
					i++ // consume the slash so '**/' also matches zero segments
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '.', '+', '(', ')', '|', '^', '$', '{', '}', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteString("$")
	re := regexp.MustCompile(b.String())
	globCache[pattern] = re
	return re
}

func isBinary(b []byte) bool {
	n := len(b)
	if n > 512 {
		n = 512
	}
	for i := 0; i < n; i++ {
		if b[i] == 0 {
			return true
		}
	}
	return false
}
