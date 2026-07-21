// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecReadRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("SECRET"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A symlink committed inside the (attacker-controlled) tree pointing outside.
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}

	out := execRead(root, "escape")
	if !strings.HasPrefix(out, "error:") {
		t.Fatalf("expected error reading symlink escape, got: %q", out)
	}
	if strings.Contains(out, "SECRET") {
		t.Fatalf("symlink escape leaked outside content: %q", out)
	}
}

func TestExecReadRejectsParentEscape(t *testing.T) {
	root := t.TempDir()
	if out := execRead(root, "../../etc/hostname"); !strings.HasPrefix(out, "error:") {
		t.Fatalf("expected error for parent escape, got: %q", out)
	}
}

func TestExecReadAllowsNormalFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.go"), []byte("package x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out := execRead(root, "a.go"); out != "package x" {
		t.Fatalf("expected file content, got: %q", out)
	}
}

func TestGrepSkipsSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("NEEDLE_SECRET"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link.txt")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "real.txt"), []byte("NEEDLE_ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := execGrep(root, "NEEDLE", "")
	if strings.Contains(out, "NEEDLE_SECRET") {
		t.Fatalf("grep followed a symlink out of the sandbox: %q", out)
	}
	if !strings.Contains(out, "real.txt") {
		t.Fatalf("grep missed the in-tree file: %q", out)
	}
}

func TestRedactSecrets(t *testing.T) {
	t.Setenv("GH_TOKEN", "ghp_thisisareallylongtokenvalue123456")
	in := "found token ghp_thisisareallylongtokenvalue123456 and sk-ant-api03-abcdefghijklmnopqrstuv in the file"
	out := redactSecrets(in)
	if strings.Contains(out, "ghp_thisisareallylong") || strings.Contains(out, "sk-ant-api03-abcdefghijklmnopqrstuv") {
		t.Fatalf("secrets not redacted: %q", out)
	}
}
