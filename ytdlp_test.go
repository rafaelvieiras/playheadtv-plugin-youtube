package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// recorder captures the arguments an extractor call would receive.
func recorder() (command, *[]string) {
	var seen []string
	return func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
		seen = args
		return []byte(`{}`), nil
	}, &seen
}

func TestWithJSRuntimeTellsTheExtractorWhereTheRuntimeIs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envJSRuntime, dir)

	inner, seen := recorder()
	if _, err := withJSRuntime(inner)(context.Background(), nil, "-j", "url"); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"--js-runtimes", jsRuntimeName + ":" + dir, "-j", "url"}
	if len(*seen) != len(want) {
		t.Fatalf("args = %v, want %v", *seen, want)
	}
	for i := range want {
		if (*seen)[i] != want[i] {
			t.Fatalf("args = %v, want %v", *seen, want)
		}
	}
}

// A runtime override may name the binary itself, which is what a developer
// running out of a source checkout has.
func TestWithJSRuntimeAcceptsTheRuntimeBinary(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, jsRuntimeName)
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write runtime: %v", err)
	}
	t.Setenv(envJSRuntime, bin)

	inner, seen := recorder()
	if _, err := withJSRuntime(inner)(context.Background(), nil, "-j"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if (*seen)[1] != jsRuntimeName+":"+dir {
		t.Fatalf("args = %v, want the runtime's directory %q", *seen, dir)
	}
}

// Without a runtime the extractor still runs: it degrades instead of failing,
// and a plugin that refused to list anything because a helper was absent would
// be worse than one that extracts less.
func TestWithJSRuntimeIsANoopWithoutARuntime(t *testing.T) {
	t.Setenv(envJSRuntime, filepath.Join(t.TempDir(), "absent"))

	inner, seen := recorder()
	if _, err := withJSRuntime(inner)(context.Background(), nil, "-j", "url"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(*seen) != 2 || (*seen)[0] != "-j" {
		t.Fatalf("args = %v, want them untouched", *seen)
	}
}
