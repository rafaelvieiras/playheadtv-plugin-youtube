package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// The extractor and the remuxer are shipped inside this plugin's artifact, next
// to the entrypoint, so installing the plugin is the only step a host needs.
// The environment overrides exist for tests and for a developer running the
// binary straight out of a source checkout.
const (
	envExtractor = "PLAYHEADTV_PLUGIN_EXTRACTOR"
	envRemuxer   = "PLAYHEADTV_PLUGIN_REMUXER"
)

// bundled returns the path of a helper program shipped in the artifact.
func bundled(envVar, name string) (string, error) {
	if override := os.Getenv(envVar); override != "" {
		return override, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate executable: %w", err)
	}
	path := filepath.Join(filepath.Dir(exe), name)
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("bundled %s is missing from the artifact (%s): %w", name, path, err)
	}
	return path, nil
}

// command runs a helper and returns its stdout. stderr is kept out of stdout on
// purpose: stdout is protocol traffic only, so diagnostics go to our own stderr.
type command func(ctx context.Context, stdin []byte, args ...string) ([]byte, error)

func runHelper(bin string) command {
	return func(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bin, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if stdin != nil {
			cmd.Stdin = bytes.NewReader(stdin)
		}
		if err := cmd.Run(); err != nil {
			return stdout.Bytes(), &helperError{err: err, stderr: stderr.String()}
		}
		return stdout.Bytes(), nil
	}
}

// helperError keeps the helper's own message: the host logs it, and it is the
// only explanation of why an item would not resolve.
type helperError struct {
	err    error
	stderr string
}

func (e *helperError) Error() string {
	msg := strings.TrimSpace(e.stderr)
	if i := strings.LastIndexByte(msg, '\n'); i >= 0 {
		msg = msg[i+1:]
	}
	msg = strings.TrimPrefix(msg, "ERROR: ")
	if msg == "" {
		return e.err.Error()
	}
	return msg
}

func (e *helperError) Unwrap() error { return e.err }

// asCoded is errors.As for the contract's error codes, kept as a small helper so
// handle() stays readable.
func asCoded(err error, target **codedError) bool {
	return errors.As(err, target)
}

// decodeJSON decodes helper output, tolerating leading diagnostics. Some
// versions print a deprecation warning on stdout before the document.
func decodeJSON(data []byte, v any) error {
	trimmed := bytes.TrimSpace(data)
	if i := bytes.IndexByte(trimmed, '{'); i > 0 {
		trimmed = trimmed[i:]
	}
	if len(trimmed) == 0 {
		return errors.New("no output")
	}
	return json.Unmarshal(trimmed, v)
}
