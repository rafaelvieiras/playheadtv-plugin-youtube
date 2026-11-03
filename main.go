// Command playheadtv-plugin-youtube is a Playhead TV source plugin
// (docs/PLUGINS.md, protocol 1).
//
// It speaks one JSON object per line over stdin/stdout and offers two
// capabilities: catalog.list (the videos behind a channel, playlist or single
// video, for the backend's pull sync) and media.resolve (something a player can
// open right now).
//
// Resolving is the hard half. This site stopped serving progressive files that
// carry both video and audio, so "one URL of one file" no longer exists for it.
// The host contract still asks for exactly one playable URL, so this plugin
// assembles the stream itself: it remuxes the pairs it picked into HLS on local
// disk (no re-encode, no transcoding cost) and serves that playlist from a
// loopback HTTP server. The host hands the URL to its player and never learns
// anything about the two upstream streams.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const protocolVersion = 1

// Manifest is plugin.json, which ships inside the artifact. The host verifies
// the installed manifest against the registry, and `info` echoes this same
// file, so the artifact has exactly one description of itself.
type Manifest struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	Version      string   `json:"version"`
	Protocol     int      `json:"protocol"`
	Entrypoint   string   `json:"entrypoint"`
	Capabilities []string `json:"capabilities"`
	// Config is echoed from plugin.json: the settings a source of this plugin
	// takes, so the host can build the screen without knowing any of them.
	Config []manifestField `json:"config,omitempty"`
}

// manifestField is one declared setting (protocol 1, "config").
type manifestField struct {
	Key     string           `json:"key"`
	Label   string           `json:"label"`
	Type    string           `json:"type"`
	Options []manifestChoice `json:"options,omitempty"`
	Help    string           `json:"help,omitempty"`
}

type manifestChoice struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type response struct {
	ID     json.RawMessage `json:"id"`
	Result any             `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Reserved error codes from the contract.
const (
	codeConfig      = "config"
	codeNotFound    = "not_found"
	codeUnavailable = "unavailable"
	codeRateLimited = "rate_limited"
	codeUnsupported = "unsupported"
)

// codedError carries a contract error code up to the response writer.
type codedError struct {
	code string
	msg  string
}

func (e *codedError) Error() string { return e.msg }

func errf(code, format string, args ...any) error {
	return &codedError{code: code, msg: fmt.Sprintf(format, args...)}
}

type server struct {
	manifest Manifest
	out      *json.Encoder
	writeMu  sync.Mutex

	catalog *catalog
	streams *streams

	// One extraction per item is enough for minutes. It answers with urls, not
	// media, and running the extractor again costs seconds on the critical path
	// of a channel that is only being changed.
	extractMu sync.Mutex
	extracts  map[string]extraction

	// clock is time.Now outside a test.
	clock func() time.Time
}

func main() {
	manifest, err := loadManifest()
	if err != nil {
		fatal(err)
	}
	extractor, err := bundled(envExtractor, "yt-dlp")
	if err != nil {
		fatal(err)
	}
	remuxer, err := bundled(envRemuxer, "ffmpeg")
	if err != nil {
		fatal(err)
	}

	srv := &server{
		manifest: manifest,
		out:      json.NewEncoder(os.Stdout),
		catalog:  newCatalog(),
		extracts: map[string]extraction{},
		clock:    time.Now,
	}
	srv.catalog.init(extractor)
	if srv.streams, err = newStreams(); err != nil {
		fatal(err)
	}
	defer srv.streams.close()
	srv.streams.init(remuxer)

	if err := srv.serve(os.Stdin); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "plugin-youtube:", err)
	os.Exit(1)
}

// loadManifest reads plugin.json next to the executable, so the manifest the
// host installed is the one this process reports.
func loadManifest() (Manifest, error) {
	var m Manifest
	exe, err := os.Executable()
	if err != nil {
		return m, fmt.Errorf("locate executable: %w", err)
	}
	path := filepath.Join(filepath.Dir(exe), "plugin.json")
	body, err := os.ReadFile(path)
	if err != nil {
		return m, fmt.Errorf("read manifest: %w", err)
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return m, fmt.Errorf("parse %s: %w", path, err)
	}
	return m, nil
}

// serve reads requests until stdin closes (or a bad line makes the stream
// unrecoverable). Requests are answered in order: the host serializes its calls
// anyway, and a single reader keeps stdout strictly line-atomic.
func (s *server) serve(in io.Reader) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			// Without a usable id there is nobody to answer; the host treats a
			// garbled line as a protocol failure.
			fmt.Fprintln(os.Stderr, "plugin-youtube: bad request line:", err)
			return nil
		}
		resp := s.handle(req)
		if err := s.write(resp); err != nil {
			return err
		}
		if req.Method == "shutdown" {
			return nil
		}
	}
	return scanner.Err()
}

func (s *server) write(resp response) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.out.Encode(resp)
}

func (s *server) handle(req request) (resp response) {
	resp.ID = req.ID

	defer func() {
		if r := recover(); r != nil {
			resp.Result = nil
			resp.Error = &rpcError{Code: codeUnavailable, Message: fmt.Sprintf("plugin panic: %v", r)}
		}
	}()

	var (
		result any
		err    error
	)
	switch req.Method {
	case "info":
		result = s.manifest
	case "catalog.list":
		result, err = s.list(req.Params)
	case "media.resolve":
		result, err = s.resolve(req.Params)
	case "shutdown":
		result = struct{}{}
	default:
		err = errf(codeUnsupported, "unknown method %q", req.Method)
	}

	if err != nil {
		code := codeUnavailable
		var ce *codedError
		if ok := asCoded(err, &ce); ok {
			code = ce.code
		}
		msg := err.Error()
		if code != codeConfig && code != codeNotFound && code != codeUnsupported && code != codeRateLimited {
			code = codeUnavailable
		}
		resp.Error = &rpcError{Code: code, Message: msg}
		return resp
	}
	resp.Result = result
	return resp
}
