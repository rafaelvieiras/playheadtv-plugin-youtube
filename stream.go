package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The plugin assembles its own stream. This site serves video and audio as
// separate files, and the host contract asks for exactly one playable URL, so
// the pieces are remuxed (-c copy: no re-encode, no CPU per second of video) and
// served as one HTTP body. A player opens it like any other progressive file and
// never learns there were two upstream files.
//
// HLS with the two tracks in SEPARATE playlists, which is the shape that survives
// -c copy. The muxer cannot cut where it wants: video only on keyframes, audio
// anywhere. Putting both tracks in one playlist therefore gives one stream
// segments of 3.0s, 12.28s and 0.13s with the audio starting seconds after the
// video — measured — and a player reading that stopped a few seconds in with data
// still buffered (no error, no buffering flag, one frozen picture). With one
// playlist per track (an HLS audio group) each track is cut on its own terms and
// the player aligns them by timestamp, which is what it does for DASH.
//
// One continuous body would be simpler and was tried: media3 refuses an MP4 whose
// length it cannot know, because its MP4 reader has to seek back after sniffing
// (reproduced: "Source error", no detail). A playlist per track it can read.

// ipv4Only pins the remuxer's own connection to IPv4, and it is load-bearing on
// a dual-stack host. The extractor is forced to IPv4 (`-4`: the site challenges
// IPv6 with "Sign in to confirm you're not a bot", and a challenge is not
// retryable), so the media URL it hands out carries `ip=<that address>`; the CDN
// then refuses a request that arrives from any other address, over IPv6 with a
// bare "403 Forbidden". ffmpeg prefers IPv6 where the host has it, and the remux
// died on its first read while the same URL worked when the extractor fetched it.
// A local address of 0.0.0.0 makes ffmpeg open an IPv4 socket (it tries the IPv6
// candidate, cannot bind it, and falls through), which matches the URL it was
// given.
var ipv4Only = []string{"-local_addr", "0.0.0.0"}

// streamEvictIdle is how quiet a stream must be before a new resolve may take its
// place when the pool is full. Long enough to never break playback that is
// merely thinking (a player rebuffering), short enough that a handful of
// abandoned streams cannot make the plugin refuse to start one more.
const streamEvictIdle = 20 * time.Second

// streamIdleTimeout is how long a stream that nobody ever fetched survives. A
// stream in use is bounded by its own response: when the player disconnects the
// body ends and the process goes with it. This only covers the window between a
// resolve handing out a URL and the player opening it.
const streamIdleTimeout = 90 * time.Second

// The playlists the remux writes. One variant per track, joined by the master;
// see the file comment for why they are not one playlist.
const (
	masterPlaylist = "master.m3u8"
	videoVariant   = "v0"
	audioVariant   = "audio"
)

// hlsSegmentSeconds is what the muxer aims for between cuts. The tracks are cut
// independently, so the real durations differ per track and that is fine.
const hlsSegmentSeconds = 6

// hlsFirstSegmentSeconds is the length of the first segment only. It is what
// bounds how long a tune-in waits for the stream to be readable.
const hlsFirstSegmentSeconds = 2

// streamReadyTimeout bounds the wait for the first segment. Remuxing starts at
// the requested offset, so this is "however long the site takes to answer".
const streamReadyTimeout = 60 * time.Second

// playlistReadAttempts bounds how long a half-written playlist is waited for.
const playlistReadAttempts = 25

const playlistReadPause = 10 * time.Millisecond

// requestLogLimit caps the request log file.
const requestLogLimit = 256 * 1024

// streamMaxConcurrent caps simultaneously remuxing streams: a crossfade needs
// two, and nothing legitimate needs many more.
const streamMaxConcurrent = 4

type streams struct {
	mu      sync.Mutex
	byToken map[string]*stream
	baseURL string
	lf      net.Listener
	srv     *http.Server
	remuxer string
	clock   func() time.Time
}

// streamParams is what a token stands for: enough to start the same stream
// again when a player asks for the URL a second time.
type streamParams struct {
	inputs  []remuxInput
	startAt float64
	headers map[string]string
}

// stream is one primed remux, waiting for the player to open its URL.
type stream struct {
	token  string
	params streamParams

	mu      sync.Mutex
	lastUse time.Time
	procs   []*process
}

// process is one running remux and the directory it writes into.
type process struct {
	cmd *exec.Cmd
	dir string

	mu      sync.Mutex
	stderr  *tailBuffer
	exitErr error
}

func newStreams() (*streams, error) {
	// Loopback by default: a stream the player fetches from the same machine.
	// A host that runs the plugin elsewhere (a server, with the player on a TV)
	// sets PLAYHEADTV_PLUGIN_LISTEN to a reachable address and rewrites the host
	// of the URLs it hands to its client: the port, the token and the paths stay
	// this plugin's own.
	lf, err := net.Listen("tcp", listenAddress())
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddress(), err)
	}
	// Advertised as loopback always: a listener bound to every interface reports
	// "[::]:port" or "0.0.0.0:port", which no player can open. A host that is not on
	// this machine rewrites the host part of the URL it hands out.
	s := &streams{
		byToken: map[string]*stream{},
		baseURL: "http://127.0.0.1:" + strconv.Itoa(lf.Addr().(*net.TCPAddr).Port) + "/s/",
		lf:      lf,
		clock:   time.Now,
	}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.srv.Serve(lf) }()
	go s.reap()
	fmt.Fprintf(os.Stderr, "plugin-youtube: stream server on %s\n", s.baseURL)
	return s, nil
}

func (s *streams) init(remuxer string) { s.remuxer = remuxer }

// listenAddress is where the stream server binds.
func listenAddress() string {
	if addr := strings.TrimSpace(os.Getenv("PLAYHEADTV_PLUGIN_LISTEN")); addr != "" {
		return addr
	}
	return "127.0.0.1:0"
}

// open primes one stream from the given inputs and returns once the remuxer has
// produced something, so the host never hands its player a URL that cannot be
// read.
func (s *streams) open(ctx context.Context, inputs []remuxInput, startAt float64, headers map[string]string) (*stream, error) {
	// A full pool is not a reason to refuse a viewer: the stalest stream makes
	// room. Refusing here used to be the whole story, which turned one abandoned
	// playback into minutes of every new tune-in failing (the client retried, the
	// pool never drained).
	if evicted, err := s.makeRoom(); err != nil {
		return nil, err
	} else if evicted != nil {
		evicted.stop()
	}

	// Both inputs must begin at the same instant, and with -c copy the only
	// instant the video can begin on is a keyframe. Asking both for exactly
	// startAt gives the video the keyframe BEFORE it (up to a whole GOP, ~10s on
	// these sources) while the audio loses nothing, so the first seconds of the
	// stream have picture and no sound — and a player that follows the audio
	// clock has nothing to follow. Asking both for the keyframe itself removes
	// the gap: the audio is cut there too.
	probeStarted := s.clock()
	if keyframe, ok := s.keyframeAt(ctx, inputs, startAt, headers); ok {
		startAt = keyframe
		requestLog(nil, fmt.Sprintf("stream: keyframe at %.3fs in %s",
			keyframe, s.clock().Sub(probeStarted).Round(time.Millisecond)), 0, 0)
	}

	token, err := newToken()
	if err != nil {
		return nil, err
	}
	st := &stream{
		token:   token,
		params:  streamParams{inputs: inputs, startAt: startAt, headers: headers},
		lastUse: s.clock(),
	}
	var procs []*process
	for attempt := 0; attempt < refusedReadAttempts; attempt++ {
		if attempt > 0 {
			// A refusal is not a verdict. The source answers 403 for a freshly
			// resolved url on one of its nodes and serves the very same url a few
			// seconds later (measured: the url it had just refused opened five
			// seconds after, from the same machine and the same address). From
			// here the two look identical, so trying again tells them apart — and
			// the pause is the load-bearing part: retrying at once hits the same
			// node and is refused again.
			time.Sleep(refusedReadPause)
		}
		procs, err = s.start(st.params)
		if err == nil || !refusedByTheSource(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	st.procs = procs

	s.mu.Lock()
	s.byToken[token] = st
	s.mu.Unlock()
	return st, nil
}

func (s *streams) url(st *stream) string { return s.baseURL + st.token + "/" + masterPlaylist }

// start launches one remux writing a fragmented MP4 to a pipe, and reads enough
// of it to know the source works.
// start launches one remux per input, each writing its own variant playlist, and
// writes the master that joins them.
//
// One process per track, not one process with two inputs. Sharing a process made
// the tracks wait for each other: the muxer cuts the video on keyframes (up to a
// whole GOP apart) and the audio segment beside it is only written at the same
// moment, so the audio playlist lagged a segment behind the video one and a player
// following the audio clock stopped at every boundary. Independent processes each
// read their own source at their own speed.
func (s *streams) start(params streamParams) ([]*process, error) {
	dir, err := os.MkdirTemp("", "playheadtv-yt-")
	if err != nil {
		return nil, fmt.Errorf("create stream directory: %w", err)
	}
	var procs []*process
	for _, in := range params.inputs {
		variant := videoVariant
		if in.selector == "a" {
			variant = audioVariant
		}
		if err := os.MkdirAll(filepath.Join(dir, variant), 0o700); err != nil {
			stopAll(procs)
			os.RemoveAll(dir)
			return nil, fmt.Errorf("create stream directory: %w", err)
		}
		args := []string{"-hide_banner", "-nostdin", "-loglevel", "error", "-y"}
		args = append(args, headerArgs(params.headers)...)
		args = append(args, ipv4Only...)
		if params.startAt > 0 {
			args = append(args, "-ss", strconv.FormatFloat(params.startAt, 'f', 3, 64))
		}
		args = append(args,
			"-i", in.url,
			// One input per process, so the map always addresses input 0.
			"-map", fmt.Sprintf("0:%s:0", in.selector),
			"-c", "copy",
			"-f", "hls",
			"-hls_time", strconv.Itoa(hlsSegmentSeconds),
			// The first segment is the only one a tune-in waits for, so it is cut
			// early: a viewer waits two seconds of media for the stream to exist,
			// not six.
			"-hls_init_time", strconv.Itoa(hlsFirstSegmentSeconds),
			// An event playlist: it begins where the host asked and grows to the end
			// of the item, keeping every segment behind it. A sliding live window
			// lists one segment at the moment a player opens it, and a player that
			// follows a live edge has no cushion.
			"-hls_playlist_type", "event",
			// Fragmented MP4, not MPEG-TS: the sources are already fMP4, and TS would
			// make every player reverse a repackaging it never needed.
			"-hls_segment_type", "fmp4",
			"-hls_flags", "independent_segments+temp_file",
			"-hls_segment_filename", filepath.Join(dir, variant, "seg%05d.m4s"),
			filepath.Join(dir, variant, "index.m3u8"),
		)
		// Not CommandContext: the caller's context ends when the resolve answers, and
		// a remux tied to it was killed one segment later (reproduced). Its life is
		// the stream's, ended by stop().
		cmd := exec.Command(s.remuxer, args...)
		proc := &process{cmd: cmd, dir: dir, stderr: &tailBuffer{}}
		cmd.Stdout = proc.stderr
		cmd.Stderr = proc.stderr
		if err := cmd.Start(); err != nil {
			stopAll(procs)
			os.RemoveAll(dir)
			return nil, fmt.Errorf("start remux: %w", err)
		}
		go func() {
			// The exit is kept, not dropped: a remux that dies while a player watches
			// leaves a playlist with nothing new in it, which the player shows as a
			// frozen picture with no error.
			err := cmd.Wait()
			proc.markExited(err)
			if reason, dead := proc.exited(); dead {
				requestLog(nil, "remux ended: "+reason, 0, 0)
			}
		}()
		procs = append(procs, proc)
	}
	if err := writeMaster(dir); err != nil {
		stopAll(procs)
		os.RemoveAll(dir)
		return nil, err
	}
	if err := waitReady(dir, procs, streamReadyTimeout); err != nil {
		stopAll(procs)
		os.RemoveAll(dir)
		return nil, err
	}
	return procs, nil
}

// How a refused first read is retried before it counts as the item's problem.
// Two pauses of two seconds stay well inside the resolve budget the host allows
// and cover the window in which the source was measured to answer differently.
const (
	refusedReadAttempts = 3
	refusedReadPause    = 2 * time.Second
)

// refusedByTheSource reports a failure the source answered with a refusal, rather
// than one that says the item itself cannot be played.
func refusedByTheSource(err error) bool {
	var coded *codedError
	if !errors.As(err, &coded) {
		return false
	}
	msg := strings.ToLower(coded.msg)
	return strings.Contains(msg, "403") || strings.Contains(msg, "forbidden")
}

// writeMaster writes the playlist that joins the two variants. Written here
// rather than by the muxer because the muxer only writes one when it produces
// both variants itself.
func writeMaster(dir string) error {
	body := "#EXTM3U\n#EXT-X-VERSION:7\n" +
		"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"audio\",DEFAULT=YES,URI=\"" + audioVariant + "/index.m3u8\"\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO=\"aud\"\n" +
		videoVariant + "/index.m3u8\n"
	path := filepath.Join(dir, masterPlaylist)
	if err := os.WriteFile(path+".tmp", []byte(body), 0o600); err != nil {
		return fmt.Errorf("write master playlist: %w", err)
	}
	return os.Rename(path+".tmp", path)
}

func stopAll(procs []*process) {
	for _, proc := range procs {
		proc.stop()
	}
}

// waitReady blocks until both variant playlists list a segment, so the host never
// hands its player a URL that has nothing behind it.
func waitReady(dir string, procs []*process, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if listed(dir, videoVariant) && listed(dir, audioVariant) {
			return nil
		}
		alive := false
		for _, proc := range procs {
			if proc.running() {
				alive = true
			}
		}
		if !alive {
			var msg strings.Builder
			for _, proc := range procs {
				if detail := strings.TrimSpace(proc.stderr.String()); detail != "" {
					msg.WriteString(detail)
					msg.WriteString(" ")
				}
			}
			if msg.Len() == 0 {
				return errf(codeUnavailable, "remux exited before the first segment")
			}
			return errf(codeUnavailable, "%s", strings.TrimSpace(msg.String()))
		}
		time.Sleep(150 * time.Millisecond)
	}
	return errf(codeUnavailable, "no segment after %s", timeout)
}

func listed(dir, variant string) bool {
	body, err := os.ReadFile(filepath.Join(dir, variant, "index.m3u8"))
	return err == nil && strings.Contains(string(body), "#EXTINF")
}

// markExited records that the remux ended, keeping the last of its diagnostic
// output as the reason. The first reason wins: a process only ends once.
func (p *process) markExited(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exitErr != nil {
		return
	}
	if err == nil {
		err = errors.New("the remux finished")
	}
	if detail := strings.TrimSpace(p.stderr.String()); detail != "" {
		err = fmt.Errorf("%w: %s", err, detail)
	}
	p.exitErr = err
}

// exited reports whether the remux ended, and why.
func (p *process) exited() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exitErr == nil {
		return "", false
	}
	return p.exitErr.Error(), true
}

func (p *process) running() bool {
	if p.cmd == nil || p.cmd.ProcessState != nil {
		return false
	}
	return true
}

// stop kills the remux.
func (p *process) stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	if p.dir != "" {
		os.RemoveAll(p.dir)
	}
}

// stop kills this stream's remux, if it still has one.
func (st *stream) stop() {
	st.mu.Lock()
	procs := st.procs
	st.procs = nil
	st.mu.Unlock()
	stopAll(procs)
}

func (st *stream) paramsCopy() streamParams {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.params
}

func (st *stream) touch(now time.Time) {
	st.mu.Lock()
	st.lastUse = now
	st.mu.Unlock()
}

func (st *stream) lastUsed() time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.lastUse
}

// makeRoom stops and returns the least recently used stream when the pool is
// full and that stream has been quiet for [streamEvictIdle]. It returns a nil
// stream when there is room, and an error only when every slot is in use.
func (s *streams) makeRoom() (*stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := 0
	var stalest *stream
	for token, st := range s.byToken {
		st.mu.Lock()
		alive := false
		for _, proc := range st.procs {
			if proc.running() {
				alive = true
			}
		}
		st.mu.Unlock()
		if !alive {
			// A process that already exited holds a slot for nothing.
			delete(s.byToken, token)
			continue
		}
		live++
		if stalest == nil || st.lastUsed().Before(stalest.lastUsed()) {
			stalest = st
		}
	}
	if live < streamMaxConcurrent {
		return nil, nil
	}
	if stalest == nil || s.clock().Sub(stalest.lastUsed()) < streamEvictIdle {
		return nil, errf(codeUnavailable, "already remuxing %d streams, all in use", live)
	}
	delete(s.byToken, stalest.token)
	return stalest, nil
}

func (s *streams) forget(token string) {
	s.mu.Lock()
	delete(s.byToken, token)
	s.mu.Unlock()
}

// reap frees streams nothing ever fetched.
func (s *streams) reap() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := s.clock()
		var stale []*stream
		s.mu.Lock()
		for token, st := range s.byToken {
			if now.Sub(st.lastUsed()) > streamIdleTimeout {
				stale = append(stale, st)
				delete(s.byToken, token)
			}
		}
		s.mu.Unlock()
		for _, st := range stale {
			st.stop()
		}
	}
}

func (s *streams) close() {
	s.mu.Lock()
	all := make([]*stream, 0, len(s.byToken))
	for _, st := range s.byToken {
		all = append(all, st)
	}
	s.byToken = map[string]*stream{}
	s.mu.Unlock()
	for _, st := range all {
		st.stop()
	}
	_ = s.srv.Close()
}

// ServeHTTP serves one stream's files: the master playlist, the two variant
// playlists, their init segments and their segments. Only a client holding the
// token can read them, and the token is unguessable.
func (s *streams) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/s/")
	token, name, _ := strings.Cut(rest, "/")
	s.mu.Lock()
	st, ok := s.byToken[token]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	st.touch(s.clock())

	st.mu.Lock()
	procs := st.procs
	st.mu.Unlock()
	if len(procs) == 0 {
		http.NotFound(w, r)
		return
	}
	// Either track reports the stream's death: both are the same source.
	proc := procs[0]
	for _, p := range procs {
		if reason, dead := p.exited(); dead {
			proc = p
			_ = reason
			break
		}
	}

	// Only the files this plugin wrote, under this stream's own directory: the
	// name is resolved and then checked to be inside it.
	full := filepath.Join(proc.dir, filepath.Clean("/"+name))
	if !strings.HasPrefix(full, filepath.Clean(proc.dir)+string(os.PathSeparator)) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if strings.HasSuffix(name, ".m3u8") {
		// A playlist is rewritten in place, and a player that reads it mid-write
		// gets a truncated one: no error it can explain, just "source error" and a
		// dead stream (reproduced: a channel played ~20s and then failed on a
		// playlist refresh). Only a complete playlist is served, and a half-written
		// one is re-read a moment later.
		body, ok := completePlaylist(full)
		if !ok {
			reason, dead := proc.exited()
			requestLog(r, rest, http.StatusNotFound, 0)
			if dead {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusGone)
				_, _ = io.WriteString(w, "remux ended: "+reason)
				return
			}
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		requestLog(r, rest, http.StatusOK, len(body))
		return
	}
	if strings.HasSuffix(name, ".m4s") || strings.HasSuffix(name, ".mp4") {
		w.Header().Set("Content-Type", "video/mp4")
	}
	if _, err := os.Stat(full); err != nil {
		reason, dead := proc.exited()
		requestLog(r, rest, http.StatusNotFound, 0)
		if dead {
			// 410, not 404: the stream existed and is gone, so a player stops
			// instead of retrying a URL that will never work again. The reason is
			// the only explanation anyone will get.
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusGone)
			_, _ = io.WriteString(w, "remux ended: "+reason)
			return
		}
		http.NotFound(w, r)
		return
	}
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	http.ServeFile(rec, r, full)
	requestLog(r, rest, rec.status, rec.written)
}

// requestLog keeps the last requests in a file next to the streams, so a player
// failure on a device with no readable log can still be traced: the plugin's own
// stderr only reaches the host's debug log.
func requestLog(r *http.Request, path string, status, written int) {
	name := filepath.Join(os.TempDir(), "playheadtv-yt-requests.log")
	if info, err := os.Stat(name); err == nil && info.Size() > requestLogLimit {
		// Bounded: this is a diagnostic, not a journal.
		_ = os.Truncate(name, 0)
	}
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	method, rng := "-", ""
	if r != nil {
		method, rng = r.Method, r.Header.Get("Range")
	}
	fmt.Fprintf(f, "%s %s %s -> %d (%d) range=%s\n",
		time.Now().UTC().Format("15:04:05.000"), method, path, status, written, rng)
}

// completePlaylist reads a playlist and returns it only when it is whole: the
// remuxer rewrites the file in place, so a read can land in the middle of it. It
// retries briefly, because the rewrite is short.
func completePlaylist(path string) ([]byte, bool) {
	for attempt := 0; attempt < playlistReadAttempts; attempt++ {
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, false
		}
		text := string(body)
		// A variant lists segments; the master lists variants. Both end with a
		// newline only when they were written to the end.
		if strings.HasPrefix(text, "#EXTM3U") && strings.HasSuffix(text, "\n") &&
			(strings.Contains(text, "#EXTINF") || strings.Contains(text, "#EXT-X-STREAM-INF")) {
			return body, true
		}
		time.Sleep(playlistReadPause)
	}
	return nil, false
}

// statusRecorder remembers what a response ended up being, so the log says which
// request failed and with what.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.written += n
	return n, err
}

type remuxInput struct {
	url      string
	selector string // "v" or "a"
}

// headerArgs turns upstream headers into remuxer flags. User-Agent is set with
// its own flag because that is the only header the remuxer treats specially.
func headerArgs(headers map[string]string) []string {
	if len(headers) == 0 {
		return nil
	}
	var args []string
	var rest []string
	for k, v := range headers {
		if strings.EqualFold(k, "User-Agent") {
			args = append(args, "-user_agent", v)
			continue
		}
		rest = append(rest, k+": "+v+"\r\n")
	}
	if len(rest) > 0 {
		args = append(args, "-headers", strings.Join(rest, ""))
	}
	return args
}

func newToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// tailBuffer keeps the last of a process's diagnostic output.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

const tailLimit = 4 * 1024

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > tailLimit {
		t.buf = t.buf[len(t.buf)-tailLimit:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// keyframeProbeWindow is how far before the target the keyframe probe starts
// looking, and keyframeProbeTimeout bounds it: the probe reads at most that much
// before the seek target, so it is a few megabytes and a second or two.
var (
	keyframeProbeWindow  = 20 * time.Second
	keyframeProbeTimeout = 20 * time.Second
	keyframePts          = regexp.MustCompile(`iskey:1.*?pts_time:([0-9.]+)|pts_time:([0-9.]+).*?iskey:1`)
)

// keyframeAt reports the last keyframe at or before target, or false when the
// inputs cannot be probed (a single input needs no probing: its own timeline is
// the timeline).
func (s *streams) keyframeAt(ctx context.Context, inputs []remuxInput, target float64, headers map[string]string) (float64, bool) {
	if len(inputs) < 2 || target <= 0 {
		return 0, false
	}
	var video string
	for _, in := range inputs {
		if in.selector == "v" {
			video = in.url
			break
		}
	}
	if video == "" {
		return 0, false
	}
	// The seek point can be a full GOP before the target, so the probe starts a
	// window earlier and asks the decoder for its first frame: the decoder always
	// begins on a keyframe, and -copyts keeps that frame's timestamp in source
	// time (without it the probe would report the seek point as zero).
	from := target - keyframeProbeWindow.Seconds()
	if from < 0 {
		from = 0
	}
	probeCtx, cancel := context.WithTimeout(ctx, keyframeProbeTimeout)
	defer cancel()
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "info", "-copyts",
		"-ss", strconv.FormatFloat(from, 'f', 3, 64), "-noaccurate_seek"}
	args = append(args, headerArgs(headers)...)
	args = append(args, ipv4Only...)
	args = append(args, "-i", video, "-map", "0:v:0", "-skip_frame", "nokey",
		"-vf", "showinfo", "-frames:v", "1", "-f", "null", "-")
	// Combined output on purpose: ffmpeg writes the filter report to stderr and
	// exits non-zero when it is killed at the end of the probe, which is the
	// expected outcome here — the report is still what we came for.
	out, _ := exec.CommandContext(probeCtx, s.remuxer, args...).CombinedOutput()
	return parseKeyframePts(string(out), target)
}

// parseKeyframePts reads the timestamp of the first keyframe the probe decoded.
// Split out of keyframeAt so the shape of the remuxer's report is pinned by a
// test rather than by whoever next reads this code.
func parseKeyframePts(report string, target float64) (float64, bool) {
	for _, m := range keyframePts.FindAllStringSubmatch(report, -1) {
		value := m[1]
		if value == "" {
			value = m[2]
		}
		at, err := strconv.ParseFloat(value, 64)
		if err != nil || at <= 0 || at > target {
			continue
		}
		return at, true
	}
	return 0, false
}
