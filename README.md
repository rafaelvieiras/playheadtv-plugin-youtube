# Playhead TV plugin: YouTube

A source plugin for [Playhead TV](../playheadtv) — the self-hosted simulated
linear TV app. It teaches Playhead TV to list and play videos from YouTube:
channel uploads, playlists, and single videos.

It implements the plugin contract in `docs/PLUGINS.md` (in the Playhead TV
repository): protocol 1, one JSON object per line over stdin/stdout, capabilities
`catalog.list` and `media.resolve`.

## What it deliberately does not do

**No transcoding, ever.** The plugin never decodes or re-encodes video. Which
shape it answers with is decided by what the host says it can play:

| mode | what the plugin does | cost |
| --- | --- | --- |
| `single` | hands over the source's own URL, one file carrying both tracks | nothing |
| `pair` | hands over a video URL and an audio URL; **the player** merges them | nothing |
| `remux` | produces a stream itself, `-c copy` into a 2-minute sliding HLS window | light CPU, bounded disk |

`pair` is the default. `remux` happens only when the host explicitly names it in
its request, and it is the only mode where this process touches media at all —
a container rewrite, not a transcode, with the window capped so it cannot grow on
a small machine (a 2 GB set-top box, a single-board computer).


## How the stream is shaped

The remux answers with **one continuous fragmented MP4 body**, not HLS:

- **one body, no segments and no playlist.** With `-c copy` the muxer cannot cut
  where it wants: the video can only be cut on a keyframe and the audio anywhere,
  so the two tracks of one stream end up with different boundaries and odd
  declared durations (measured in one stream: 3.0s, 12.28s and 0.13s, with the
  audio starting seconds after the video). A player reading that through its own
  HLS extractor stopped a few seconds into every stream with data still buffered —
  no error, no buffering flag, one frozen picture. A single body has no
  boundaries to disagree about.
- **fragmented MP4 with an empty moov.** The first bytes are playable and nothing
  has to be finished before it can be read.
- **backpressure, not a pacing flag.** Nothing paces the remux: it writes into the
  HTTP response, so when the player stops reading the socket fills, the write
  blocks and the muxer waits. No `-re`, no live edge to fall off, no disk.
- **both inputs cut at the same keyframe.** A probe finds the keyframe the seek
  lands on and asks the video *and* the audio for exactly that instant. Without it
  the video keeps its earlier keyframe while the audio is cut at the requested
  time, and the first seconds of the stream have picture and no sound.

The body only moves forward: a player may not seek it or resume it from an
offset. The client re-resolves at the position it needs.

## Install

The host installs plugins from a registry. Point it at this repository's
released registry document:

    PLAYHEADTV_PLUGIN_REGISTRY_URL=https://github.com/rafaelvieiras/playheadtv-plugin-youtube/releases/latest/download/registry.json

then install the plugin and leave it enabled (`POST /plugins/youtube/install` in
the REST API, or the `install_plugin` MCP tool).

Each artifact is self-contained: the plugin binary, the extractor it drives
(`yt-dlp`) and the remuxer it may use (`ffmpeg`). Nothing is installed on the
host system.

## Configure a source

Create a `plugin` media source whose config names what to list:

```json
{
  "type": "plugin",
  "name": "Blender channel",
  "plugin_id": "youtube",
  "plugin_config": {
    "url": "https://www.youtube.com/channel/UCSMOQeBJ2RAnuFungnQOxLg/videos",
    "max_items": 200
  }
}
```

| field | meaning |
| --- | --- |
| `url` | channel, playlist or single video page (required) |
| `kind` | `auto` (default), `channel`, `playlist`, `video` |
| `max_items` | how much of the list to read in one sync (default 500, max 5000) |
| `include_shorts` | list shorts too (default off) |
| `series_title` | group the items under this name in the guide |
| `max_height` | tallest video to play (default 720) |
| `audio_only` | play only the audio track, for radio-style channels |

Live streams, upcoming premieres, members-only videos, shorts and anything with
an unknown length are never listed: the host cannot schedule them.

## Protocol by hand

The plugin is an ordinary program that talks on stdin/stdout, so it can be driven
without a host. Send it one request per line:

    {"id":1,"method":"info"}
    {"id":2,"method":"catalog.list","params":{"config":{"url":"https://www.youtube.com/@BlenderOfficial/videos"},"cursor":"","limit":5}}
    {"id":3,"method":"media.resolve","params":{"config":{"url":"https://www.youtube.com/@BlenderOfficial/videos"},"item":{"external_id":"sO_lhW8av3U"},"modes":["single","pair"]}}

Run it with `PLAYHEADTV_PLUGIN_EXTRACTOR` and `PLAYHEADTV_PLUGIN_REMUXER`
pointing at the two helper programs (inside a released artifact they are simply
next to the binary). Those two environment variables exist for development and
tests; a host never sets them.

`PLAYHEADTV_PLUGIN_LISTEN` moves the remux server off loopback (`0.0.0.0:0` when
the player is on another machine). The URLs still name `127.0.0.1` and the bound
port: a host that is not on this machine rewrites the host part of the URL it
gives its client, and keeps everything else. A `remux` stream is produced at
playback speed (`-re`), so its sliding window always holds the media the player
is about to fetch, never a copy that ran ahead of it.

## Build

    ./build.sh                  # every target
    ./build.sh linux/amd64      # one target

Outputs `dist/plugin-youtube-<os>-<arch>.tar.gz` and `dist/registry.json` with a
SHA-256 per artifact. The release workflow does exactly this on a tag and
publishes both, so the registry can never describe bytes that were not released.

Artifacts are large (~115 MB) because they carry an `ffmpeg` build. Only remuxing
uses it, and only a minimal build would be enough; a slimmer artifact is a
possible follow-up.

The remuxer is a BtbN static build, not johnvansickle's: that one segfaults on the
media URLs an extractor hands out (every item of a real playlist reproduced it),
which surfaced as `remux exited before the first segment` on every play. Verify any
replacement against a real source URL, with `-re` and two inputs.

## Legal

This plugin is not affiliated with, endorsed by, or supported by YouTube. The
extractor it bundles is a third-party project with its own license. Whoever runs
this is responsible for the content played through it and for following the
source site's terms of service.
