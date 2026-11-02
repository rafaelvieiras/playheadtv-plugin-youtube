#!/bin/sh
# Builds one download artifact per platform and the registry document that lists
# them (docs/PLUGINS.md in the Playhead TV repo).
#
# Each artifact is self-contained: the plugin binary, the extractor it drives and
# the remuxer it may use. Installing the plugin is the only step a host takes; it
# never installs a system package.
#
#   ./build.sh                    # build every target
#   VERSION=2026.10.26 ./build.sh # name the release explicitly
#
# Output: dist/plugin-youtube-<os>-<arch>.tar.gz and dist/registry.json.
set -eu

VERSION=${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo 0.0.0-dev)}
VERSION=${VERSION#v}
REPO=${REPO:-github.com/rafaelvieiras/playheadtv-plugin-youtube}
BASE_URL=${BASE_URL:-https://$REPO/releases/download/v$VERSION}
CACHE=${CACHE:-.cache}
OUT=${OUT:-dist}

EXTRACTOR_BASE=${EXTRACTOR_BASE:-https://github.com/yt-dlp/yt-dlp/releases/latest/download}
REMUXER_BASE=${REMUXER_BASE:-https://github.com/BtbN/FFmpeg-Builds/releases/download/latest}
REMUXER_WINDOWS=${REMUXER_WINDOWS:-$REMUXER_BASE/ffmpeg-master-latest-win64-gpl.zip}

mkdir -p "$CACHE" "$OUT"

fetch() {
	url=$1
	dest=$2
	[ -f "$dest" ] || curl -fsSL "$url" -o "$dest"
}

# unpack_remuxer <archive> <member-pattern> <dest>
unpack_remuxer() {
	archive=$1
	pattern=$2
	dest=$3
	case $archive in
	*.zip)
		unzip -o -j "$archive" "$pattern" -d "$(dirname "$dest")" >/dev/null
		;;
	*.tar.xz)
		tmp=$(mktemp -d)
		tar -xJf "$archive" -C "$tmp" --wildcards "$pattern"
		find "$tmp" -name "$(basename "$dest")" -type f -exec cp {} "$dest" \;
		rm -rf "$tmp"
		;;
	esac
	chmod 0755 "$dest"
}

manifest() {
	cat <<EOF
{
  "id": "youtube",
  "name": "YouTube",
  "description": "Channels, playlists and single videos from YouTube.",
  "version": "$VERSION",
  "protocol": 1,
  "entrypoint": "plugin-youtube$1",
  "capabilities": ["catalog.list", "media.resolve"]
}
EOF
}

artifacts=""
build_target() {
	os=$1
	arch=$2
	ext=$3

	name="plugin-youtube-$os-$arch"
	stage="$OUT/$name"
	rm -rf "$stage"
	mkdir -p "$stage"

	echo "==> $name"
	CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "-s -w" -o "$stage/plugin-youtube$ext" .

	case $os in
	linux)
		case $arch in
		amd64) fetch "$EXTRACTOR_BASE/yt-dlp_linux" "$CACHE/yt-dlp-linux-amd64" ;;
		arm64) fetch "$EXTRACTOR_BASE/yt-dlp_linux_aarch64" "$CACHE/yt-dlp-linux-arm64" ;;
		*) echo "no extractor for $os/$arch" >&2; exit 1 ;;
		esac
		cp "$CACHE/yt-dlp-linux-$arch" "$stage/yt-dlp"
		chmod 0755 "$stage/yt-dlp"
		# Not johnvansickle's static build: it segfaults on the media URLs an
		# extractor hands out (reproduced on every item of a real playlist), which
		# made every `remux` fail with "exited before the first segment". These
		# builds were verified against real source URLs, with -re and two inputs.
		fetch "$REMUXER_BASE/ffmpeg-master-latest-linux$([ "$arch" = amd64 ] && echo 64 || echo arm64)-gpl.tar.xz" \
			"$CACHE/ffmpeg-linux-$arch.tar.xz"
		unpack_remuxer "$CACHE/ffmpeg-linux-$arch.tar.xz" "*/bin/ffmpeg" "$stage/ffmpeg"
		;;
	windows)
		fetch "$EXTRACTOR_BASE/yt-dlp.exe" "$CACHE/yt-dlp-windows-amd64.exe"
		cp "$CACHE/yt-dlp-windows-amd64.exe" "$stage/yt-dlp.exe"
		fetch "$REMUXER_WINDOWS" "$CACHE/ffmpeg-windows-amd64.zip"
		unpack_remuxer "$CACHE/ffmpeg-windows-amd64.zip" "*/bin/ffmpeg.exe" "$stage/ffmpeg.exe"
		;;
	*)
		echo "unsupported target $os/$arch" >&2
		exit 1
		;;
	esac

	manifest "$ext" >"$stage/plugin.json"

	tarball="$OUT/$name.tar.gz"
	# Flat archive: the host extracts into a directory it owns and runs the
	# entrypoint by name, so there is no top-level directory to strip.
	tar -czf "$tarball" -C "$stage" .
	sha=$(sha256sum "$tarball" | cut -d' ' -f1)
	size=$(wc -c <"$tarball" | tr -d ' ')
	echo "    $sha  $size bytes"
	artifacts="$artifacts $os $arch $sha $size"
}

# Targets can be narrowed for a local run: ./build.sh linux/amd64
if [ $# -gt 0 ]; then
	for target in "$@"; do
		case $target in
		linux/amd64) build_target linux amd64 "" ;;
		linux/arm64) build_target linux arm64 "" ;;
		windows/amd64) build_target windows amd64 ".exe" ;;
		*) echo "unknown target $target" >&2; exit 1 ;;
		esac
	done
else
	build_target linux amd64 ""
	build_target linux arm64 ""
	build_target windows amd64 ".exe"
fi

# Registry document. Hosts refuse an artifact with no hash, so every entry here
# carries one, computed from the bytes this run produced.
{
	printf '{\n  "schema": 1,\n  "plugins": [\n    {\n'
	printf '      "id": "youtube",\n      "name": "YouTube",\n'
	printf '      "description": "Channels, playlists and single videos from YouTube.",\n'
	printf '      "versions": [\n        {\n          "version": "%s",\n          "protocol": 1,\n          "artifacts": [\n' "$VERSION"
	first=1
	set -- $artifacts
	while [ $# -gt 0 ]; do
		os=$1
		arch=$2
		sha=$3
		size=$4
		shift 4
		[ $first -eq 1 ] || printf ',\n'
		first=0
		printf '            {\n              "os": "%s",\n              "arch": "%s",\n              "url": "%s/plugin-youtube-%s-%s.tar.gz",\n              "sha256": "%s",\n              "size": %s\n            }' \
			"$os" "$arch" "$BASE_URL" "$os" "$arch" "$sha" "$size"
	done
	printf '\n          ]\n        }\n      ]\n    }\n  ]\n}\n'
} >"$OUT/registry.json"

echo
echo "dist/registry.json:"
cat "$OUT/registry.json"
