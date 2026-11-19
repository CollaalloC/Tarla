#!/usr/bin/env bash
# Usage: package.sh <remote-only Release.app> <new Local.app path> <Developer ID identity or ->
set -euo pipefail
if [[ $# -ne 3 || "$(uname -s)" != Darwin ]]; then
  echo "usage (macOS): package.sh <source.app> <new-local.app> <Developer ID Application identity or ->" >&2
  exit 2
fi
src=$1
dst=$2
identity=$3
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../../.." && pwd)
if [[ ! -d "$src/Contents/MacOS" || -e "$dst" || "$dst" != *.app ]]; then
  echo "Provide an existing Flutter release bundle and a new .app destination." >&2
  exit 2
fi
if [[ $identity != '-' && $identity != 'Developer ID Application: '* ]]; then
  echo "Select a Developer ID Application signing identity from your own keychain." >&2
  exit 2
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
stage="$work/Tarla Local.app"
ditto "$src" "$stage"
contents="$stage/Contents"
app_id=dev.tarla.client.local
launch_id="$app_id.tarlad"
helper_dir="$contents/Helpers"
plist="$contents/Info.plist"
mkdir -p "$helper_dir" "$contents/Library/LaunchAgents"
install -m 644 "$here/$launch_id.plist" "$contents/Library/LaunchAgents/$launch_id.plist"
plutil -replace CFBundleIdentifier -string "$app_id" "$plist"
plutil -replace CFBundleDisplayName -string 'Tarla Local' "$plist"
plutil -replace NSLocalNetworkUsageDescription -string 'Tarla connects to devices you pair with this workspace.' "$plist"
# Local core management needs the user's private ~/.tarla tree. The remote-only
# source app keeps its original sandbox, identity and signatures.
plistbuddy=/usr/libexec/PlistBuddy
exe=$("$plistbuddy" -c 'Print CFBundleExecutable' "$plist")
# Flutter may emit a universal app even on a single-architecture build host.
# Match its actual slices, including both helpers for a universal distribution.
read -ra archs <<< "$(lipo -archs "$contents/MacOS/$exe")"
for arch in "${archs[@]}"; do
  case "$arch" in
    arm64) target=aarch64-apple-darwin ;;
    x86_64) target=x86_64-apple-darwin ;;
    *) echo "unsupported architecture: $arch" >&2; exit 2 ;;
  esac
  rustup target add "$target"
  (
    cd "$repo"
    export MACOSX_DEPLOYMENT_TARGET=13.0
    cargo build --locked --release --target "$target" -p tarla-cli --bin tarla
    cargo rustc --locked --release --target "$target" -p tarlad --bin tarlad -- \
      -C "link-arg=-Wl,-sectcreate,__TEXT,__info_plist,$here/daemon-info.plist"
  )
  target_dir=${CARGO_TARGET_DIR:-$repo/target}
  [[ $target_dir == /* ]] || target_dir="$repo/$target_dir"
  for helper in tarla tarlad; do
    cp "$target_dir/$target/release/$helper" "$work/$helper-$arch"
  done
done
for helper in tarla tarlad; do
  lipo -create "$work/$helper-"* -output "$helper_dir/$helper"
  chmod 755 "$helper_dir/$helper"
done
plutil -replace LSMinimumSystemVersion -string '13.0' "$plist"
sign=(codesign --force --sign "$identity")
if [[ $identity == '-' ]]; then sign+=(--timestamp=none); else sign+=(--timestamp --options runtime); fi
for helper in tarla tarlad; do
  "${sign[@]}" --identifier "$app_id.$helper" "$helper_dir/$helper"
done
# Sign nested code from the inside out; no recursive signature rewriting of
# unrelated files or arbitrary entitlements from the build host.
while IFS= read -r -d '' framework; do
  "${sign[@]}" "$framework"
done < <(find "$contents/Frameworks" -depth -type d -name '*.framework' -print0)
"${sign[@]}" "$stage"
codesign --verify --deep --strict --verbose=2 "$stage"
ditto "$stage" "$dst"
