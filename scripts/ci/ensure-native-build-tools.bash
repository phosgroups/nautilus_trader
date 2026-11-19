#!/usr/bin/env bash
set -euo pipefail

link_with_lld() {
  clang -fuse-ld=lld -x c - -o /dev/null <<< 'int main(void) { return 0; }'
}

missing=()
if ! clang --version >/dev/null 2>&1 || ! clang++ --version >/dev/null 2>&1; then
  missing+=(clang)
  ld.lld --version >/dev/null 2>&1 || missing+=(lld)
elif ! link_with_lld; then
  missing+=(lld)
fi
patchelf --version >/dev/null 2>&1 || missing+=(patchelf)

if (( ${#missing[@]} == 0 )); then
  echo "Native build tools available. Skipping apt."
  exit 0
fi

echo "Installing native build tools: ${missing[*]}"
sudo apt-get update
sudo apt-get install -y --no-install-recommends "${missing[@]}"

clang++ --version
patchelf --version
link_with_lld
