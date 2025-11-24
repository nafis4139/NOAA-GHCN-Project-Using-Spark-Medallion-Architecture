#!/usr/bin/env bash
# Download NOAA GHCN-Daily metadata files (stations & inventory)

# Basic safety settings for Bash
# Exit immediately on error (-e), treat unset variables as errors (-u),
# and ensure that any failure in a pipeline causes the whole pipeline to fail (-o pipefail)
set -euo pipefail

# Base URL for metadata files
BASE_URL="https://www.ncei.noaa.gov/pub/data/ghcn/daily"

# Output directory
DATA_DIR="${DATA_DIR:-/home/ubuntu/spark-notebooks/project/data/meta}"

# Metadata files to download
FILES=("ghcnd-stations.txt" "ghcnd-inventory.txt")

# Temporary suffix for safe atomic writes
TMP_SUFFIX=".tmp.$$"

mkdir -p "$DATA_DIR"


# Check whether wget or curl is available
have_wget=false
have_curl=false
if command -v wget >/dev/null 2>&1; then
  have_wget=true
elif command -v curl >/dev/null 2>&1; then
  have_curl=true
else
  echo "[error] Neither wget nor curl is installed. Please install one of them." >&2
  exit 1
fi

echo "Downloading to: $DATA_DIR"

# Loop through the required metadata files
for f in "${FILES[@]}"; do
  url="${BASE_URL}/${f}"
  dest="${DATA_DIR}/${f}"
  tmp="${dest}${TMP_SUFFIX}"

  echo "[fetch] ${f}"

  # Download using wget if available, otherwise curl
  if $have_wget; then
    if wget -q --show-progress -O "$tmp" "$url"; then
      :
    else
      echo "[warn] wget failed for ${f}" >&2
      rm -f "$tmp"
      continue
    fi
  else
    # curl fallback
    if curl -fL --progress-bar -o "$tmp" "$url"; then
      :
    else
      echo "[warn] curl failed for ${f}" >&2
      rm -f "$tmp"
      continue
    fi
  fi

  # Ensure the downloaded file is not empty
  if [[ ! -s "$tmp" ]]; then
    echo "[warn] downloaded ${f} is empty; skipping replace" >&2
    rm -f "$tmp"
    continue
  fi

  # Move temporary file into place
  mv -f "$tmp" "$dest"
  echo "[saved] ${dest} ($(du -h "$dest" | awk '{print $1}'))"
done

echo "[done] Metadata available in: $DATA_DIR"
ls -lh "$DATA_DIR"
