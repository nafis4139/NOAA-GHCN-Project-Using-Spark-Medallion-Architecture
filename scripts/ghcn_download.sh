#!/usr/bin/env bash
# Download NOAA GHCN-Daily by-year files and produce raw .txt outputs
# Downloads directly from NCEI using wget.

# Standard safety settings for Bash scripts
# Exit immediately on error (-e), treat unset variables as errors (-u),
# and ensure that any failure in a pipeline causes the whole pipeline to fail (-o pipefail)
set -euo pipefail

# Years to download
YEARS=()
if [[ $# -gt 0 ]]; then
  for y in "$@"; do YEARS+=("$y"); done
else
  for y in $(seq 2010 2025); do YEARS+=("$y"); done
fi

# Output directory
DATA_DIR=${DATA_DIR:-"/home/ubuntu/spark-notebooks/project/data/raw"}
mkdir -p "$DATA_DIR"

# Direct public NCEI URL base:
HTTPS_BASE="https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year"

printf "Downloading to: %s\n" "$DATA_DIR"

# Download and decompress each requested year
for year in "${YEARS[@]}"; do
  gz_target="$DATA_DIR/${year}.csv.gz"
  raw_target="$DATA_DIR/${year}.txt"

  # Skip if final .txt already exists
  if [[ -f "$raw_target" ]]; then
    printf "[skip] %s already exists\n" "$raw_target"
    continue
  fi

  # Ensure wget is available and download the file
  if command -v wget >/dev/null 2>&1; then
    printf "[wget] %s\n" "$year"
    if wget --tries=3 --continue -O "$gz_target" "$HTTPS_BASE/${year}.csv.gz"; then

      # Try available decompression tools in order
      # Decompress the .gz to a .txt
      if command -v gunzip >/dev/null 2>&1; then
        if gunzip -c "$gz_target" > "$raw_target"; then
          rm -f "$gz_target"
          printf "[decompress] %s -> %s\n" "$gz_target" "$raw_target"
          continue
        else
          printf "[warn] decompression failed for %s\n" "$gz_target" >&2
        fi
      elif command -v gzip >/dev/null 2>&1; then
        if gzip -dc "$gz_target" > "$raw_target"; then
          rm -f "$gz_target"
          printf "[decompress] %s -> %s\n" "$gz_target" "$raw_target"
          continue
        else
          printf "[warn] decompression failed for %s\n" "$gz_target" >&2
        fi
      elif command -v zcat >/dev/null 2>&1; then
        if zcat "$gz_target" > "$raw_target"; then
          rm -f "$gz_target"
          printf "[decompress] %s -> %s\n" "$gz_target" "$raw_target"
          continue
        else
          printf "[warn] decompression failed for %s\n" "$gz_target" >&2
        fi
      else
        printf "[error] no gunzip/gzip/zcat available to decompress %s\n" "$gz_target" >&2
        exit 1
      fi
      
    else
      printf "[warn] wget failed for %s\n" "$year" >&2
    fi
  else
    printf "[error] wget is not installed. Please install wget and retry.\n" >&2
    exit 1
  fi

done

# Show storage usage
du -sh "$DATA_DIR" || true
printf "\nCombining per-year TXT files into one file...\n"

# Create combined file
COMBINED="$DATA_DIR/ghcn_all_years.txt"
# create or truncate combined file
: > "$COMBINED"

# Combine all yearly .txt files
shopt -s nullglob
txt_count=0
for f in "$DATA_DIR"/*.txt; do
  # avoid including the combined file itself if it already exists
  if [[ "$f" == "$COMBINED" ]]; then
    continue
  fi
  printf "[combine] adding %s\n" "$(basename "$f")"
  cat "$f" >> "$COMBINED"
  txt_count=$((txt_count+1))
done
shopt -u nullglob

# Final status message
if [[ $txt_count -eq 0 ]]; then
  printf "[warn] no .txt files found to combine in %s\n" "$DATA_DIR"
else
  printf "[done] combined %d files -> %s\n" "$txt_count" "$COMBINED"
fi

printf "\nDone. Summary:\n"
du -sh "$DATA_DIR" || true
ls -lh "$DATA_DIR" | head -n 20
