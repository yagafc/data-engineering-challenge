#!/bin/bash
set -e

# Default values
BASE_URL="https://data.commoncrawl.org"
DEST_DIR="../../data/raw/"
NUM_SEGMENTS=3
CRAWL_DATA_VERSION="CC-MAIN-2024-51"
FORMAT="WET"
MAX_FILTERED=0  # Default 0: no limit for filtered paths

usage() {
  echo "Usage: $0 [--crawl-data-version VERSION] [--n-segment NUM_SEGMENTS] [--destination DEST_DIR] [--format FORMAT] [--max-filtered MAX_FILTERED]"
  echo "  --crawl-data-version VERSION   Specify the Common Crawl data version (e.g., CC-MAIN-2024-51)"
  echo "  --n-segment NUM_SEGMENTS       Specify the number of segments to download (default is 3)"
  echo "  --destination DEST_DIR         Specify the destination directory"
  echo "  --format FORMAT                Specify the file format (WARC, WAT, or WET, default is WET)"
  echo "  --max-filtered MAX_FILTERED     Limit the number of filtered paths (default is no limit)"
  echo "  --help                         Show this help message"
  exit 1
}

validate_format() {
  local valid_formats=("WET" "WARC" "WAT")
  local is_valid="false"
  for format in "${valid_formats[@]}"; do
    if [[ "$1" == "$format" ]]; then
      is_valid="true"
      break
    fi
  done
  if [[ "$is_valid" == "false" ]]; then
    echo "Error: Invalid format. Please specify one of: WET, WARC, or WAT."
    exit 1
  fi
}

get_latest_version() {
  echo "Fetching the latest Common Crawl version ..."
  echo "curl -s $BASE_URL/latest-crawl"
  CRAWL_DATA_VERSION=$(curl -s $BASE_URL/latest-crawl | grep -o 'CC-MAIN-[0-9]\{4\}-[0-9]\{2\}' | sort -V | tail -n 1)
  if [[ -z "$CRAWL_DATA_VERSION" ]]; then
    echo "Error retrieving the Common Crawl data version."
    exit 1
  fi
}

download_and_decompress() {
  local url=$1
  local dest=$2

  echo "Downloading file $url ..."

  if ! curl -s -o "$dest" "$url"; then
    echo "Error: Failed to download $url."
    exit 1
  fi

  if [[ ! -s "$dest" ]]; then
    echo "Error: Downloaded file $dest is empty or invalid."
    rm -f "$dest"
    exit 1
  fi

  if ! gzip -t "$dest"; then
    echo "Error: $dest is not a valid gzip file."
    rm -f "$dest"
    exit 1
  fi

  echo "Decompressing $dest ..."
  gzip -df "$dest"
}

download_filtered_paths() {
  local segment_file=$1
  local num_segments=$2
  local max_filtered=$3
  local lower_case_format=$4
  local dest_dir=$5

  segments=($(tail -n "$num_segments" "$segment_file"))

  if [[ "$max_filtered" -gt 0 ]]; then
    echo "Limiting the filtered paths to $max_filtered entries ..."
    cat "$segment_file" | grep -E "$(IFS=\|; echo "${segments[*]}")" | tail -n "$max_filtered" | while read -r path; do
      dir_path=$(dirname "$dest_dir/$path")
      mkdir -p "$dir_path"
      download_and_decompress "$BASE_URL/$path" "$dest_dir/$path"
    done
  else
    echo "Downloading all filtered paths ..."
    cat "$segment_file" | grep -E "$(IFS=\|; echo "${segments[*]}")" | while read -r path; do
      dir_path=$(dirname "$dest_dir/$path")
      mkdir -p "$dir_path"
      download_and_decompress "$BASE_URL/$path" "$dest_dir/$path"
    done
  fi
}

main() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      --crawl-data-version)
        CRAWL_DATA_VERSION="$2"
        shift 2
        ;;
      --n-segment)
        NUM_SEGMENTS="$2"
        shift 2
        ;;
      --destination)
        DEST_DIR="$2"
        shift 2
        ;;
      --format)
        FORMAT="$2"
        validate_format "$FORMAT"
        shift 2
        ;;
      --max-filtered)
        MAX_FILTERED="$2"
        shift 2
        ;;
      --help)
        usage
        ;;
      *)
        echo "Unknown option: $1"
        usage
        ;;
    esac
  done

  if [[ -z "$CRAWL_DATA_VERSION" ]]; then
    get_latest_version
  fi

  mkdir -p "$DEST_DIR/$CRAWL_DATA_VERSION"

  TEMP_PATHS_URL="$BASE_URL/crawl-data/$CRAWL_DATA_VERSION"

  download_and_decompress "$TEMP_PATHS_URL/segment.paths.gz" "$DEST_DIR/$CRAWL_DATA_VERSION/segment.paths.gz"

  LOWER_CASE_FORMAT=$(echo "$FORMAT" | tr '[:upper:]' '[:lower:]')

  download_and_decompress "$TEMP_PATHS_URL/$LOWER_CASE_FORMAT.paths.gz" "$DEST_DIR/$CRAWL_DATA_VERSION/$LOWER_CASE_FORMAT.paths.gz"

  download_filtered_paths "$DEST_DIR/$CRAWL_DATA_VERSION/$LOWER_CASE_FORMAT.paths" "$NUM_SEGMENTS" "$MAX_FILTERED" "$LOWER_CASE_FORMAT" "$DEST_DIR"

  echo "Download completed. Files saved to $DEST_DIR"
}

main "$@"