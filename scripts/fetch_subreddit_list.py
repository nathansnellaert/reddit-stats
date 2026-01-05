#!/usr/bin/env python3
"""Fetch top subreddits from Arctic Shift data.

Arctic Shift provides comprehensive Reddit archives including subreddit metadata
with subscriber counts. This script downloads and processes their data to extract
the top subreddits by subscriber count.

Usage:
    1. Download the subreddits file from Arctic Shift:
       https://github.com/ArthurHeitmann/arctic_shift/releases/tag/2025_01_subreddits

       Download: subreddits_2025-01.zst (via Filen link on release page)

    2. Run this script:
       python scripts/fetch_subreddit_list.py subreddits_2025-01.zst

    3. Output: subreddits.json with top 100k subreddits by subscriber count
"""

import json
import sys
from pathlib import Path

try:
    import zstandard as zstd
except ImportError:
    print("Install zstandard: pip install zstandard")
    sys.exit(1)


def process_arctic_shift_file(input_path: Path, output_path: Path, top_n: int = 100_000):
    """Process Arctic Shift subreddits file and extract top N by subscribers."""

    print(f"Reading {input_path}...")

    subreddits = []

    # Decompress and read NDJSON
    with open(input_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            # Read in chunks and process lines
            buffer = b''
            total = 0

            while True:
                chunk = reader.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break

                buffer += chunk
                lines = buffer.split(b'\n')
                buffer = lines[-1]  # Keep incomplete line

                for line in lines[:-1]:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        name = data.get('display_name') or data.get('name')
                        subscribers = data.get('subscribers', 0) or 0

                        if name and subscribers > 0:
                            subreddits.append((name, subscribers))

                        total += 1
                        if total % 100_000 == 0:
                            print(f"  Processed {total:,} subreddits...")

                    except json.JSONDecodeError:
                        continue

            # Process final buffer
            if buffer.strip():
                try:
                    data = json.loads(buffer)
                    name = data.get('display_name') or data.get('name')
                    subscribers = data.get('subscribers', 0) or 0
                    if name and subscribers > 0:
                        subreddits.append((name, subscribers))
                except json.JSONDecodeError:
                    pass

    print(f"  Total subreddits with subscribers: {len(subreddits):,}")

    # Sort by subscriber count descending
    print("Sorting by subscriber count...")
    subreddits.sort(key=lambda x: x[1], reverse=True)

    # Take top N
    top_subreddits = [name for name, _ in subreddits[:top_n]]

    print(f"Top {len(top_subreddits):,} subreddits:")
    print(f"  #1: {top_subreddits[0]} ({subreddits[0][1]:,} subscribers)")
    print(f"  #10: {top_subreddits[9]} ({subreddits[9][1]:,} subscribers)")
    print(f"  #100: {top_subreddits[99]} ({subreddits[99][1]:,} subscribers)")
    print(f"  #1000: {top_subreddits[999]} ({subreddits[999][1]:,} subscribers)")
    if len(top_subreddits) >= 10000:
        print(f"  #10000: {top_subreddits[9999]} ({subreddits[9999][1]:,} subscribers)")
    if len(top_subreddits) >= 100000:
        print(f"  #100000: {top_subreddits[99999]} ({subreddits[99999][1]:,} subscribers)")

    # Save to JSON
    print(f"Saving to {output_path}...")
    with open(output_path, 'w') as f:
        json.dump(top_subreddits, f, indent=2)

    print(f"Done! Saved {len(top_subreddits):,} subreddits to {output_path}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"Error: {input_path} not found")
        print()
        print("Download from: https://github.com/ArthurHeitmann/arctic_shift/releases/tag/2025_01_subreddits")
        sys.exit(1)

    # Output to repo root
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    output_path = repo_root / "subreddits.json"

    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000

    process_arctic_shift_file(input_path, output_path, top_n)


if __name__ == "__main__":
    main()
