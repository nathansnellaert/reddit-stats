import argparse
import sys

from subsets_utils import validate_environment
from ingest import subreddit_subscribers


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--transform-only", action="store_true")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    needs_continuation = False

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("\n--- Ingesting subreddit subscribers ---")
        needs_continuation = subreddit_subscribers.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("  (no transforms yet - run profiler first)")

    if needs_continuation:
        print("\nExiting with code 2 to signal continuation needed")
        sys.exit(2)


if __name__ == "__main__":
    main()
