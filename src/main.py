import argparse

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

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("\n--- Ingesting subreddit subscribers ---")
        subreddit_subscribers.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("  (no transforms yet - run profiler first)")


if __name__ == "__main__":
    main()
