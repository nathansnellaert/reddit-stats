"""Fetch historical daily subscriber counts for top subreddits.

Data source: subredditstats.com API - Historical subscriber time series (10+ years)

Note: We read from a pre-fetched subreddits.json file containing top 100k subreddits
by subscriber count. This list was extracted from Arctic Shift's Reddit archive.

To refresh the list:
    1. Download subreddits_YYYY-MM.zst from:
       https://github.com/ArthurHeitmann/arctic_shift/releases
    2. Run: python scripts/fetch_subreddit_list.py <downloaded_file.zst>
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

from subsets_utils import get, save_raw_parquet, load_state, save_state


# Rate limits for subredditstats.com
STATS_CALLS_PER_MINUTE = 30

# Leave buffer for transforms + log upload (GitHub hard limit is 6h)
GH_ACTIONS_MAX_RUN_SECONDS = 5.8 * 60 * 60  # ~5h 48m


def load_subreddit_list() -> list[str]:
    """Load subreddit list from pre-fetched JSON file.

    Contains top 100k subreddits by subscriber count from Arctic Shift data.
    See module docstring for refresh instructions.
    """
    # Find subreddits.json relative to this file (in repo root)
    repo_root = Path(__file__).parent.parent.parent
    subreddits_file = repo_root / "subreddits.json"

    if not subreddits_file.exists():
        raise FileNotFoundError(
            f"subreddits.json not found at {subreddits_file}. "
            "Run the local fetch script to generate it."
        )

    with open(subreddits_file) as f:
        return json.load(f)


@sleep_and_retry
@limits(calls=STATS_CALLS_PER_MINUTE, period=60)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_subreddit_stats(subreddit: str) -> dict | None:
    """Fetch historical subscriber data from subredditstats.com."""
    url = f"https://subredditstats.com/api/subreddit"
    params = {"name": subreddit}

    response = get(url, params=params, timeout=60.0)

    if response.status_code == 404:
        return None

    response.raise_for_status()
    return response.json()


def utc_day_to_date(utc_day: int) -> str:
    """Convert UTC day number to ISO date string."""
    # UTC day is days since Unix epoch (1970-01-01)
    timestamp = utc_day * 86400
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def run() -> bool:
    """Fetch subscriber time series for top subreddits.

    Returns:
        bool: True if more work remains (should retrigger), False if complete
    """
    start_time = time.time()
    state = load_state("subreddit_subscribers")
    fetched_subreddits = set(state.get("fetched", []))

    # Load from pre-fetched file
    subreddits = load_subreddit_list()
    print(f"  Loaded {len(subreddits)} subreddits from subreddits.json")

    # Fetch historical data for each subreddit
    pending = [s for s in subreddits if s not in fetched_subreddits]
    print(f"  Fetching stats for {len(pending)} subreddits ({len(fetched_subreddits)} already done)...")

    for i, subreddit in enumerate(pending):
        # Check time budget before processing
        elapsed = time.time() - start_time
        if elapsed >= GH_ACTIONS_MAX_RUN_SECONDS:
            print(f"  Time budget exhausted after {elapsed/3600:.1f} hours, {len(pending) - i} subreddits remaining")
            return True  # More work to do

        print(f"    [{i+1}/{len(pending)}] {subreddit}...", end=" ", flush=True)

        try:
            stats = fetch_subreddit_stats(subreddit)

            if not stats:
                print("not found")
                fetched_subreddits.add(subreddit)
                save_state("subreddit_subscribers", {"fetched": list(fetched_subreddits)})
                continue

            time_series = stats.get("subscriberCountTimeSeries", [])

            if not time_series:
                print("no time series")
                fetched_subreddits.add(subreddit)
                save_state("subreddit_subscribers", {"fetched": list(fetched_subreddits)})
                continue

            # Convert to table format
            rows = []
            for point in time_series:
                utc_day = point.get("utcDay")
                count = point.get("count")
                if utc_day is not None and count is not None:
                    rows.append({
                        "subreddit": subreddit,
                        "date": utc_day_to_date(utc_day),
                        "subscribers": count
                    })

            if rows:
                # Create PyArrow table
                table = pa.table({
                    "subreddit": pa.array([r["subreddit"] for r in rows], type=pa.string()),
                    "date": pa.array([r["date"] for r in rows], type=pa.string()),
                    "subscribers": pa.array([r["subscribers"] for r in rows], type=pa.int64()),
                })

                save_raw_parquet(table, f"subscribers/{subreddit}")
                print(f"{len(rows)} days")
            else:
                print("empty")

            fetched_subreddits.add(subreddit)
            save_state("subreddit_subscribers", {"fetched": list(fetched_subreddits)})

        except Exception as e:
            print(f"error: {e}")
            continue

    print(f"  Done! Fetched data for {len(fetched_subreddits)} subreddits")
    return False  # All done
