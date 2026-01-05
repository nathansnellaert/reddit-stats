"""Fetch historical daily subscriber counts for top subreddits.

Data sources:
- Reddit public API: List of popular subreddits
- subredditstats.com API: Historical subscriber time series (10+ years)
"""

from datetime import datetime, timezone
import time

import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

from subsets_utils import get, save_raw_parquet, load_state, save_state


# Rate limits
REDDIT_CALLS_PER_MINUTE = 30
STATS_CALLS_PER_MINUTE = 30

# How many subreddits to track
MAX_SUBREDDITS = 500


@sleep_and_retry
@limits(calls=REDDIT_CALLS_PER_MINUTE, period=60)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_popular_subreddits(after: str | None = None, limit: int = 100) -> dict:
    """Fetch popular subreddits from Reddit's public API."""
    url = "https://www.reddit.com/subreddits/popular.json"
    params = {"limit": limit, "raw_json": 1}
    if after:
        params["after"] = after

    response = get(
        url,
        params=params,
        headers={"User-Agent": "SubredditStats/1.0 (data collection)"},
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


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


def run():
    """Fetch subscriber time series for top subreddits."""
    state = load_state("subreddit_subscribers")
    fetched_subreddits = set(state.get("fetched", []))

    # Step 1: Get list of popular subreddits from Reddit
    print("  Fetching popular subreddits from Reddit...")
    subreddits = []
    after = None

    while len(subreddits) < MAX_SUBREDDITS:
        data = fetch_popular_subreddits(after=after)
        children = data.get("data", {}).get("children", [])

        if not children:
            break

        for child in children:
            sub_data = child.get("data", {})
            name = sub_data.get("display_name")
            if name and name not in subreddits:
                subreddits.append(name)

        after = data.get("data", {}).get("after")
        if not after:
            break

        print(f"    Found {len(subreddits)} subreddits so far...")

    print(f"  Found {len(subreddits)} popular subreddits")

    # Step 2: Fetch historical data for each subreddit
    pending = [s for s in subreddits if s not in fetched_subreddits]
    print(f"  Fetching stats for {len(pending)} subreddits ({len(fetched_subreddits)} already done)...")

    for i, subreddit in enumerate(pending):
        print(f"    [{i+1}/{len(pending)}] {subreddit}...", end=" ", flush=True)

        try:
            stats = fetch_subreddit_stats(subreddit)

            if not stats:
                print("not found")
                continue

            time_series = stats.get("subscriberCountTimeSeries", [])

            if not time_series:
                print("no time series")
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

                fetched_subreddits.add(subreddit)
                save_state("subreddit_subscribers", {"fetched": list(fetched_subreddits)})
            else:
                print("empty")

        except Exception as e:
            print(f"error: {e}")
            continue

    print(f"  Done! Fetched data for {len(fetched_subreddits)} subreddits")
