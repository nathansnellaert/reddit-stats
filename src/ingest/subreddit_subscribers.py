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


# Rate limits for subredditstats.com - conservative to avoid blocks
STATS_CALLS_PER_MINUTE = 15

# Max consecutive errors before stopping (API might be blocking us)
MAX_CONSECUTIVE_ERRORS = 50

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


class PermanentError(Exception):
    """Error that shouldn't be retried (404, 403, etc.)"""
    pass


def should_retry(exception):
    """Only retry on transient errors."""
    if isinstance(exception, PermanentError):
        return False
    return True


@sleep_and_retry
@limits(calls=STATS_CALLS_PER_MINUTE, period=60)
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    retry=lambda retry_state: should_retry(retry_state.outcome.exception()) if retry_state.outcome else True,
    reraise=True
)
def fetch_subreddit_stats(subreddit: str) -> dict | None:
    """Fetch historical subscriber data from subredditstats.com."""
    url = f"https://subredditstats.com/api/subreddit"
    params = {"name": subreddit}

    response = get(url, params=params, timeout=60.0)

    # Permanent failures - don't retry
    if response.status_code == 404:
        return None
    if response.status_code == 403:
        raise PermanentError(f"Forbidden: {subreddit}")

    # Rate limit - retry with backoff
    if response.status_code == 429:
        raise Exception(f"Rate limited (429) for {subreddit}")

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
    failed_subreddits = set(state.get("failed", []))
    # Track subreddits that failed due to API blocking - retry after cooldown
    blocked_subreddits = set(state.get("blocked", []))
    blocked_at = state.get("blocked_at")  # ISO timestamp when blocking started

    # Clear blocked list if enough time has passed (24 hours cooldown)
    if blocked_at and blocked_subreddits:
        blocked_time = datetime.fromisoformat(blocked_at)
        hours_since_block = (datetime.now(timezone.utc) - blocked_time).total_seconds() / 3600
        if hours_since_block >= 24:
            print(f"  Clearing {len(blocked_subreddits)} blocked subreddits after {hours_since_block:.1f}h cooldown")
            blocked_subreddits.clear()
            blocked_at = None
            save_state("subreddit_subscribers", {
                "fetched": list(fetched_subreddits),
                "failed": list(failed_subreddits),
                "blocked": [],
                "blocked_at": None
            })

    # Load from pre-fetched file
    subreddits = load_subreddit_list()
    print(f"  Loaded {len(subreddits)} subreddits from subreddits.json")

    # Fetch historical data for each subreddit (skip already fetched, failed, AND blocked)
    pending = [s for s in subreddits if s not in fetched_subreddits and s not in failed_subreddits and s not in blocked_subreddits]
    print(f"  Fetching stats for {len(pending)} subreddits ({len(fetched_subreddits)} done, {len(failed_subreddits)} failed, {len(blocked_subreddits)} blocked)...")

    if not pending and blocked_subreddits:
        print(f"  All pending items are blocked. Waiting for cooldown.")
        return False  # Don't retrigger - wait for scheduled run after cooldown

    consecutive_errors = 0
    processed_this_run = 0
    current_block_batch = []  # Track subreddits that fail during current blocking period

    for i, subreddit in enumerate(pending):
        # Stop if too many consecutive errors (API might be blocking us)
        # Check this BEFORE time budget - if we're blocked, don't burn time on retries
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            print(f"  Hit {consecutive_errors} consecutive errors - API is blocking.")
            # Add all subreddits that failed during this block to blocked list
            blocked_subreddits.update(current_block_batch)
            save_state("subreddit_subscribers", {
                "fetched": list(fetched_subreddits),
                "failed": list(failed_subreddits),
                "blocked": list(blocked_subreddits),
                "blocked_at": datetime.now(timezone.utc).isoformat()
            })
            print(f"  Added {len(current_block_batch)} subreddits to blocked list for 24h cooldown")
            return False  # Don't retrigger immediately - wait for cooldown

        # Check time budget before processing
        elapsed = time.time() - start_time
        if elapsed >= GH_ACTIONS_MAX_RUN_SECONDS:
            # If we had consecutive errors, save them to blocked list before exiting
            if current_block_batch:
                blocked_subreddits.update(current_block_batch)
                save_state("subreddit_subscribers", {
                    "fetched": list(fetched_subreddits),
                    "failed": list(failed_subreddits),
                    "blocked": list(blocked_subreddits),
                    "blocked_at": datetime.now(timezone.utc).isoformat()
                })
                print(f"  Time budget exhausted after {elapsed/3600:.1f}h, {len(pending) - i} remaining")
                print(f"  Added {len(current_block_batch)} subreddits to blocked list (were failing)")
                return False  # Don't retrigger - we're being blocked
            print(f"  Time budget exhausted after {elapsed/3600:.1f} hours, {len(pending) - i} subreddits remaining")
            return True  # More work to do

        print(f"    [{i+1}/{len(pending)}] {subreddit}...", end=" ", flush=True)

        try:
            stats = fetch_subreddit_stats(subreddit)
            consecutive_errors = 0  # Reset on success
            current_block_batch.clear()  # Clear block batch on success

            if not stats:
                print("not found")
                fetched_subreddits.add(subreddit)
                save_state("subreddit_subscribers", {
                    "fetched": list(fetched_subreddits),
                    "failed": list(failed_subreddits),
                    "blocked": list(blocked_subreddits),
                    "blocked_at": blocked_at
                })
                continue

            time_series = stats.get("subscriberCountTimeSeries", [])

            if not time_series:
                print("no time series")
                fetched_subreddits.add(subreddit)
                save_state("subreddit_subscribers", {
                    "fetched": list(fetched_subreddits),
                    "failed": list(failed_subreddits),
                    "blocked": list(blocked_subreddits),
                    "blocked_at": blocked_at
                })
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
            processed_this_run += 1
            save_state("subreddit_subscribers", {
                "fetched": list(fetched_subreddits),
                "failed": list(failed_subreddits),
                "blocked": list(blocked_subreddits),
                "blocked_at": blocked_at
            })

        except PermanentError as e:
            # Permanent failure - mark as failed and move on
            print(f"permanent error: {e}")
            failed_subreddits.add(subreddit)
            consecutive_errors = 0  # Don't count permanent errors
            current_block_batch.clear()
            save_state("subreddit_subscribers", {
                "fetched": list(fetched_subreddits),
                "failed": list(failed_subreddits),
                "blocked": list(blocked_subreddits),
                "blocked_at": blocked_at
            })

        except Exception as e:
            # Transient error - track for potential blocking
            print(f"error: {e}")
            consecutive_errors += 1
            current_block_batch.append(subreddit)

    print(f"  Done! Fetched {processed_this_run} this run, {len(fetched_subreddits)} total, {len(failed_subreddits)} failed")
    return False  # All done
