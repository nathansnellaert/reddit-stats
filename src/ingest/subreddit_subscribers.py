"""Fetch historical daily subscriber counts for top subreddits.

Data source: subredditstats.com API - Historical subscriber time series (10+ years)

Note: Reddit blocks cloud IPs, so we read from a pre-fetched subreddits.json file
that was generated locally from Reddit's API.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

from subsets_utils import get, save_raw_parquet, load_state, save_state


# Rate limits for subredditstats.com
STATS_CALLS_PER_MINUTE = 30


def load_subreddit_list() -> list[str]:
    """Load subreddit list from pre-fetched JSON file.

    The subreddits.json file is generated locally (Reddit blocks cloud IPs)
    and committed to the repo. To refresh, run locally:

        python -c "
        import httpx, json, time
        subs, after = [], None
        while len(subs) < 2000:
            r = httpx.get('https://www.reddit.com/subreddits/popular.json',
                params={'limit': 100, 'after': after} if after else {'limit': 100},
                headers={'User-Agent': 'SubredditStats/1.0'}, timeout=30)
            data = r.json()['data']
            subs.extend(c['data']['display_name'] for c in data['children'])
            after = data.get('after')
            if not after: break
            time.sleep(0.5)
        json.dump(subs, open('subreddits.json', 'w'), indent=2)
        "
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


def run():
    """Fetch subscriber time series for top subreddits."""
    state = load_state("subreddit_subscribers")
    fetched_subreddits = set(state.get("fetched", []))

    # Load from pre-fetched file (Reddit blocks cloud IPs)
    subreddits = load_subreddit_list()
    print(f"  Loaded {len(subreddits)} subreddits from subreddits.json")

    # Fetch historical data for each subreddit
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
