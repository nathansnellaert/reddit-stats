"""Fetch historical daily subscriber counts for top subreddits.

Data source: subredditstats.com API - Historical subscriber time series (10+ years)

Note: Reddit blocks cloud IPs, so we use a curated list of popular subreddits
instead of fetching from Reddit's API.
"""

from datetime import datetime, timezone

import pyarrow as pa
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

from subsets_utils import get, save_raw_parquet, load_state, save_state


# Rate limits for subredditstats.com
STATS_CALLS_PER_MINUTE = 30

# Curated list of popular subreddits (Reddit blocks cloud IPs)
# This list covers major categories: news, entertainment, tech, gaming, finance, etc.
POPULAR_SUBREDDITS = [
    # Top general
    "AskReddit", "funny", "pics", "gaming", "worldnews", "news", "movies", "todayilearned",
    "science", "aww", "music", "videos", "memes", "Showerthoughts", "EarthPorn",
    "explainlikeimfive", "IAmA", "books", "LifeProTips", "space", "DIY", "Jokes",
    "food", "sports", "television", "gadgets", "nottheonion", "OldSchoolCool",
    "Documentaries", "photoshopbattles", "GetMotivated", "UpliftingNews", "history",
    # Tech
    "technology", "programming", "Python", "javascript", "webdev", "learnprogramming",
    "dataisbeautiful", "MachineLearning", "artificial", "linux", "apple", "Android",
    "Bitcoin", "CryptoCurrency", "ethereum", "cscareerquestions",
    # Gaming
    "leagueoflegends", "pcgaming", "PS5", "xbox", "NintendoSwitch", "Minecraft",
    "Overwatch", "FortNiteBR", "gaming", "Games", "Steam", "buildapc", "pcmasterrace",
    "truegaming", "patientgamers", "GameDeals", "IndieGaming",
    # Finance
    "wallstreetbets", "stocks", "investing", "personalfinance", "financialindependence",
    "CreditCards", "churning", "povertyfinance", "Bogleheads", "options",
    # Entertainment
    "Marvel", "DCcomics", "StarWars", "harrypotter", "gameofthrones", "anime",
    "manga", "netflix", "television", "movies", "horror", "scifi", "Fantasy",
    # Sports
    "nba", "nfl", "soccer", "baseball", "hockey", "MMA", "CFB", "CollegeBasketball",
    "formula1", "tennis", "golf", "running", "Fitness", "bodybuilding",
    # Lifestyle
    "Art", "ArtFundamentals", "drawing", "photography", "crafts", "sewing",
    "woodworking", "gardening", "plants", "cooking", "recipes", "MealPrepSunday",
    "EatCheapAndHealthy", "vegan", "keto", "loseit", "progresspics",
    # Discussion
    "AskScience", "askphilosophy", "changemyview", "unpopularopinion", "TrueOffMyChest",
    "relationship_advice", "AmItheAsshole", "tifu", "confession", "NoStupidQuestions",
    # News & Politics
    "politics", "worldpolitics", "geopolitics", "economics", "environment",
    # Humor
    "mildlyinteresting", "mildlyinfuriating", "Unexpected", "WatchPeopleDieInside",
    "PublicFreakout", "facepalm", "CrappyDesign", "assholedesign",
    # Animals
    "cats", "dogs", "AnimalsBeingDerps", "AnimalsBeingBros", "NatureIsFuckingLit",
    "natureismetal", "Eyebleach", "rarepuppers", "aww",
    # Meta / Community
    "AskMen", "AskWomen", "TwoXChromosomes", "MensLib", "teenagers", "college",
    "Parenting", "raisedbynarcissists", "ADHD", "depression", "anxiety",
    # Hobbies
    "Lego", "boardgames", "DnD", "rpg", "MagicArena", "chess", "poker",
    "audiophile", "headphones", "vinyl", "Guitar", "WeAreTheMusicMakers",
    # Education
    "education", "Teachers", "college", "GradSchool", "ApplyingToCollege",
    "languagelearning", "learn_arabic", "Spanish", "French", "German",
    # Career
    "jobs", "resumes", "careerguidance", "ITCareerQuestions", "EngineeringStudents",
    # Home
    "HomeImprovement", "InteriorDesign", "malelivingspace", "CozyPlaces",
    "RoomPorn", "Frugal", "BuyItForLife", "minimalism",
    # Local / Regional
    "AskAnAmerican", "AskEurope", "AskUK", "canada", "australia", "india",
    "europe", "unitedkingdom", "de", "france", "japan",
    # Science
    "askscience", "chemistry", "Physics", "biology", "neuroscience", "Astronomy",
    "geology", "engineering", "math", "statistics",
    # Misc popular
    "TIHI", "cursedcomments", "BrandNewSentence", "rareinsults", "oddlysatisfying",
    "Satisfyingasfuck", "interestingasfuck", "Damnthatsinteresting", "BeAmazed",
    "nextfuckinglevel", "HumansBeingBros", "MadeMeSmile",
]


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

    # Use curated list (Reddit blocks cloud IPs)
    subreddits = POPULAR_SUBREDDITS
    print(f"  Using curated list of {len(subreddits)} popular subreddits")

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
