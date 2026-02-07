"""
Shared utilities for Polymarket Sports Analytics.

This module contains configuration, schedule functions, and data utilities
shared across update_picks.py and update_analyze.py.
"""

import os
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Tuple


# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = "."

# Filtering thresholds (defaults)
LATE_PICK_THRESHOLD = 0.95
MIN_WIN_PCT = 70.0
MIN_GAMES_FOR_WIN_PCT = 5
MIN_GAMES = 3
MIN_WINS = 3

# Sport-specific configurations for 2025-2026 seasons
SPORTS_CONFIG = {
    "NFL": {
        "input_csv": "db_trades_nfl.csv",
        "season_start": datetime(2025, 9, 4),   # 2025 season, Week 1 starts Thu Sep 4
        "total_weeks": 18,
        "season_year": 2025,
    },
    "NBA": {
        "input_csv": "db_trades_nba.csv",
        "season_start": datetime(2025, 10, 22), # 2025-26 season starts Wed Oct 22
        "total_weeks": 26,                       # ~26 weeks regular season
        "season_year": 2026,
    },
    "CFB": {
        "input_csv": "db_trades_cfb.csv",
        "season_start": datetime(2025, 8, 23),  # 2025 season, Week 0 late August (Sat)
        "total_weeks": 16,                       # Including bowl season
        "season_year": 2025,
    },
    "CBB": {
        "input_csv": "db_trades_cbb.csv",
        "season_start": datetime(2025, 11, 4),  # 2025-26 season starts Tue Nov 4
        "total_weeks": 22,                       # Through March Madness
        "season_year": 2026,
    },
}

# Cache of latest week found in CSVs to avoid repeated reads.
_MAX_WEEK_CACHE = {}


# =============================================================================
# Schedule Functions
# =============================================================================

def get_csv_max_week(sport: str) -> int:
    """
    Get max week number implied by latest game_start_time in the CSV.

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        Max week number, or 0 if file is missing or has no valid dates
    """
    if sport in _MAX_WEEK_CACHE:
        return _MAX_WEEK_CACHE[sport]

    config = SPORTS_CONFIG[sport]
    input_csv = config["input_csv"]
    season_start = config["season_start"]
    max_week = 0

    if os.path.exists(input_csv):
        try:
            dates_df = pd.read_csv(input_csv, usecols=["game_start_time"])
            game_dates = pd.to_datetime(
                dates_df["game_start_time"].astype(str).str[:10],
                errors="coerce",
            )
            max_date = game_dates.max()
            if pd.notna(max_date) and max_date >= season_start:
                max_week = ((max_date - season_start).days // 7) + 1
        except Exception:
            max_week = 0

    _MAX_WEEK_CACHE[sport] = max_week
    return max_week


def get_week_dates(week: int, sport: str) -> Tuple[str, str]:
    """
    Get start and end dates for a specific week.

    Args:
        week: Week number
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        Tuple of (start_date, end_date) in 'YYYY-MM-DD' format
    """
    config = SPORTS_CONFIG[sport]
    total_weeks = config["total_weeks"]
    season_start = config["season_start"]
    max_weeks = max(total_weeks, get_csv_max_week(sport))

    if week < 1 or week > max_weeks:
        raise ValueError(f"Week must be between 1 and {max_weeks}, got {week}")

    # Each week is 7 days
    week_offset = (week - 1) * 7

    start_date = season_start + timedelta(days=week_offset)
    end_date = start_date + timedelta(days=7)

    return (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))


def get_current_week(sport: str) -> int:
    """
    Auto-detect the current week based on today's date.

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        Current week number, or 0 if before season
    """
    config = SPORTS_CONFIG[sport]
    season_start = config["season_start"]
    total_weeks = config["total_weeks"]
    week_from_csv = get_csv_max_week(sport)

    today = datetime.now()

    # Before season starts
    if today < season_start:
        return 0

    # Calculate days since season start
    days_elapsed = (today - season_start).days
    week_from_today = (days_elapsed // 7) + 1

    # Use latest CSV week when available to avoid jumping ahead of data.
    if week_from_csv > 0:
        return min(week_from_today, week_from_csv)

    # Fall back to configured season length if CSV data is unavailable.
    return min(week_from_today, total_weeks)


def get_last_n_weeks(n: int, sport: str) -> List[int]:
    """
    Get the last N weeks including the current week.

    Args:
        n: Number of weeks to return
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        List of week numbers from oldest to newest
    """
    current = get_current_week(sport)

    if current == 0:
        return []

    start_week = max(1, current - n + 1)
    return list(range(start_week, current + 1))


# =============================================================================
# Data Normalization Functions
# =============================================================================

def normalize_is_correct(value) -> Optional[bool]:
    """
    Normalize is_correct_pick values to boolean.

    Handles various input formats: booleans, strings ("TRUE"/"FALSE"),
    integers (0/1), and floats.

    Args:
        value: Raw value from CSV

    Returns:
        True, False, or None for missing/invalid values
    """
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in ("TRUE", "1"):
            return True
        if normalized in ("FALSE", "0"):
            return False
    return None


def coerce_numeric_series(series: pd.Series) -> pd.Series:
    """
    Convert a Series to numeric values, removing commas when present.

    Args:
        series: pandas Series with numeric strings (may include commas)

    Returns:
        Series of floats with non-parsable values set to NaN
    """
    if series is None:
        return pd.Series([], dtype="float64")
    return pd.to_numeric(series.astype(str).str.replace(",", ""), errors="coerce")


def parse_game_teams(game_str) -> Tuple[str, str]:
    """
    Parse 'Team A vs Team B' or 'Team A vs. Team B' into (team_a, team_b).

    Args:
        game_str: Game string like "Kansas City Chiefs vs Buffalo Bills"

    Returns:
        Tuple of (team_a, team_b), or ("", "") if parsing fails
    """
    # Type validation - ensure we have a string
    if not isinstance(game_str, str) or not game_str:
        return ("", "")
    if " vs" not in game_str.lower():
        return ("", "")
    # Case-insensitive replacement and split
    normalized = re.sub(r' vs\.? ', ' vs ', game_str, flags=re.IGNORECASE)
    parts = normalized.split(" vs ")
    if len(parts) == 2:
        return (parts[0].strip(), parts[1].strip())
    return ("", "")


# =============================================================================
# Data Filtering Functions
# =============================================================================

def filter_by_weeks(df: pd.DataFrame, weeks: List[int], sport: str) -> pd.DataFrame:
    """
    Filter DataFrame to only include games from specified weeks.

    Uses game_date to determine which week a game belongs to.

    Args:
        df: DataFrame with 'game_date' column
        weeks: List of week numbers to include
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        Filtered DataFrame
    """
    if not weeks or df.empty:
        return df

    # Build date ranges for all weeks
    date_ranges = []
    for week in weeks:
        try:
            start, end = get_week_dates(week, sport)
            date_ranges.append((start, end))
        except ValueError as e:
            print(f"Warning: Skipping invalid week {week}: {e}")
            continue

    # Filter by date
    mask = pd.Series([False] * len(df))

    for start, end in date_ranges:
        week_mask = (df["game_date"] >= start) & (df["game_date"] < end)
        mask = mask | week_mask

    filtered = df[mask]

    return filtered


# =============================================================================
# Output Helpers
# =============================================================================

def get_output_filename(
    weeks: Optional[List[int]],
    is_season: bool,
    sport: str,
    prefix: str = "leaderboard"
) -> str:
    """
    Generate output filename based on filter and sport.

    Args:
        weeks: List of week numbers, or None for full season
        is_season: Whether generating full season report
        sport: Sport key (NFL, NBA, CFB, CBB)
        prefix: Filename prefix ("leaderboard" or "analysis")

    Returns:
        Output filepath like "leaderboard_nfl_week_5.xlsx"
    """
    sport_lower = sport.lower()
    if sport_lower == "all":
        season_year = datetime.now().year
    else:
        config = SPORTS_CONFIG[sport]
        season_year = config["season_year"]

    if is_season or weeks is None:
        return os.path.join(OUTPUT_DIR, f"{prefix}_{sport_lower}_season_{season_year}.xlsx")

    if len(weeks) == 1:
        return os.path.join(OUTPUT_DIR, f"{prefix}_{sport_lower}_week_{weeks[0]}.xlsx")

    return os.path.join(OUTPUT_DIR, f"{prefix}_{sport_lower}_weeks_{min(weeks)}-{max(weeks)}.xlsx")
