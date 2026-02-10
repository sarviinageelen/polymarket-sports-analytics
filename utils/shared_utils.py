"""
Shared utilities for Polymarket Sports Analytics.

This module contains configuration, schedule functions, and data utilities
shared across update_picks.py and update_analyze.py.
"""

import os
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


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

# Sport-specific configurations with multi-season support.
# `regular_weeks` is regular season length; `end_date` includes postseason cap.
SPORTS_CONFIG = {
    "NFL": {
        "input_csv": "db_trades_nfl.csv",
        "seasons": [
            {
                "season_id": "2025",
                "label": "2025",
                "start_date": datetime(2025, 9, 4),
                "regular_weeks": 18,
                "end_date": datetime(2026, 2, 8),
                "default": True,
            },
            {
                "season_id": "2024",
                "label": "2024",
                "start_date": datetime(2024, 9, 5),
                "regular_weeks": 18,
                "end_date": datetime(2025, 2, 9),
                "default": False,
            },
        ],
    },
    "NBA": {
        "input_csv": "db_trades_nba.csv",
        "seasons": [
            {
                "season_id": "2025-26",
                "label": "2025-26",
                "start_date": datetime(2025, 10, 22),
                "regular_weeks": 26,
                "end_date": datetime(2026, 6, 22),
                "default": True,
            },
            {
                "season_id": "2024-25",
                "label": "2024-25",
                "start_date": datetime(2024, 10, 22),
                "regular_weeks": 26,
                "end_date": datetime(2025, 6, 23),
                "default": False,
            },
        ],
    },
    "CFB": {
        "input_csv": "db_trades_cfb.csv",
        "seasons": [
            {
                "season_id": "2025",
                "label": "2025",
                "start_date": datetime(2025, 8, 23),
                "regular_weeks": 16,
                "end_date": datetime(2026, 1, 20),
                "default": True,
            },
            {
                "season_id": "2024",
                "label": "2024",
                "start_date": datetime(2024, 8, 24),
                "regular_weeks": 16,
                "end_date": datetime(2025, 1, 20),
                "default": False,
            },
        ],
    },
    "CBB": {
        "input_csv": "db_trades_cbb.csv",
        "seasons": [
            {
                "season_id": "2025-26",
                "label": "2025-26",
                "start_date": datetime(2025, 11, 4),
                "regular_weeks": 22,
                "end_date": datetime(2026, 4, 7),
                "default": True,
            },
            {
                "season_id": "2024-25",
                "label": "2024-25",
                "start_date": datetime(2024, 11, 4),
                "regular_weeks": 22,
                "end_date": datetime(2025, 4, 8),
                "default": False,
            },
        ],
    },
}

# Cache of latest week found in CSVs to avoid repeated reads.
_MAX_WEEK_CACHE = {}


# =============================================================================
# Season Helpers
# =============================================================================

def get_seasons(sport: str) -> List[Dict]:
    """
    Get configured seasons for a sport, newest first.

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        List of season dicts sorted by start_date descending.
    """
    if sport not in SPORTS_CONFIG:
        raise ValueError(f"Unknown sport: {sport}")
    seasons = SPORTS_CONFIG[sport].get("seasons", [])
    return sorted(seasons, key=lambda s: s["start_date"], reverse=True)


def get_season(sport: str, season_id: Optional[str] = None) -> Dict:
    """
    Resolve a season config for a sport.

    If season_id is omitted, returns the default season if present,
    otherwise the newest configured season.
    """
    seasons = get_seasons(sport)
    if not seasons:
        raise ValueError(f"No seasons configured for sport: {sport}")

    if season_id:
        for season in seasons:
            if season.get("season_id") == season_id:
                return season
        valid = ", ".join(s.get("season_id", "") for s in seasons)
        raise ValueError(f"Invalid season_id '{season_id}' for {sport}. Valid: {valid}")

    for season in seasons:
        if season.get("default"):
            return season

    return seasons[0]


def _extract_game_dates(df: pd.DataFrame) -> pd.Series:
    """Return game dates as pandas datetime from game_date or game_start_time."""
    if "game_date" in df.columns:
        date_series = df["game_date"].astype(str).str[:10]
    elif "game_start_time" in df.columns:
        date_series = df["game_start_time"].astype(str).str[:10]
    else:
        raise ValueError("DataFrame must contain 'game_date' or 'game_start_time'")
    return pd.to_datetime(date_series, errors="coerce")


def _max_weeks_allowed(sport: str, season_id: str, include_postseason: bool = True) -> int:
    """Get maximum allowed week count for a season."""
    season = get_season(sport, season_id)
    season_start = season["start_date"]
    season_end = season["end_date"]
    regular_weeks = int(season.get("regular_weeks", 0))

    span_days = (season_end.date() - season_start.date()).days
    postseason_weeks = (span_days // 7) + 1 if span_days >= 0 else regular_weeks

    if include_postseason:
        return max(1, max(regular_weeks, postseason_weeks))
    return max(1, regular_weeks)


# =============================================================================
# Schedule Functions
# =============================================================================

def get_csv_max_week(sport: str, season_id: Optional[str] = None) -> int:
    """
    Get max week number implied by latest game_start_time in the CSV
    for the selected season window.

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier (e.g. "2025", "2025-26")

    Returns:
        Max week number, or 0 if file is missing or has no valid dates.
    """
    season = get_season(sport, season_id)
    resolved_season_id = season["season_id"]
    cache_key = (sport, resolved_season_id)
    if cache_key in _MAX_WEEK_CACHE:
        return _MAX_WEEK_CACHE[cache_key]

    input_csv = SPORTS_CONFIG[sport]["input_csv"]
    season_start = season["start_date"]
    season_end = season["end_date"]
    max_week = 0

    if os.path.exists(input_csv):
        try:
            dates_df = pd.read_csv(input_csv, usecols=["game_start_time"])
            game_dates = pd.to_datetime(
                dates_df["game_start_time"].astype(str).str[:10],
                errors="coerce",
            )
            game_dates = game_dates[(game_dates >= season_start) & (game_dates <= season_end)]
            max_date = game_dates.max()
            if pd.notna(max_date):
                max_week = ((max_date - season_start).days // 7) + 1
        except Exception:
            max_week = 0

    max_week = min(max_week, _max_weeks_allowed(sport, resolved_season_id, include_postseason=True))
    _MAX_WEEK_CACHE[cache_key] = max_week
    return max_week


def get_week_dates(week: int, sport: str, season_id: str) -> Tuple[str, str]:
    """
    Get start and end dates for a specific week in a selected season.

    Args:
        week: Week number
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier (e.g. "2025", "2025-26")

    Returns:
        Tuple of (start_date, end_date_exclusive) in 'YYYY-MM-DD' format.
    """
    season = get_season(sport, season_id)
    max_weeks = _max_weeks_allowed(sport, season_id, include_postseason=True)

    if week < 1 or week > max_weeks:
        raise ValueError(f"Week must be between 1 and {max_weeks}, got {week}")

    week_offset = (week - 1) * 7
    start_date = season["start_date"] + timedelta(days=week_offset)
    end_date_exclusive = start_date + timedelta(days=7)
    season_end_exclusive = season["end_date"] + timedelta(days=1)
    if end_date_exclusive > season_end_exclusive:
        end_date_exclusive = season_end_exclusive

    return (start_date.strftime("%Y-%m-%d"), end_date_exclusive.strftime("%Y-%m-%d"))


def get_current_week(sport: str, season_id: str, include_postseason: bool = True) -> int:
    """
    Auto-detect the current week based on today's date and available CSV data.

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier
        include_postseason: Whether to allow weeks beyond regular season

    Returns:
        Current week number, or 0 if before season start.
    """
    season = get_season(sport, season_id)
    season_start = season["start_date"]
    season_end = season["end_date"]
    max_weeks_allowed = _max_weeks_allowed(sport, season_id, include_postseason=include_postseason)
    week_from_csv = get_csv_max_week(sport, season_id)

    today = datetime.now()
    if today < season_start:
        return 0

    effective_today = min(today, season_end)
    days_elapsed = (effective_today.date() - season_start.date()).days
    week_from_today = (days_elapsed // 7) + 1

    effective_week = week_from_today
    if week_from_csv > 0:
        effective_week = max(week_from_today, week_from_csv)

    return min(effective_week, max_weeks_allowed)


def get_last_n_weeks(
    n: int,
    sport: str,
    season_id: str,
    include_postseason: bool = True
) -> List[int]:
    """
    Get the last N weeks including the current week.

    Args:
        n: Number of weeks to return
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier
        include_postseason: Whether to allow weeks beyond regular season

    Returns:
        List of week numbers from oldest to newest.
    """
    current = get_current_week(sport, season_id, include_postseason=include_postseason)
    if current == 0:
        return []

    start_week = max(1, current - n + 1)
    return list(range(start_week, current + 1))


def get_season_week_limit(sport: str, season_id: str, include_postseason: bool = True) -> int:
    """Return total selectable weeks for a season."""
    return _max_weeks_allowed(sport, season_id, include_postseason=include_postseason)


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

def filter_by_season(df: pd.DataFrame, sport: str, season_id: str) -> pd.DataFrame:
    """
    Filter DataFrame to rows that fall within the selected season window.

    Args:
        df: DataFrame with game_date or game_start_time
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier

    Returns:
        Filtered DataFrame.
    """
    if df.empty:
        return df

    season = get_season(sport, season_id)
    season_start = season["start_date"]
    season_end = season["end_date"]
    game_dates = _extract_game_dates(df)

    mask = (game_dates >= season_start) & (game_dates <= season_end)
    return df[mask]


def filter_by_weeks(df: pd.DataFrame, weeks: List[int], sport: str, season_id: str) -> pd.DataFrame:
    """
    Filter DataFrame to only include games from specified weeks.

    Uses game_date to determine which week a game belongs to.

    Args:
        df: DataFrame with 'game_date' column
        weeks: List of week numbers to include
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier

    Returns:
        Filtered DataFrame
    """
    if not weeks or df.empty:
        return df

    game_dates = _extract_game_dates(df)

    # Build date ranges for all weeks
    date_ranges = []
    for week in weeks:
        try:
            start, end = get_week_dates(week, sport, season_id)
            date_ranges.append((start, end))
        except ValueError as e:
            print(f"Warning: Skipping invalid week {week}: {e}")
            continue

    # Filter by date
    mask = pd.Series(False, index=df.index)

    for start, end in date_ranges:
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        week_mask = (game_dates >= start_dt) & (game_dates < end_dt)
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
    prefix: str = "leaderboard",
    season_id: Optional[str] = None
) -> str:
    """
    Generate output filename based on filter and sport.

    Args:
        weeks: List of week numbers, or None for full season
        is_season: Whether generating full season report
        sport: Sport key (NFL, NBA, CFB, CBB)
        prefix: Filename prefix ("leaderboard" or "analysis")
        season_id: Optional season identifier for sport-specific output

    Returns:
        Output filepath like "leaderboard_nfl_week_5.xlsx"
    """
    def _season_year_tag(season: Dict) -> int:
        season_key = str(season.get("season_id", "")).strip()
        if "-" in season_key:
            left, right = season_key.split("-", 1)
            if right.isdigit():
                if len(right) == 2 and left.isdigit() and len(left) >= 2:
                    return int(f"{left[:2]}{right}")
                return int(right)
        if season_key.isdigit():
            return int(season_key)
        return int(season["end_date"].year)

    sport_lower = sport.lower()
    if sport_lower == "all":
        season_tag = datetime.now().year
    else:
        season = get_season(sport, season_id)
        # Preserve existing naming style with year-based suffix.
        season_tag = _season_year_tag(season)

    if is_season or weeks is None:
        return os.path.join(OUTPUT_DIR, f"{prefix}_{sport_lower}_season_{season_tag}.xlsx")

    if len(weeks) == 1:
        return os.path.join(OUTPUT_DIR, f"{prefix}_{sport_lower}_week_{weeks[0]}.xlsx")

    return os.path.join(OUTPUT_DIR, f"{prefix}_{sport_lower}_weeks_{min(weeks)}-{max(weeks)}.xlsx")
