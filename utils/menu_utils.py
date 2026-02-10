"""
Interactive menu utilities for Polymarket Sports Analytics.

This module contains reusable menu functions for sport selection,
season selection, time period selection, and other interactive prompts.
"""

import os
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from utils.shared_utils import (
    SPORTS_CONFIG,
    get_current_week,
    get_last_n_weeks,
    get_season,
    get_season_week_limit,
    get_seasons,
    get_week_dates,
)


# =============================================================================
# Sport Selection
# =============================================================================

def select_sport(
    title: str = "Menu",
    include_all: bool = False,
    check_csv: bool = True
) -> Optional[str]:
    """
    Display sport selection menu and return selected sport.

    Args:
        title: Menu title to display
        include_all: Whether to include "All sports" option
        check_csv: Whether to validate CSV file exists

    Returns:
        Sport key (NFL, NBA, CFB, CBB), "all" if include_all, "exit" to exit,
        or None for invalid choice
    """
    print("=" * 50)
    print(title)
    print("=" * 50)
    print()
    print("Select sport:")

    if include_all:
        print("  1. All sports")
        print("  2. NFL")
        print("  3. NBA")
        print("  4. CFB")
        print("  5. CBB")
        print("  0. Exit")
        print()
        choice = input("Enter choice (0-5): ").strip()
        sport_map = {"1": "all", "2": "NFL", "3": "NBA", "4": "CFB", "5": "CBB"}
    else:
        print("  1. NFL")
        print("  2. NBA")
        print("  3. CFB")
        print("  4. CBB")
        print("  0. Exit")
        print()
        choice = input("Enter choice (0-4): ").strip()
        sport_map = {"1": "NFL", "2": "NBA", "3": "CFB", "4": "CBB"}

    if choice == "0":
        print("Exiting.")
        return "exit"

    sport = sport_map.get(choice)
    if not sport:
        print("Invalid choice")
        return None

    if check_csv and sport != "all":
        config = SPORTS_CONFIG[sport]
        if not os.path.exists(config["input_csv"]):
            print(f"\nWarning: {config['input_csv']} not found.")
            print("Please run update_trades.py first to generate the data file.")
            return None

    return sport


# =============================================================================
# Season Selection
# =============================================================================

def select_season(sport: str) -> Optional[str]:
    """
    Display season selection menu and return season_id.

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        season_id string, "exit", or None for invalid selection.
    """
    seasons = get_seasons(sport)
    if not seasons:
        print(f"No seasons configured for {sport}.")
        return None

    print()
    print(f"Select season ({sport}):")
    for idx, season in enumerate(seasons, start=1):
        default_tag = " [default]" if season.get("default") else ""
        start_str = season["start_date"].strftime("%Y-%m-%d")
        end_str = season["end_date"].strftime("%Y-%m-%d")
        regular_weeks = season["regular_weeks"]
        print(
            f"  {idx}. {season['label']} "
            f"(Start: {start_str}, Regular: {regular_weeks}w, End: {end_str}){default_tag}"
        )
    print("  0. Exit")
    print()

    choice = input(f"Enter choice (0-{len(seasons)}): ").strip()
    if choice == "0":
        print("Exiting.")
        return "exit"

    if not choice.isdigit():
        print("Invalid choice")
        return None

    index = int(choice) - 1
    if index < 0 or index >= len(seasons):
        print("Invalid choice")
        return None

    return seasons[index]["season_id"]


# =============================================================================
# Time Period Selection
# =============================================================================

def _format_week_span(week: int, sport: str, season_id: str) -> Tuple[str, str]:
    """Return week span as inclusive date strings."""
    start_str, end_exclusive_str = get_week_dates(week, sport, season_id)
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_exclusive_str, "%Y-%m-%d") - timedelta(days=1)
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


def select_time_period(sport: str, season_id: str) -> Tuple[Optional[List[int]], bool]:
    """
    Display time period selection menu and return (weeks, is_season).

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)
        season_id: Season identifier

    Returns:
        Tuple of (weeks list, is_season flag).
        Returns (None, False) for exit or invalid choice.
    """
    season = get_season(sport, season_id)
    current = get_current_week(sport, season_id, include_postseason=True)
    total_weeks = get_season_week_limit(sport, season_id, include_postseason=True)
    season_start = season["start_date"].strftime("%Y-%m-%d")
    season_end = season["end_date"].strftime("%Y-%m-%d")

    print()
    print(f"{sport} {season['label']} Season (Postseason Included)")
    print(f"   Current Week: {current} of {total_weeks}")
    print(f"   Season Window: {season_start} to {season_end}")
    print()

    options = {}
    option_idx = 1

    print("Select time period:")
    if current >= 1:
        start, end = _format_week_span(current, sport, season_id)
        print(f"  {option_idx}. Latest week (Week {current}: {start} to {end})")
        options[str(option_idx)] = ([current], False)
        option_idx += 1

        if current > 1:
            prev_week = current - 1
            start, end = _format_week_span(prev_week, sport, season_id)
            print(f"  {option_idx}. Previous week (Week {prev_week}: {start} to {end})")
            options[str(option_idx)] = ([prev_week], False)
            option_idx += 1

        last5 = get_last_n_weeks(5, sport, season_id, include_postseason=True)
        if last5:
            start, _ = _format_week_span(min(last5), sport, season_id)
            _, end = _format_week_span(max(last5), sport, season_id)
            print(
                f"  {option_idx}. Last 5 weeks "
                f"(Weeks {min(last5)}-{max(last5)}: {start} to {end})"
            )
            options[str(option_idx)] = (last5, False)
            option_idx += 1
    else:
        print("   (Season has not started yet based on current date.)")

    print(f"  {option_idx}. Whole season (Weeks 1-{total_weeks})")
    options[str(option_idx)] = (None, True)
    print("  0. Exit")
    print()

    choice = input(f"Enter choice (0-{option_idx}): ").strip()
    if choice == "0":
        print("Exiting.")
        return None, False

    if choice not in options:
        print("Invalid choice")
        return None, False

    weeks, is_season = options[choice]
    if is_season:
        print(f"\nGenerating {sport} {season['label']} Full Season report...")
    else:
        week_range = f"{min(weeks)}-{max(weeks)}" if len(weeks) > 1 else str(weeks[0])
        print(f"\nGenerating {sport} {season['label']} Week(s) {week_range} report...")

    return weeks, is_season


# =============================================================================
# Resolution Status Selection (for update_trades.py)
# =============================================================================

def select_resolution_status() -> Optional[str]:
    """
    Display resolution status menu and return filter type.

    Returns:
        "all", "resolved", "unresolved", "exit", or None for invalid choice
    """
    print()
    print("Select market status:")
    print("  1. All markets")
    print("  2. Resolved only")
    print("  3. Unresolved only")
    print("  0. Exit")
    print()

    choice = input("Enter choice (0-3): ").strip()

    if choice == "0":
        return "exit"

    status_map = {"1": "all", "2": "resolved", "3": "unresolved"}
    return status_map.get(choice)
