"""
Interactive menu utilities for Polymarket Sports Analytics.

This module contains reusable menu functions for sport selection,
time period selection, and other interactive prompts.
"""

import os
from typing import List, Optional, Tuple

from utils.shared_utils import SPORTS_CONFIG, get_current_week, get_last_n_weeks


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

    # Check if input file exists (for picks/analysis generators)
    if check_csv and sport != "all":
        config = SPORTS_CONFIG[sport]
        if not os.path.exists(config["input_csv"]):
            print(f"\nWarning: {config['input_csv']} not found.")
            print("Please run update_trades.py first to generate the data file.")
            return None

    return sport


# =============================================================================
# Time Period Selection
# =============================================================================

def select_time_period(sport: str) -> Tuple[Optional[List[int]], bool]:
    """
    Display time period selection menu and return (weeks, is_season).

    Args:
        sport: Sport key (NFL, NBA, CFB, CBB)

    Returns:
        Tuple of (weeks list, is_season flag).
        Returns (None, False) for exit or invalid choice.
    """
    config = SPORTS_CONFIG[sport]
    season_year = config["season_year"]

    current = get_current_week(sport)
    print()
    print(f"{sport} {season_year} Season")
    print(f"   Current Week: {current}")
    print()

    # Build menu options
    last5 = get_last_n_weeks(5, sport)

    print("Select time period:")
    print(f"  1. Latest week (Week {current})")
    if current > 1:
        print(f"  2. Previous week (Week {current - 1})")
    if last5:
        print(f"  3. Last 5 weeks (Weeks {min(last5)}-{max(last5)})")
    print(f"  4. Whole season")
    print("  0. Exit")
    print()

    choice = input("Enter choice (0-4): ").strip()

    if choice == "0":
        print("Exiting.")
        return None, False

    if choice == "1":
        weeks = [current]
        print(f"\nGenerating {sport} Week {current} leaderboard...")
    elif choice == "2" and current > 1:
        weeks = [current - 1]
        print(f"\nGenerating {sport} Week {current - 1} leaderboard...")
    elif choice == "3" and last5:
        weeks = last5
        print(f"\nGenerating {sport} Weeks {min(last5)}-{max(last5)} leaderboard...")
    elif choice == "4":
        weeks = None
        print(f"\nGenerating {sport} Full Season leaderboard...")
    else:
        print("Invalid choice")
        return None, False

    return weeks, (weeks is None)


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
