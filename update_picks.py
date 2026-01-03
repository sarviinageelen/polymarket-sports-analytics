import os
import time
import pandas as pd
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from typing import List, Optional, Tuple

# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = "."

# Sport-specific configurations for 2025-2026 seasons
SPORTS_CONFIG = {
    "NFL": {
        "input_csv": "db_trades_nfl.csv",
        "season_start": datetime(2025, 9, 3),   # 2025 season, Week 1 starts Thu Sep 4
        "total_weeks": 18,
        "season_year": 2025,
    },
    "NBA": {
        "input_csv": "db_trades_nba.csv",
        "season_start": datetime(2025, 10, 21), # 2025-26 season starts late Oct
        "total_weeks": 26,                       # ~26 weeks regular season
        "season_year": 2026,
    },
    "CFB": {
        "input_csv": "db_trades_cfb.csv",
        "season_start": datetime(2025, 8, 23),  # 2025 season, Week 0 late August
        "total_weeks": 16,                       # Including bowl season
        "season_year": 2025,
    },
    "CBB": {
        "input_csv": "db_trades_cbb.csv",
        "season_start": datetime(2025, 11, 3),  # 2025-26 season starts early Nov
        "total_weeks": 22,                       # Through March Madness
        "season_year": 2026,
    },
}

# Cache of latest week found in CSVs to avoid repeated reads.
_MAX_WEEK_CACHE = {}

# Colors
GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
YELLOW_FILL = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)


# =============================================================================
# Schedule Functions
# =============================================================================

def get_csv_max_week(sport: str) -> int:
    """
    Get max week number implied by latest game_start_time in the CSV.
    Returns 0 if the file is missing or has no valid dates.
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
# Data Transformation
# =============================================================================

def load_and_transform(input_csv: str) -> pd.DataFrame:
    """
    Load db_trades_nfl.csv and transform to expected format.

    Transformations:
        - match_title -> game
        - game_start_time -> game_date (extract date)
        - is_correct_pick -> result (TRUE->won, FALSE->lost, empty->pending)
    """
    print(f"Loading {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"   Loaded {len(df):,} rows")

    print("Transforming data...")

    # Extract game_date from game_start_time (just take first 10 chars: YYYY-MM-DD)
    df['game_date'] = df['game_start_time'].str[:10]

    # Create unique game identifier (match_title + date) to handle rematches
    df['game'] = df['match_title'] + ' (' + df['game_date'] + ')'

    # Map is_correct_pick to result
    # Values can be booleans or "TRUE"/"FALSE" strings from CSV.
    def normalize_is_correct(value):
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

    normalized_is_correct = df['is_correct_pick'].apply(normalize_is_correct)
    df['is_correct_pick'] = normalized_is_correct
    df['result'] = normalized_is_correct.map({True: 'won', False: 'lost'}).fillna('pending')

    print(f"   Transformed {len(df):,} rows")

    # ==========================================================================
    # Filter out late picks (price >= 0.95)
    # These are bets placed when games are nearly decided
    # ==========================================================================
    print("Filtering late picks...")

    # Calculate user's entry price for their pick
    # If user_pick matches first team (Team A), use yes_avg_price, else no_avg_price
    def get_pick_price(row):
        parts = row['game'].replace(" vs. ", " vs ").split(" vs ")
        team_a = parts[0].strip() if len(parts) == 2 else ""
        if row['user_pick'] == team_a:
            return row.get('yes_avg_price', 0) or 0
        else:
            return row.get('no_avg_price', 0) or 0

    df['pick_price'] = df.apply(get_pick_price, axis=1)

    # Filter out individual late picks (price >= 0.95)
    original_picks = len(df)
    df = df[df['pick_price'] < 0.95]
    excluded_picks = original_picks - len(df)
    print(f"   Excluded {excluded_picks:,} late picks (price >= 0.95)")
    print(f"   Remaining: {len(df):,} picks")

    # ==========================================================================
    # Filter users by win rate (>= 70% with minimum 5 games)
    # ==========================================================================
    print("Filtering by win rate...")
    user_stats = df.groupby('user_address').agg(
        total_games=('result', lambda x: x.isin(['won', 'lost']).sum()),
        wins=('result', lambda x: (x == 'won').sum())
    ).reset_index()

    user_stats['win_pct'] = (user_stats['wins'] / user_stats['total_games'] * 100).fillna(0)

    # Only filter users with >= 5 games; keep users with < 5 games (not enough data)
    qualified_users = user_stats[
        (user_stats['total_games'] < 5) |  # Keep users with < 5 games
        (user_stats['win_pct'] >= 70)       # Keep users with >= 70% win rate
    ]['user_address']

    original_users = df['user_address'].nunique()
    df = df[df['user_address'].isin(qualified_users)]
    excluded_users = original_users - df['user_address'].nunique()
    print(f"   Excluded {excluded_users:,} users (< 70% win rate with 5+ games)")
    print(f"   Remaining: {df['user_address'].nunique():,} users")

    return df


# =============================================================================
# Helper Functions
# =============================================================================

def calculate_streak(results: list, streak_type: str) -> int:
    """
    Calculate current winning or losing streak.
    Results should be ordered from most recent to oldest.
    """
    if not results:
        return 0

    streak = 0
    target = "won" if streak_type == "win" else "lost"

    for result in results:
        if result == target:
            streak += 1
        elif result in ("won", "lost"):  # Stop at opposite result, skip pending
            break

    return streak


def filter_by_weeks(df: pd.DataFrame, weeks: List[int], sport: str) -> pd.DataFrame:
    """
    Filter DataFrame to only include games from specified weeks.

    Uses game_date to determine which week a game belongs to.
    """
    if not weeks or df.empty:
        return df

    # Build date ranges for all weeks
    date_ranges = []
    for week in weeks:
        start, end = get_week_dates(week, sport)
        date_ranges.append((start, end))

    # Filter by date
    mask = pd.Series([False] * len(df))

    for start, end in date_ranges:
        week_mask = (df["game_date"] >= start) & (df["game_date"] < end)
        mask = mask | week_mask

    filtered = df[mask]

    return filtered


def get_output_filename(weeks: Optional[List[int]], is_season: bool, sport: str) -> str:
    """Generate output filename based on filter and sport."""
    config = SPORTS_CONFIG[sport]
    season_year = config["season_year"]
    sport_lower = sport.lower()

    if is_season or weeks is None:
        return os.path.join(OUTPUT_DIR, f"leaderboard_{sport_lower}_season_{season_year}.xlsx")

    if len(weeks) == 1:
        return os.path.join(OUTPUT_DIR, f"leaderboard_{sport_lower}_week_{weeks[0]}.xlsx")

    return os.path.join(OUTPUT_DIR, f"leaderboard_{sport_lower}_weeks_{min(weeks)}-{max(weeks)}.xlsx")


# =============================================================================
# Excel Generation
# =============================================================================

def generate_excel(df: pd.DataFrame, output_file: str, title: str):
    """Generate Excel leaderboard from filtered DataFrame using vectorized operations."""

    if df.empty:
        print("No data to generate Excel from")
        return

    start_time = time.time()

    # Get unique games ordered by game_start_time
    sort_col = "game_start_time" if "game_start_time" in df.columns else "game_date"
    games_df = df[["game", sort_col]].drop_duplicates().sort_values(sort_col)
    games = games_df["game"].tolist()
    print(f"   Found {len(games)} games")

    # Get unique users
    total_users = df["user_address"].nunique()
    print(f"   Found {total_users:,} users")

    # ==========================================================================
    # OPTIMIZED: Vectorized stats calculation
    # ==========================================================================
    print("Computing stats (vectorized)...")

    # Group by user and compute stats in one pass
    stats_df = df.groupby("user_address").agg(
        wins=("result", lambda x: (x == "won").sum()),
        losses=("result", lambda x: (x == "lost").sum()),
        pending=("result", lambda x: (x == "pending").sum()),
    ).reset_index()

    # Calculate games (total picks made)
    stats_df["games"] = stats_df["wins"] + stats_df["losses"] + stats_df["pending"]

    # Calculate win percentage
    stats_df["total_decided"] = stats_df["wins"] + stats_df["losses"]
    stats_df["win_pct"] = (100 * stats_df["wins"] / stats_df["total_decided"]).round(1)
    stats_df["win_pct"] = stats_df["win_pct"].fillna(0)
    stats_df = stats_df.drop(columns=["total_decided"])

    print(f"   Stats computed in {time.time() - start_time:.1f}s")

    # ==========================================================================
    # OPTIMIZED: Vectorized streak calculation
    # ==========================================================================
    print("Computing streaks...")
    streak_time = time.time()

    # Filter to resolved games only for streak calculation (exclude pending)
    df_resolved = df[df['result'].isin(['won', 'lost'])].copy()
    # Sort by time descending, then by is_correct_pick ascending (losses first)
    # This ensures losses break streaks correctly when multiple games have same timestamp
    df_sorted = df_resolved.sort_values(
        [sort_col, 'is_correct_pick'],
        ascending=[False, True]  # Most recent first, losses (False) before wins (True)
    )

    # Calculate streaks using groupby
    def calc_streaks(group):
        results = group["result"].tolist()
        win_streak = 0
        loss_streak = 0

        # Win streak
        for r in results:
            if r == "won":
                win_streak += 1
            elif r == "lost":
                break

        # Loss streak
        for r in results:
            if r == "lost":
                loss_streak += 1
            elif r == "won":
                break

        return pd.Series({"win_streak": win_streak, "loss_streak": loss_streak})

    streaks_df = df_sorted.groupby("user_address").apply(calc_streaks, include_groups=False).reset_index()

    # Merge stats and streaks
    stats_df = stats_df.merge(streaks_df, on="user_address", how="left")
    stats_df["win_streak"] = stats_df["win_streak"].fillna(0).astype(int)
    stats_df["loss_streak"] = stats_df["loss_streak"].fillna(0).astype(int)

    print(f"   Streaks computed in {time.time() - streak_time:.1f}s")

    # ==========================================================================
    # Calculate Last 10 Games Win Rate (recent form across whole dataset)
    # ==========================================================================
    print("Computing last 10 form...")
    last10_time = time.time()

    # Only consider resolved games for last 10 calculation
    resolved_df = df[df['result'].isin(['won', 'lost'])].copy()

    if not resolved_df.empty:
        # Parse game_start_time with UTC to handle mixed timezones
        resolved_df['game_time_parsed'] = pd.to_datetime(resolved_df['game_start_time'], utc=True)

        # Sort by user and time descending, take last 10 per user
        resolved_df = resolved_df.sort_values(['user_address', 'game_time_parsed'], ascending=[True, False])
        last_10_df = resolved_df.groupby('user_address').head(10)

        # Calculate last 10 stats
        last_10_stats = last_10_df.groupby('user_address').agg(
            last_10_wins=('result', lambda x: (x == 'won').sum()),
            last_10_games=('result', 'count')
        ).reset_index()

        # Merge with stats_df
        stats_df = stats_df.merge(last_10_stats, on='user_address', how='left')
        stats_df['last_10_wins'] = stats_df['last_10_wins'].fillna(0).astype(int)
        stats_df['last_10_games'] = stats_df['last_10_games'].fillna(0).astype(int)
    else:
        stats_df['last_10_wins'] = 0
        stats_df['last_10_games'] = 0

    print(f"   Last 10 computed in {time.time() - last10_time:.1f}s")

    # ==========================================================================
    # OPTIMIZED: Vectorized pivot for picks
    # ==========================================================================
    print("Pivoting picks...")
    pivot_time = time.time()

    # Pivot picks
    picks_pivot = df.pivot_table(
        index="user_address",
        columns="game",
        values="user_pick",
        aggfunc="first"
    )

    # Pivot results
    results_pivot = df.pivot_table(
        index="user_address",
        columns="game",
        values="result",
        aggfunc="first"
    )

    # Rename result columns
    results_pivot.columns = [f"{col}_result" for col in results_pivot.columns]

    # Combine pivots
    pivot_df = picks_pivot.join(results_pivot).reset_index()

    print(f"   Pivot completed in {time.time() - pivot_time:.1f}s")

    # ==========================================================================
    # Calculate consensus percentages per game (team names only, formulas will calculate %)
    # ==========================================================================
    print("Computing consensus...")
    consensus_time = time.time()

    # Build consensus data for each game (team names and date extracted separately)
    game_consensus = {}
    for game_name in games:
        # Game name format: "Team A vs Team B (YYYY-MM-DD)" or "Team A vs. Team B (YYYY-MM-DD)"
        # Extract date from end (last 12 chars are " (YYYY-MM-DD)")
        if game_name.endswith(')') and len(game_name) > 13:
            game_date = game_name[-11:-1]  # Extract "YYYY-MM-DD"
            match_title = game_name[:-13]   # Remove " (YYYY-MM-DD)"
        else:
            game_date = ""
            match_title = game_name

        # Parse team names from match title (format: "Team A vs Team B" or "Team A vs. Team B")
        parts = match_title.replace(" vs. ", " vs ").split(" vs ")
        if len(parts) == 2:
            team_a = parts[0].strip()
            team_b = parts[1].strip()
        else:
            team_a = "Team A"
            team_b = "Team B"

        game_consensus[game_name] = {
            'team_a': team_a,
            'team_b': team_b,
            'match_title': match_title,
            'game_date': game_date,
        }

    print(f"   Consensus computed in {time.time() - consensus_time:.1f}s")

    # ==========================================================================
    # Merge everything
    # ==========================================================================
    print("Merging data...")
    merge_time = time.time()

    result_df = stats_df.merge(pivot_df, on="user_address", how="left")

    # Sort by wins desc, win_pct desc, loss_streak asc
    result_df = result_df.sort_values(
        ["wins", "win_pct", "loss_streak"],
        ascending=[False, False, True]
    ).reset_index(drop=True)

    # Add rank column
    result_df.insert(0, "rank", range(1, len(result_df) + 1))

    # Fill NaN with empty string for display
    result_df = result_df.fillna("")

    print(f"   Merge completed in {time.time() - merge_time:.1f}s")
    print(f"Created {len(result_df):,} user rows (total: {time.time() - start_time:.1f}s)")

    # ==========================================================================
    # Write Excel (normal mode for freeze pane support)
    # ==========================================================================
    print(f"Writing Excel...")
    excel_time = time.time()

    wb = Workbook()
    ws = wb.active
    ws.title = title[:31]

    # Define columns to write (order: rank, user_address, games, wins, losses, win_pct, win_streak, last_10, then game columns)
    stats_cols = ["rank", "user_address", "games", "wins", "losses", "win_pct", "win_streak", "last_10"]
    display_cols = stats_cols + games
    num_stats_cols = len(stats_cols)

    # Create display column for last_10 (just the wins count)
    result_df["last_10"] = result_df["last_10_wins"].fillna(0).astype(int)

    # Header display names mapping
    header_names = {
        "rank": "rank",
        "user_address": "user_address",
        "games": "games",
        "wins": "wins",
        "losses": "losses",
        "win_pct": "win %",
        "win_streak": "win streak",
        "last_10": "last 10",
    }

    center_align = Alignment(horizontal="center")

    # Calculate total rows for data range
    total_rows = len(result_df)
    header_row_count = 6

    # ==========================================================================
    # Write 6 header rows with consensus data
    # Row 1: Team A consensus % (formula)
    # Row 2: Team A name (clean, no date)
    # Row 3: Team B consensus % (formula)
    # Row 4: Team B name (clean, no date)
    # Row 5: Game date
    # Row 6: Match title (main header, "Team A vs Team B" without date)
    # ==========================================================================

    # Calculate data range for formulas (data starts at row 7)
    data_start_row = header_row_count + 1
    data_end_row = data_start_row + total_rows - 1

    # Row 1: Team A consensus percentages (formulas)
    for col_idx, col_name in enumerate(display_cols, 1):
        if col_idx <= num_stats_cols:
            # Stats columns - empty for rows 1-4
            cell = ws.cell(row=1, column=col_idx, value="")
        else:
            # Game columns - Team A % formula
            consensus = game_consensus.get(col_name, {})
            team_a = consensus.get('team_a', '')
            col_letter = get_column_letter(col_idx)
            # Formula: COUNTIF(range, team_name) / COUNTA(range)
            formula = f'=COUNTIF({col_letter}{data_start_row}:{col_letter}{data_end_row},"{team_a}")/COUNTA({col_letter}{data_start_row}:{col_letter}{data_end_row})'
            cell = ws.cell(row=1, column=col_idx, value=formula)
            cell.number_format = '0%'
        cell.alignment = center_align
        cell.font = Font(bold=True)

    # Row 2: Team A names
    for col_idx, col_name in enumerate(display_cols, 1):
        if col_idx <= num_stats_cols:
            cell = ws.cell(row=2, column=col_idx, value="")
        else:
            consensus = game_consensus.get(col_name, {})
            cell = ws.cell(row=2, column=col_idx, value=consensus.get('team_a', ''))
        cell.alignment = center_align
        cell.font = Font(bold=True)

    # Row 3: Team B consensus percentages (formulas)
    for col_idx, col_name in enumerate(display_cols, 1):
        if col_idx <= num_stats_cols:
            cell = ws.cell(row=3, column=col_idx, value="")
        else:
            consensus = game_consensus.get(col_name, {})
            team_b = consensus.get('team_b', '')
            col_letter = get_column_letter(col_idx)
            # Formula: COUNTIF(range, team_name) / COUNTA(range)
            formula = f'=COUNTIF({col_letter}{data_start_row}:{col_letter}{data_end_row},"{team_b}")/COUNTA({col_letter}{data_start_row}:{col_letter}{data_end_row})'
            cell = ws.cell(row=3, column=col_idx, value=formula)
            cell.number_format = '0%'
        cell.alignment = center_align
        cell.font = Font(bold=True)

    # Row 4: Team B names
    for col_idx, col_name in enumerate(display_cols, 1):
        if col_idx <= num_stats_cols:
            cell = ws.cell(row=4, column=col_idx, value="")
        else:
            consensus = game_consensus.get(col_name, {})
            cell = ws.cell(row=4, column=col_idx, value=consensus.get('team_b', ''))
        cell.alignment = center_align
        cell.font = Font(bold=True)

    # Row 5: Game dates
    for col_idx, col_name in enumerate(display_cols, 1):
        if col_idx <= num_stats_cols:
            cell = ws.cell(row=5, column=col_idx, value="")
        else:
            consensus = game_consensus.get(col_name, {})
            cell = ws.cell(row=5, column=col_idx, value=consensus.get('game_date', ''))
        cell.alignment = center_align
        cell.font = Font(bold=True)

    # Row 6: Main header row (stats headers + match titles without date)
    for col_idx, col_name in enumerate(display_cols, 1):
        if col_idx <= num_stats_cols:
            display_name = header_names.get(col_name, col_name)
        else:
            # For game columns, use clean match_title without date
            consensus = game_consensus.get(col_name, {})
            display_name = consensus.get('match_title', col_name)
        cell = ws.cell(row=6, column=col_idx, value=display_name)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = center_align

    # Convert games to set for fast lookup
    games_set = set(games)

    # Write data rows (starting at row 7)
    for row_idx, row_data in enumerate(result_df.itertuples(index=False)):
        if (row_idx + 1) % 10000 == 0:
            pct = ((row_idx + 1) / total_rows) * 100
            print(f"   Writing row {row_idx + 1:,}/{total_rows:,} ({pct:.1f}%)", end="\r")

        row_dict = result_df.iloc[row_idx]
        excel_row = row_idx + header_row_count + 1  # +1 for 1-indexed, +header_row_count for headers

        for col_idx, col_name in enumerate(display_cols, 1):
            value = row_dict.get(col_name, "")
            cell = ws.cell(row=excel_row, column=col_idx, value=value)
            cell.alignment = center_align

            # Add hyperlink for user_address column
            if col_name == "user_address" and value:
                cell.hyperlink = f"https://polymarket.com/profile/{value}"
                cell.font = Font(color="0563C1", underline="single")

            # Apply color formatting for game columns
            if col_name in games_set:
                result_col = f"{col_name}_result"
                result = row_dict.get(result_col, "")

                if result == "won":
                    cell.fill = GREEN_FILL
                elif result == "lost":
                    cell.fill = RED_FILL
                elif result == "pending":
                    cell.fill = YELLOW_FILL

    print(f"   Writing row {total_rows:,}/{total_rows:,} (100.0%)")
    print(f"   Excel rows written in {time.time() - excel_time:.1f}s")

    # Add freeze pane after last_10 column and header rows (column I, row 7)
    ws.freeze_panes = "I7"

    # Save
    print("Saving file...")
    save_time = time.time()
    wb.save(output_file)
    print(f"   File saved in {time.time() - save_time:.1f}s")

    # Print preview
    print("\n" + "="*80)
    print("PREVIEW (Top 10)")
    print("="*80)
    preview_cols = ["rank", "user_address", "games", "wins", "losses", "win_pct", "last_10"]
    preview_df = result_df[preview_cols].head(10).copy()
    preview_df["user_address"] = preview_df["user_address"].apply(lambda x: x[:18] + "..." if len(str(x)) > 20 else x)
    preview_df.columns = ["rank", "user_address", "games", "wins", "losses", "win %", "last 10"]
    print(preview_df.to_string(index=False))

    total_time = time.time() - start_time
    print(f"\nSaved: {output_file}")
    print(f"Total time: {total_time:.1f}s")


# =============================================================================
# Main Operations
# =============================================================================

def do_generate(sport: str, weeks: Optional[List[int]] = None, is_season: bool = False):
    """Main generation function."""
    config = SPORTS_CONFIG[sport]
    input_csv = config["input_csv"]
    season_year = config["season_year"]

    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Input file not found: {input_csv}")
        return

    # Load and transform data
    df = load_and_transform(input_csv)

    # Filter by weeks if specified
    if weeks and not is_season:
        week_range = f"{min(weeks)}-{max(weeks)}" if len(weeks) > 1 else str(weeks[0])
        print(f"Filtering to Week(s) {week_range}...")
        df = filter_by_weeks(df, weeks, sport)
        print(f"   Filtered to {len(df):,} rows")
        title = f"{sport} Weeks {week_range}"
    else:
        title = f"{sport} Season {season_year}"

    if df.empty:
        print("No data matches the filter criteria")
        return

    # Generate output filename
    output_file = get_output_filename(weeks, is_season, sport)

    # Generate Excel
    generate_excel(df, output_file, title)


def parse_week_range(range_str: str) -> List[int]:
    """Parse week range string like '10-14' into list [10, 11, 12, 13, 14]."""
    if "-" in range_str:
        parts = range_str.split("-")
        start = int(parts[0])
        end = int(parts[1])
        return list(range(start, end + 1))
    else:
        return [int(range_str)]


# =============================================================================
# Interactive Menu
# =============================================================================

def select_sport() -> Optional[str]:
    """Display sport selection menu and return selected sport."""
    print("=" * 50)
    print("Leaderboard Generator")
    print("=" * 50)
    print()
    print("Select sport:")
    print("  1. NFL")
    print("  2. NBA")
    print("  3. CFB")
    print("  4. CBB")
    print("  0. Exit")
    print()

    choice = input("Enter choice (0-4): ").strip()

    if choice == "0":
        print("Exiting.")
        return None

    sport_map = {"1": "NFL", "2": "NBA", "3": "CFB", "4": "CBB"}
    sport = sport_map.get(choice)

    if not sport:
        print("Invalid choice")
        return None

    # Check if input file exists
    config = SPORTS_CONFIG[sport]
    if not os.path.exists(config["input_csv"]):
        print(f"\nWarning: {config['input_csv']} not found.")
        print("Please run update_trades.py first to generate the data file.")
        return None

    return sport


def select_time_period(sport: str) -> Tuple[Optional[List[int]], bool]:
    """Display time period selection menu and return (weeks, is_season)."""
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


def main():
    # Step 1: Select sport
    sport = select_sport()
    if not sport:
        return

    # Step 2: Select time period
    weeks, is_season = select_time_period(sport)
    if weeks is None and not is_season:
        return

    # Step 3: Generate leaderboard
    do_generate(sport=sport, weeks=weeks, is_season=is_season)


if __name__ == "__main__":
    main()
