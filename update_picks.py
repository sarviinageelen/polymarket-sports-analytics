import os
import time
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
from typing import List, Optional

# Import from shared modules
from utils.shared_utils import (
    SPORTS_CONFIG,
    normalize_is_correct,
    parse_game_teams,
    filter_by_weeks,
    get_output_filename,
)
from utils.excel_utils import (
    GREEN_FILL,
    RED_FILL,
    YELLOW_FILL,
    HEADER_FILL,
    HEADER_FONT,
    format_time,
)
from utils.menu_utils import select_sport, select_time_period


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

    # Validate required columns exist
    required_cols = ['is_correct_pick', 'game_start_time', 'match_title',
                     'user_pick', 'yes_avg_price', 'no_avg_price', 'user_address']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    print("Transforming data...")

    # Extract game_date from game_start_time (just take first 10 chars: YYYY-MM-DD)
    df['game_date'] = df['game_start_time'].str[:10]

    # Create unique game identifier (match_title + date) to handle rematches
    df['game'] = df['match_title'] + ' (' + df['game_date'] + ')'

    # Map is_correct_pick to result using shared normalize function
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
        team_a, _ = parse_game_teams(row['game'])
        if row['user_pick'] == team_a:
            price = row.get('yes_avg_price')
            return price if price is not None and not pd.isna(price) else 0
        else:
            price = row.get('no_avg_price')
            return price if price is not None and not pd.isna(price) else 0

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

    # ==========================================================================
    # Filter users by minimum games (>= 3 games)
    # ==========================================================================
    print("Filtering by minimum games...")
    user_game_counts = df.groupby('user_address').size()
    users_with_min_games = user_game_counts[user_game_counts >= 3].index
    original_users = df['user_address'].nunique()
    df = df[df['user_address'].isin(users_with_min_games)]
    excluded_users = original_users - df['user_address'].nunique()
    print(f"   Excluded {excluded_users:,} users (< 3 games)")
    print(f"   Remaining: {df['user_address'].nunique():,} users")

    # ==========================================================================
    # Filter users by minimum wins (>= 3 wins)
    # ==========================================================================
    print("Filtering by minimum wins...")
    user_wins = df[df['result'] == 'won'].groupby('user_address').size()
    users_with_min_wins = user_wins[user_wins >= 3].index
    original_users = df['user_address'].nunique()
    df = df[df['user_address'].isin(users_with_min_wins)]
    excluded_users = original_users - df['user_address'].nunique()
    print(f"   Excluded {excluded_users:,} users (< 3 wins)")
    print(f"   Remaining: {df['user_address'].nunique():,} users")

    return df


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

    print(f"   Stats computed in {format_time(time.time() - start_time)}")

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

    print(f"   Streaks computed in {format_time(time.time() - streak_time)}")

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

    print(f"   Last 10 computed in {format_time(time.time() - last10_time)}")

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

    print(f"   Pivot completed in {format_time(time.time() - pivot_time)}")

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

        # Parse team names from match title
        team_a, team_b = parse_game_teams(match_title)
        if not team_a:
            team_a = "Team A"
            team_b = "Team B"

        game_consensus[game_name] = {
            'team_a': team_a,
            'team_b': team_b,
            'match_title': match_title,
            'game_date': game_date,
        }

    print(f"   Consensus computed in {format_time(time.time() - consensus_time)}")

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

    print(f"   Merge completed in {format_time(time.time() - merge_time)}")
    print(f"Created {len(result_df):,} user rows (total: {format_time(time.time() - start_time)})")

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
    print(f"   Excel rows written in {format_time(time.time() - excel_time)}")

    # Add freeze pane after last_10 column and header rows (column I, row 7)
    ws.freeze_panes = "I7"

    # Set column widths - game columns (I onwards) get minimum width of 10
    for col_idx in range(num_stats_cols + 1, len(display_cols) + 1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = 10

    # Save
    print("Saving file...")
    save_time = time.time()
    wb.save(output_file)
    print(f"   File saved in {format_time(time.time() - save_time)}")

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
    print(f"Total time: {format_time(total_time)}")


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
    output_file = get_output_filename(weeks, is_season, sport, prefix="leaderboard")

    # Generate Excel
    generate_excel(df, output_file, title)


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    # Step 1: Select sport
    sport = select_sport(title="Leaderboard Generator")
    if not sport or sport == "exit":
        return

    # Step 2: Select time period
    weeks, is_season = select_time_period(sport)
    if weeks is None and not is_season:
        return

    # Step 3: Generate leaderboard
    do_generate(sport=sport, weeks=weeks, is_season=is_season)


if __name__ == "__main__":
    main()
