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
# Data Loading and Transformation
# =============================================================================

def load_and_transform(input_csv: str) -> pd.DataFrame:
    """
    Load CSV and transform to expected format.

    Similar to update_picks.py but with additional filters for min games/wins.
    """
    print(f"Loading {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"   Loaded {len(df):,} rows")

    print("Transforming data...")

    # Extract game_date from game_start_time
    df['game_date'] = df['game_start_time'].str[:10]

    # Create unique game identifier
    df['game'] = df['match_title'] + ' (' + df['game_date'] + ')'

    # Normalize is_correct_pick using shared function
    normalized = df['is_correct_pick'].apply(normalize_is_correct)
    df['is_correct_pick'] = normalized
    df['result'] = normalized.map({True: 'won', False: 'lost'}).fillna('pending')

    print(f"   Transformed {len(df):,} rows")

    # Filter out late picks (price >= 0.95)
    print("Filtering late picks...")

    def get_pick_price(row):
        team_a, _ = parse_game_teams(row['game'])
        if row['user_pick'] == team_a:
            return row.get('yes_avg_price', 0) or 0
        else:
            return row.get('no_avg_price', 0) or 0

    df['pick_price'] = df.apply(get_pick_price, axis=1)

    original_picks = len(df)
    df = df[df['pick_price'] < 0.95]
    excluded_picks = original_picks - len(df)
    print(f"   Excluded {excluded_picks:,} late picks (price >= 0.95)")
    print(f"   Remaining: {len(df):,} picks")

    # Filter users by win rate (>= 70% with minimum 5 games)
    print("Filtering by win rate...")
    user_stats = df.groupby('user_address').agg(
        total_games=('result', lambda x: x.isin(['won', 'lost']).sum()),
        wins=('result', lambda x: (x == 'won').sum())
    ).reset_index()

    user_stats['win_pct'] = (user_stats['wins'] / user_stats['total_games'] * 100).fillna(0)

    qualified_users = user_stats[
        (user_stats['total_games'] < 5) |
        (user_stats['win_pct'] >= 70)
    ]['user_address']

    original_users = df['user_address'].nunique()
    df = df[df['user_address'].isin(qualified_users)]
    excluded_users = original_users - df['user_address'].nunique()
    print(f"   Excluded {excluded_users:,} users (< 70% win rate with 5+ games)")
    print(f"   Remaining: {df['user_address'].nunique():,} users")

    # Filter users by minimum games
    print("Filtering by minimum games...")
    user_game_counts = df.groupby('user_address').size()
    users_with_min_games = user_game_counts[user_game_counts >= 3].index
    original_users = df['user_address'].nunique()
    df = df[df['user_address'].isin(users_with_min_games)]
    excluded_users = original_users - df['user_address'].nunique()
    print(f"   Excluded {excluded_users:,} users (< 3 games)")
    print(f"   Remaining: {df['user_address'].nunique():,} users")

    # Filter users by minimum wins
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
# Stats Calculation Functions
# =============================================================================

def calculate_user_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate per-user statistics."""
    print("Computing user stats...")

    # Basic stats
    stats_df = df.groupby("user_address").agg(
        games=("result", "count"),
        wins=("result", lambda x: (x == "won").sum()),
        losses=("result", lambda x: (x == "lost").sum()),
    ).reset_index()

    # Win percentage
    stats_df["total_decided"] = stats_df["wins"] + stats_df["losses"]
    stats_df["win_pct"] = (100 * stats_df["wins"] / stats_df["total_decided"]).round(1)
    stats_df["win_pct"] = stats_df["win_pct"].fillna(0)
    stats_df = stats_df.drop(columns=["total_decided"])

    # Win streak calculation
    sort_col = "game_start_time" if "game_start_time" in df.columns else "game_date"
    df_resolved = df[df['result'].isin(['won', 'lost'])].copy()
    df_sorted = df_resolved.sort_values(
        [sort_col, 'is_correct_pick'],
        ascending=[False, True]
    )

    def calc_win_streak(group):
        results = group["result"].tolist()
        streak = 0
        for r in results:
            if r == "won":
                streak += 1
            elif r == "lost":
                break
        return streak

    streaks = df_sorted.groupby("user_address").apply(calc_win_streak, include_groups=False)
    streaks_df = streaks.reset_index()
    streaks_df.columns = ["user_address", "streak"]

    stats_df = stats_df.merge(streaks_df, on="user_address", how="left")
    stats_df["streak"] = stats_df["streak"].fillna(0).astype(int)

    # Last 10 calculation
    resolved_df = df[df['result'].isin(['won', 'lost'])].copy()

    if not resolved_df.empty:
        resolved_df['game_time_parsed'] = pd.to_datetime(resolved_df['game_start_time'], utc=True)
        resolved_df = resolved_df.sort_values(['user_address', 'game_time_parsed'], ascending=[True, False])
        last_10_df = resolved_df.groupby('user_address').head(10)

        last_10_stats = last_10_df.groupby('user_address').agg(
            last_10=('result', lambda x: (x == 'won').sum())
        ).reset_index()

        stats_df = stats_df.merge(last_10_stats, on='user_address', how='left')
        stats_df['last_10'] = stats_df['last_10'].fillna(0).astype(int)
    else:
        stats_df['last_10'] = 0

    # Sort by win_pct for consistent ordering
    stats_df = stats_df.sort_values(
        ["win_pct", "wins", "streak"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    print(f"   Computed stats for {len(stats_df):,} users")
    return stats_df


def calculate_consensus(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate consensus percentages per game."""
    print("Computing consensus...")

    consensus_data = []

    for game_name in df['game'].unique():
        game_df = df[df['game'] == game_name]

        # Parse team names from game (format: "Team A vs Team B (YYYY-MM-DD)")
        if game_name.endswith(')') and len(game_name) > 13:
            match_title = game_name[:-13]
        else:
            match_title = game_name

        team_a, team_b = parse_game_teams(match_title)
        if not team_a:
            team_a = "Team A"
            team_b = "Team B"

        # Count picks
        total_picks = len(game_df)
        team_a_picks = (game_df['user_pick'] == team_a).sum()
        team_b_picks = (game_df['user_pick'] == team_b).sum()

        # Calculate percentages
        consensus_a = round(100 * team_a_picks / total_picks, 1) if total_picks > 0 else 0
        consensus_b = round(100 * team_b_picks / total_picks, 1) if total_picks > 0 else 0

        # Determine majority pick
        majority_pick = team_a if team_a_picks >= team_b_picks else team_b

        # Determine winner
        resolved = game_df[game_df['result'].isin(['won', 'lost'])]
        if len(resolved) > 0:
            won_row = resolved[resolved['result'] == 'won'].iloc[0] if len(resolved[resolved['result'] == 'won']) > 0 else None
            if won_row is not None:
                winner = won_row['user_pick']
            else:
                # All resolved picks lost, so winner is the other team
                lost_pick = resolved.iloc[0]['user_pick']
                winner = team_b if lost_pick == team_a else team_a
        else:
            winner = "Pending"

        consensus_data.append({
            'game': game_name,
            'team_a': team_a,
            'team_b': team_b,
            'consensus_a': consensus_a,
            'consensus_b': consensus_b,
            'majority_pick': majority_pick,
            'winner': winner,
        })

    consensus_df = pd.DataFrame(consensus_data)
    print(f"   Computed consensus for {len(consensus_df):,} games")
    return consensus_df


# =============================================================================
# Build Flat Table
# =============================================================================

def build_flat_table(df: pd.DataFrame, user_stats: pd.DataFrame, consensus: pd.DataFrame) -> pd.DataFrame:
    """Build the flat table with one row per pick."""
    print("Building flat table...")

    # Start with picks dataframe
    flat_df = df[['user_address', 'game', 'game_date', 'user_pick', 'result', 'pick_price']].copy()
    flat_df = flat_df.rename(columns={'user_pick': 'pick'})

    # Merge user stats
    flat_df = flat_df.merge(
        user_stats[['user_address', 'games', 'wins', 'losses', 'win_pct', 'streak', 'last_10']],
        on='user_address',
        how='left'
    )

    # Merge consensus
    flat_df = flat_df.merge(
        consensus[['game', 'team_a', 'team_b', 'consensus_a', 'consensus_b', 'majority_pick', 'winner']],
        on='game',
        how='left'
    )

    # Calculate pick_pct (% who picked same team as this user)
    flat_df['pick_pct'] = flat_df.apply(
        lambda row: row['consensus_a'] / 100 if row['pick'] == row['team_a'] else row['consensus_b'] / 100,
        axis=1
    )

    # Clean up game name (remove date suffix for cleaner display)
    flat_df['game_display'] = flat_df['game'].apply(
        lambda x: x[:-13] if x.endswith(')') and len(x) > 13 else x
    )

    # Convert game_date to datetime for Excel
    flat_df['game_date'] = pd.to_datetime(flat_df['game_date'])

    # Select and order columns (13 columns)
    flat_df = flat_df[[
        'user_address', 'games', 'wins', 'losses', 'win_pct', 'streak', 'last_10',
        'game_display', 'game_date', 'pick', 'result', 'pick_price', 'pick_pct'
    ]]

    flat_df = flat_df.rename(columns={'game_display': 'game'})

    # Sort by win_pct desc, then game_date
    flat_df = flat_df.sort_values(['win_pct', 'game_date'], ascending=[False, True]).reset_index(drop=True)

    print(f"   Built flat table with {len(flat_df):,} rows")
    return flat_df


# =============================================================================
# Excel Generation
# =============================================================================

def generate_excel_flat(flat_df: pd.DataFrame, output_file: str, title: str):
    """Generate Excel file with flat table format."""
    if flat_df.empty:
        print("No data to generate Excel from")
        return

    start_time = time.time()
    print(f"Generating Excel: {output_file}")

    wb = Workbook()
    ws = wb.active
    ws.title = title[:31]

    # Column headers (13 columns)
    columns = [
        ('user_address', 'User'),
        ('games', 'Games'),
        ('wins', 'Wins'),
        ('losses', 'Losses'),
        ('win_pct', 'Win %'),
        ('streak', 'Streak'),
        ('last_10', 'Last 10'),
        ('game', 'Game'),
        ('game_date', 'Game Date'),
        ('pick', 'Pick'),
        ('result', 'Result'),
        ('pick_price', 'Price'),
        ('pick_pct', 'Pick %'),
    ]

    center_align = Alignment(horizontal="center")

    # Write header row
    for col_idx, (col_key, col_name) in enumerate(columns, 1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = center_align

    # Write data rows
    total_rows = len(flat_df)
    for row_idx, row in flat_df.iterrows():
        if (row_idx + 1) % 5000 == 0:
            pct = ((row_idx + 1) / total_rows) * 100
            print(f"   Writing row {row_idx + 1:,}/{total_rows:,} ({pct:.1f}%)", end="\r")

        excel_row = row_idx + 2  # +1 for header, +1 for 1-indexed

        for col_idx, (col_key, col_name) in enumerate(columns, 1):
            value = row.get(col_key, "")

            # Handle game_date as Excel date
            if col_key == 'game_date' and pd.notna(value):
                cell = ws.cell(row=excel_row, column=col_idx, value=value)
                cell.number_format = 'YYYY-MM-DD'
                cell.alignment = center_align
                continue

            # Handle pick_pct as percentage (stored as decimal, formatted as %)
            if col_key == 'pick_pct' and pd.notna(value):
                cell = ws.cell(row=excel_row, column=col_idx, value=value)
                cell.number_format = '0.0%'
                cell.alignment = center_align
                continue

            # Format pick_price
            if col_key == 'pick_price' and pd.notna(value):
                value = round(value, 2)

            cell = ws.cell(row=excel_row, column=col_idx, value=value)
            cell.alignment = center_align

            # Add hyperlink for user_address column
            if col_key == "user_address" and value:
                cell.hyperlink = f"https://polymarket.com/profile/{value}"
                cell.font = Font(color="0563C1", underline="single")

            # Color code the pick column based on result
            if col_key == "pick":
                result = row.get('result', '')
                if result == "won":
                    cell.fill = GREEN_FILL
                elif result == "lost":
                    cell.fill = RED_FILL
                elif result == "pending":
                    cell.fill = YELLOW_FILL

    print(f"   Writing row {total_rows:,}/{total_rows:,} (100.0%)")

    # Freeze panes at H2 (after user stats, after header)
    ws.freeze_panes = "H2"

    # Auto-filter
    ws.auto_filter.ref = f"A1:{get_column_letter(len(columns))}{total_rows + 1}"

    # Adjust column widths (13 columns)
    column_widths = {
        'A': 20,  # User
        'B': 7,   # Games
        'C': 6,   # Wins
        'D': 7,   # Losses
        'E': 7,   # Win %
        'F': 7,   # Streak
        'G': 8,   # Last 10
        'H': 25,  # Game
        'I': 12,  # Game Date
        'J': 15,  # Pick
        'K': 8,   # Result
        'L': 7,   # Price
        'M': 8,   # Pick %
    }

    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width

    # Save
    print("Saving file...")
    save_time = time.time()
    wb.save(output_file)
    print(f"   File saved in {format_time(time.time() - save_time)}")

    # Preview
    print("\n" + "=" * 80)
    print("PREVIEW (First 10 rows)")
    print("=" * 80)
    preview_cols = ['user_address', 'win_pct', 'game', 'pick', 'result', 'pick_pct']
    preview_df = flat_df[preview_cols].head(10).copy()
    preview_df['user_address'] = preview_df['user_address'].apply(lambda x: x[:12] + "..." if len(str(x)) > 15 else x)
    preview_df['game'] = preview_df['game'].apply(lambda x: x[:20] + "..." if len(str(x)) > 23 else x)
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

    # Calculate stats and consensus
    user_stats = calculate_user_stats(df)
    consensus = calculate_consensus(df)

    # Build flat table
    flat_df = build_flat_table(df, user_stats, consensus)

    # Generate output filename (using "analysis" prefix)
    output_file = get_output_filename(weeks, is_season, sport, prefix="analysis")

    # Generate Excel
    generate_excel_flat(flat_df, output_file, title)


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    # Step 1: Select sport
    sport = select_sport(title="Analysis Generator (Flat Table)")
    if not sport or sport == "exit":
        return

    # Step 2: Select time period
    weeks, is_season = select_time_period(sport)
    if weeks is None and not is_season:
        return

    # Step 3: Generate analysis
    do_generate(sport=sport, weeks=weeks, is_season=is_season)


if __name__ == "__main__":
    main()
