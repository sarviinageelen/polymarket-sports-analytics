"""
Polymarket Sports Markets Fetcher
==================================

Fetches moneyline betting markets for NFL, CFB, and NBA from Polymarket's 
Gamma API and saves them to a CSV database file.

Data Source:
    - Polymarket Gamma API (https://gamma-api.polymarket.com)
    - Fetches both active and closed/resolved markets
    - Filters to moneyline markets only (excludes spreads, totals, props)

Output:
    - db_markets.parquet: Database of all moneyline markets with columns:
        - sport: Sport league (NFL, CFB, NBA)
        - condition_id: Unique market identifier for Polymarket
        - match_title: Game title (e.g., "Chiefs vs Raiders")
        - game_start_time: Scheduled game start (Excel-friendly format)
        - outcome_team_a/b: Team names extracted from market
        - is_resolved: Boolean indicating if game has concluded
        - winning_outcome: Winner team name or "Pending"
        - polymarket_slug: URL slug for market page

Usage:
    python update_markets.py

Configuration:
    - SERIES_IDS: Maps Polymarket series IDs to sport names
    - WINNER_PRICE_THRESHOLD: Price threshold to determine winner (>0.95)

API Rate Limits:
    - Uses pagination (500 events per request)
    - Fetches both closed and active events separately
"""

import requests
import pandas as pd
import json
import os
import logging
import re

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------

def setup_logging():
    """Configure logging to both console and file."""
    os.makedirs("logs", exist_ok=True)

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    # Prevent duplicate handlers on reimport
    if not log.handlers:
        file_handler = logging.FileHandler("logs/markets.log", encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        log.addHandler(file_handler)

    return log

logger = setup_logging()

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Output file path for market database
PARQUET_PATH = "db_markets.parquet"

# Polymarket Gamma API endpoints
# - /sports: List of available sports with series IDs
# - /events: List of events (games) for a given series
GAMMA_SPORTS_URL = "https://gamma-api.polymarket.com/sports"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# HTTP request timeout (seconds) and session for connection pooling
REQUEST_TIMEOUT = 30
SESSION = requests.Session()

# Mapping of Polymarket series IDs to sport abbreviations
# These IDs are specific to Polymarket's internal categorization
SERIES_IDS = {
    "10187": "NFL",   # National Football League
    "10210": "CFB",   # College Football (NCAA FBS)
    "10345": "NBA",   # National Basketball Association
    "10470": "CBB",   # College Basketball (Men's)
}

# Maximum events to fetch per API request (Gamma API pagination limit)
PAGE_LIMIT = 500

# Price threshold for determining market resolution
# When an outcome's price exceeds this, consider it the winner
# (Markets resolve to ~1.0 for winning outcome, ~0.0 for losing)
WINNER_PRICE_THRESHOLD = 0.95


def coerce_bool(value) -> bool:
    """Coerce common truthy/falsey values to bool."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)) and not pd.isna(value):
        return bool(int(value))
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n", ""}:
        return False
    return False


# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

def progress_bar(current: int, total: int, width: int = 40) -> str:
    """
    Generate an ASCII progress bar string.
    
    Args:
        current: Number of items completed
        total: Total number of items to process
        width: Character width of the progress bar (default: 40)
    
    Returns:
        Formatted string like "[====----] 4/10 (40.0%)"
    
    Example:
        >>> progress_bar(25, 100)
        '[==========------------------------------] 25/100 (25.0%)'
    """
    if total == 0:
        return f"[{'=' * width}] 0/0"

    pct = current / total
    filled = int(width * pct)
    bar = '=' * filled + '-' * (width - filled)
    return f"[{bar}] {current}/{total} ({pct*100:.1f}%)"


def print_progress(current: int, total: int, prefix: str = "", suffix: str = "") -> None:
    """
    Print a progress bar that updates in place using carriage return.
    
    Args:
        current: Number of items completed
        total: Total number of items to process
        prefix: Text to display before the progress bar
        suffix: Text to display after the progress bar
    
    Note:
        Uses '\r' to overwrite the current line until completion,
        then prints a newline to preserve the final state.
    """
    bar = progress_bar(current, total)
    end_char = '\n' if current >= total else '\r'
    line = f"{prefix}{bar} {suffix}".ljust(80)
    print(line, end=end_char, flush=True)


def parse_json_field(value):
    """
    Safely parse a field that may be a JSON string or already parsed.
    
    The Gamma API sometimes returns outcomes/prices as JSON strings
    (e.g., '["Team A", "Team B"]') instead of actual arrays.
    
    Args:
        value: The field value, either a string, list, or other type
    
    Returns:
        Parsed JSON if input was a valid JSON string, otherwise original value
    
    Example:
        >>> parse_json_field('["Chiefs", "Raiders"]')
        ['Chiefs', 'Raiders']
        >>> parse_json_field(['Chiefs', 'Raiders'])
        ['Chiefs', 'Raiders']
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


_NON_MONEYLINE_KEYWORDS = re.compile(r"\b(spread|total|over|under|prop|props)\b", re.IGNORECASE)
_NON_MONEYLINE_OUTCOMES = {"over", "under", "yes", "no", "home", "away"}


def _has_non_moneyline_keywords(text: str) -> bool:
    if not text:
        return False
    return bool(_NON_MONEYLINE_KEYWORDS.search(text))


def _outcomes_look_non_moneyline(outcomes) -> bool:
    if not isinstance(outcomes, list):
        return False
    if len(outcomes) != 2:
        return True
    normalized = [str(o).strip().lower() for o in outcomes if o is not None]
    if any(o in _NON_MONEYLINE_OUTCOMES for o in normalized):
        return True
    if any(o.startswith("over") or o.startswith("under") for o in normalized):
        return True
    return False


def _has_vs_marker(text: str) -> bool:
    if not text:
        return False
    if " @ " in text or " at " in text:
        return True
    return bool(re.search(r"\bvs\.?\b", text, re.IGNORECASE))


def is_moneyline_market(market):
    """
    Determine if a market is a moneyline (win/lose) market.
    
    Polymarket sports markets include:
    - Moneyline: Which team will win? (what we want)
    - Spread: Will Team A win by X points?
    - Totals: Will the combined score be over/under X?
    - Props: Player-specific or other prop bets
    
    Detection Strategy:
    1. Check sportsMarketType field (most reliable)
    2. Fallback: Use outcomes + question/slug/title heuristics
    
    Args:
        market: Market dict from Gamma API
    
    Returns:
        True if this is a moneyline market, False otherwise
    """
    market_type = market.get('sportsMarketType')
    market_type_norm = str(market_type).strip().lower() if market_type is not None else ""

    if market_type_norm:
        return market_type_norm == 'moneyline'

    # Fallback: inspect outcomes and text fields
    outcomes = parse_json_field(market.get('outcomes'))
    if _outcomes_look_non_moneyline(outcomes):
        return False

    question = market.get('question', '') or ''
    slug = market.get('slug', '') or ''
    title = market.get('title', '') or ''
    combined = f"{question} {slug} {title}"

    # Explicit non-moneyline keywords
    if _has_non_moneyline_keywords(combined):
        return False

    # Explicit moneyline indicator
    if 'moneyline' in combined.lower():
        return True

    # Team vs team indicator
    if _has_vs_marker(question) or _has_vs_marker(title):
        return True

    return False


def determine_winner(outcomes, outcome_prices):
    """
    Determine the winning team based on current outcome prices.
    
    When a market resolves, the winning outcome's price approaches 1.0
    and the losing outcome's price approaches 0.0. We use a threshold
    of 0.95 to identify resolved markets.
    
    Args:
        outcomes: List of outcome names, e.g., ["Chiefs", "Raiders"]
        outcome_prices: List of prices, e.g., ["0.98", "0.02"]
    
    Returns:
        - Team name if price > WINNER_PRICE_THRESHOLD (0.95)
        - "Pending" if no clear winner yet
    
    Example:
        >>> determine_winner(["Chiefs", "Raiders"], ["0.98", "0.02"])
        'Chiefs'
    """
    if not outcome_prices or not outcomes:
        return "Pending"
    
    try:
        prices = [float(p) for p in outcome_prices]
        max_price = max(prices)
        
        if max_price > WINNER_PRICE_THRESHOLD:
            winner_idx = prices.index(max_price)
            if len(outcomes) > winner_idx:
                return outcomes[winner_idx]
    except (ValueError, TypeError):
        pass
    
    return "Pending"


def extract_teams(outcomes, title):
    """
    Extract team names from market data.
    
    Priority:
    1. Use outcomes list if available (most accurate)
    2. Parse from event title ("Team A vs Team B" format)
    
    Args:
        outcomes: List of outcome strings from market, or None
        title: Event title string, e.g., "Chiefs vs Raiders"
    
    Returns:
        Tuple of (team_a, team_b) names, or ("Unknown", "Unknown") if parsing fails
    
    Example:
        >>> extract_teams(["Kansas City Chiefs", "Las Vegas Raiders"], "")
        ('Kansas City Chiefs', 'Las Vegas Raiders')
        >>> extract_teams(None, "Chiefs vs Raiders")
        ('Chiefs', 'Raiders')
    """
    if outcomes and len(outcomes) >= 2:
        return outcomes[0], outcomes[1]
    
    if ' vs ' in title:
        parts = title.split(' vs ', 1)
        return parts[0].strip(), parts[1].strip()
    
    return "Unknown", "Unknown"


# ------------------------------------------------------------------------------
# API Functions
# ------------------------------------------------------------------------------

def fetch_sports():
    """
    Fetch available sports from the Polymarket Gamma API.
    
    Makes a GET request to /sports and filters to our target leagues
    (NFL, CFB, NBA) based on series ID mapping.
    
    Returns:
        List of dicts with 'sport' (name) and 'series' (ID) keys
        for each matching sport, or empty list on error.
    
    Example return:
        [{'sport': 'nfl', 'series': '10187'}, {'sport': 'nba', 'series': '10345'}]
    """
    print("Fetching sports data from Polymarket Gamma API...")
    
    try:
        response = SESSION.get(GAMMA_SPORTS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        sports_data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching sports data: {e}")
        return []

    # Handle case where API returns dict with nested data (like fetch_events_for_series)
    if isinstance(sports_data, dict) and 'data' in sports_data:
        sports_data = sports_data['data']
    if not isinstance(sports_data, list):
        print(f"Warning: Unexpected sports response format (expected list)")
        return []

    matched_sports = []
    for sport in sports_data:
        series_id = sport.get('series')
        if series_id in SERIES_IDS:
            sport_name = sport.get('sport')
            matched_sports.append({
                'sport': sport_name,
                'series': series_id
            })
            print(f"  Found: {sport_name} (series: {series_id})")
    
    return matched_sports


def fetch_events_for_series(series_id):
    """
    Fetch all events (games) for a given series ID from the Gamma API.
    
    Uses pagination to fetch all results and makes separate requests
    for active (closed=false) and resolved (closed=true) events.
    
    Args:
        series_id: Polymarket series ID (e.g., "10187" for NFL)
    
    Returns:
        List of event dicts containing markets, titles, and metadata.
        Each event may contain multiple markets (moneyline, spread, etc.)
    
    Note:
        Uses PAGE_LIMIT (500) for pagination and continues until
        receiving fewer results than requested.
    """
    all_events = []
    
    # Fetch both closed (past) and active events
    for closed_status in ["true", "false"]:
        offset = 0
        
        while True:
            params = {
                "series_id": series_id,
                "limit": PAGE_LIMIT,
                "offset": offset,
                "order": "startDate",
                "ascending": "false",
                "closed": closed_status,
            }
            
            try:
                response = SESSION.get(GAMMA_EVENTS_URL, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                print(f"  Error fetching events (closed={closed_status}, offset={offset}): {e}")
                break
            
            # Handle different response formats
            if isinstance(data, list):
                batch = data
            elif isinstance(data, dict):
                batch = data.get('data', [])
            else:
                batch = []
            
            all_events.extend(batch)
            
            # Stop if we've received fewer results than the limit (end of data)
            if len(batch) < PAGE_LIMIT:
                break
            
            offset += PAGE_LIMIT
    
    return all_events


# ------------------------------------------------------------------------------
# Processing Functions
# ------------------------------------------------------------------------------

def process_market(market, event, sport, series):
    """
    Process a single market and extract moneyline data if valid.
    
    Checks if the market is a moneyline type, then extracts all relevant
    fields including outcomes, teams, resolution status, and identifiers.
    
    Args:
        market: Dict containing market data from Gamma API
        event: Parent event dict containing the market
        sport: Sport name (e.g., 'nfl', 'nba')
        series: Series ID for the sport
    
    Returns:
        Dict with standardized market fields if valid moneyline market,
        None otherwise (non-moneyline markets are filtered out).
    
    Output fields:
        sport, sports_series, event_id, market_id, condition_id,
        sports_market_type, match_title, game_start_time,
        outcome_team_a, outcome_team_b, is_resolved, winning_outcome,
        polymarket_slug
    """
    if not is_moneyline_market(market):
        return None
    
    title = event.get('title', '')
    
    # Parse outcomes and prices (may be JSON strings)
    outcomes = parse_json_field(market.get('outcomes'))
    outcome_prices = parse_json_field(market.get('outcomePrices'))
    
    # Determine resolution status
    is_resolved = coerce_bool(market.get('resolved', False)) or coerce_bool(market.get('closed', False))
    
    # Determine winner for resolved markets
    winner = determine_winner(outcomes, outcome_prices) if is_resolved else "Pending"
    
    # Extract team names
    team_a, team_b = extract_teams(outcomes, title)
    
    return {
        "sport": sport,
        "sports_series": series,
        "event_id": event.get('id'),
        "market_id": market.get('id'),
        "condition_id": market.get('conditionId'),
        "sports_market_type": market.get('sportsMarketType'),
        "match_title": title,
        "game_start_time": market.get('gameStartTime'),
        "outcome_team_a": team_a,
        "outcome_team_b": team_b,
        "is_resolved": is_resolved,
        "winning_outcome": winner,
        "polymarket_slug": market.get('slug'),
    }


def process_events(events, sport, series):
    """
    Process a batch of events and extract all valid moneyline markets.
    
    Iterates through events and extracts moneyline markets from each event.
    Shows progress for large batches.
    
    Args:
        events: List of event dicts from fetch_events_for_series()
        sport: Sport name for tagging output
        series: Series ID for tagging output
    
    Returns:
        List of market dicts ready for DataFrame conversion.
        Each dict contains standardized fields from process_market().
    
    Note:
        Progress is shown every 50 events when processing >100 events.
    """
    market_data = []
    total_events = len(events)

    for idx, event in enumerate(events, 1):
        # Process each market in the event
        for market in event.get('markets', []):
            market_row = process_market(market, event, sport, series)
            if market_row:
                market_data.append(market_row)

        # Show progress for large batches
        if total_events > 100 and idx % 50 == 0:
            print_progress(idx, total_events, prefix="  Processing events: ")

    # Clear progress line if shown
    if total_events > 100:
        print_progress(total_events, total_events, prefix="  Processing events: ")

    return market_data


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    """
    Main entry point for the Sports Market Fetcher.
    
    Orchestrates the full pipeline:
    1. Fetches available sports matching target series IDs
    2. For each sport, fetches all events via Gamma API
    3. Processes events to extract moneyline markets
    4. Saves results to db_markets.parquet sorted by game time
    
    Output file (PARQUET_PATH):
        Contains all moneyline markets with columns for sport,
        teams, game time, resolution status, and Polymarket IDs.
        game_start_time is stored as native datetime64 in Parquet.
    
    Console output:
        Progress updates for each sport showing event and market counts.
    """
    print("Starting Sports Market Fetcher...")
    print()
    
    # Fetch available sports
    sports = fetch_sports()
    if not sports:
        print("No matching sports found.")
        return
    
    print()
    
    # Fetch and process events for each sport
    all_markets = []
    
    for sport_info in sports:
        sport = sport_info['sport']
        series = sport_info['series']
        
        print(f"Fetching events for {sport.upper()} (series: {series})...")
        
        events = fetch_events_for_series(series)
        print(f"  Fetched {len(events)} events")
        
        markets = process_events(events, sport, series)
        print(f"  Found {len(markets)} moneyline markets")
        
        all_markets.extend(markets)
    
    print()
    
    # Save results
    if not all_markets:
        print("No matching moneyline markets found.")
        return
    
    df = pd.DataFrame(all_markets)
    try:
        df['game_start_time'] = pd.to_datetime(df['game_start_time'])
    except Exception as e:
        logger.warning(f"Some game_start_time values could not be parsed: {e}")
        df['game_start_time'] = pd.to_datetime(df['game_start_time'], errors='coerce')
    # Normalize is_resolved to bool for reliable downstream parsing
    if 'is_resolved' in df.columns:
        df['is_resolved'] = df['is_resolved'].apply(coerce_bool)
    else:
        df['is_resolved'] = False

    # Prefer resolved markets and known winners when deduplicating
    df['winning_outcome'] = df['winning_outcome'].fillna('')
    df['_has_winner'] = df['winning_outcome'].astype(str).str.strip().ne('') & df['winning_outcome'].ne('Pending')
    df = df.sort_values(
        ['_has_winner', 'is_resolved', 'game_start_time'],
        ascending=[False, False, False]
    )

    # Deduplicate by condition_id (keep best record based on sort above)
    original_count = len(df)
    df = df.drop_duplicates(subset=['condition_id'], keep='first')
    dedup_count = len(df)
    if original_count != dedup_count:
        print(f"Deduplicated: {original_count} -> {dedup_count} markets")
        logger.info(f"Deduplicated {original_count - dedup_count} duplicate markets")

    # Clean helper column
    if '_has_winner' in df.columns:
        df = df.drop(columns=['_has_winner'])

    # Final sort by game time (most recent first)
    df = df.sort_values('game_start_time', ascending=False)

    df.to_parquet(PARQUET_PATH, index=False, engine='pyarrow')

    logger.info(f"Saved {len(df)} markets to {PARQUET_PATH}")
    print(f"Total: {len(df)} markets")
    print(f"Saved to {PARQUET_PATH}")


if __name__ == "__main__":
    main()
