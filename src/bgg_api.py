"""
bgg_api.py
Handles all BGG XML API2 calls and parsing.
Fetches the hot list for game IDs, then retrieves full game details in batches.
"""

import requests
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

BASE_URL = "https://boardgamegeek.com/xmlapi2"
BATCH_SIZE = 20       # BGG allows multiple IDs per call, 20 is a safe batch size
RATE_LIMIT_DELAY = 2  # seconds between requests - be respectful to the API


def get_hot_game_ids() -> list[str]:
    """
    Fetch the BGG hotness list.
    Returns a list of game ID strings (top 50 hottest games right now).
    """
    url = f"{BASE_URL}/hot?type=boardgame"
    response = requests.get(url)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    game_ids = [item.get('id') for item in root.findall('item')]
    print(f"  Hot list returned {len(game_ids)} game IDs")
    return game_ids


def get_game_details_batch(game_ids: list[str]) -> bytes:
    """
    Fetch full details for up to 20 games in one API call.
    stats=1 includes ratings, complexity weight, and BGG rank.
    BGG sometimes returns 202 (processing) - we handle that with a retry.
    """
    ids_str = ','.join(game_ids)
    url = f"{BASE_URL}/thing?id={ids_str}&stats=1"

    time.sleep(RATE_LIMIT_DELAY)
    response = requests.get(url)

    if response.status_code == 202:
        print("  BGG returned 202 (still processing) - waiting 5s and retrying...")
        time.sleep(5)
        response = requests.get(url)

    response.raise_for_status()
    return response.content


def _safe_get(element, path: str, attr: str = 'value'):
    """Helper to safely get an attribute from a child element."""
    el = element.find(path)
    if el is not None:
        val = el.get(attr)
        return val if val not in (None, 'N/A') else None
    return None


def parse_game(item_el) -> tuple[dict, list[dict], list[dict]]:
    """
    Parse a single <item> XML element into:
      - a game dict (one row for BGG_GAMES)
      - a list of category dicts (rows for BGG_CATEGORIES)
      - a list of mechanic dicts (rows for BGG_MECHANICS)
    """
    loaded_at = datetime.now(timezone.utc).isoformat()
    game_id = item_el.get('id')

    # Primary name
    name = None
    for name_el in item_el.findall('name'):
        if name_el.get('type') == 'primary':
            name = name_el.get('value')
            break

    # Basic attributes
    year_published  = _safe_get(item_el, 'yearpublished')
    min_players     = _safe_get(item_el, 'minplayers')
    max_players     = _safe_get(item_el, 'maxplayers')
    min_playtime    = _safe_get(item_el, 'minplaytime')
    max_playtime    = _safe_get(item_el, 'maxplaytime')

    # Ratings and stats (nested under statistics/ratings)
    avg_rating        = None
    bayes_rating      = None
    num_ratings       = None
    complexity_weight = None
    bgg_rank          = None

    stats = item_el.find('statistics/ratings')
    if stats is not None:
        avg_rating        = _safe_get(stats, 'average')
        bayes_rating      = _safe_get(stats, 'bayesaverage')
        num_ratings       = _safe_get(stats, 'usersrated')
        complexity_weight = _safe_get(stats, 'averageweight')

        ranks_el = stats.find('ranks')
        if ranks_el is not None:
            for rank_el in ranks_el.findall('rank'):
                if rank_el.get('name') == 'boardgame':
                    rank_val = rank_el.get('value')
                    bgg_rank = rank_val if rank_val != 'Not Ranked' else None
                    break

    game = {
        'game_id':           game_id,
        'name':              name,
        'year_published':    year_published,
        'min_players':       min_players,
        'max_players':       max_players,
        'min_playtime':      min_playtime,
        'max_playtime':      max_playtime,
        'avg_rating':        avg_rating,
        'bayes_rating':      bayes_rating,
        'num_ratings':       num_ratings,
        'complexity_weight': complexity_weight,
        'bgg_rank':          bgg_rank,
        '_loaded_at':        loaded_at
    }

    # Categories - one row per category per game
    categories = [
        {'game_id': game_id, 'category': link.get('value'), '_loaded_at': loaded_at}
        for link in item_el.findall('link')
        if link.get('type') == 'boardgamecategory'
    ]

    # Mechanics - one row per mechanic per game
    mechanics = [
        {'game_id': game_id, 'mechanic': link.get('value'), '_loaded_at': loaded_at}
        for link in item_el.findall('link')
        if link.get('type') == 'boardgamemechanic'
    ]

    return game, categories, mechanics


def parse_games_response(xml_content: bytes) -> tuple[list, list, list]:
    """Parse a full API response containing multiple game items."""
    root = ET.fromstring(xml_content)
    all_games, all_categories, all_mechanics = [], [], []

    for item_el in root.findall('item'):
        game, categories, mechanics = parse_game(item_el)
        all_games.append(game)
        all_categories.extend(categories)
        all_mechanics.extend(mechanics)

    return all_games, all_categories, all_mechanics


def extract_all_games(game_ids: list[str]) -> tuple[list, list, list]:
    """
    Main extraction function.
    Splits game IDs into batches and fetches details for all of them.
    Returns combined lists of games, categories, and mechanics.
    """
    all_games, all_categories, all_mechanics = [], [], []

    batches = [game_ids[i:i + BATCH_SIZE] for i in range(0, len(game_ids), BATCH_SIZE)]
    total_batches = len(batches)

    for i, batch in enumerate(batches, start=1):
        print(f"  Fetching batch {i}/{total_batches} ({len(batch)} games)...")
        xml_content = get_game_details_batch(batch)
        games, categories, mechanics = parse_games_response(xml_content)
        all_games.extend(games)
        all_categories.extend(categories)
        all_mechanics.extend(mechanics)
        print(f"    → {len(games)} games parsed")

    return all_games, all_categories, all_mechanics
