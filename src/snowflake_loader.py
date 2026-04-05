"""
snowflake_loader.py
Handles the Snowflake connection and all data loading into RAW schema tables.
Uses truncate-and-reload strategy (full refresh each run).
"""

import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Create and return a Snowflake connection using credentials from .env.
    To find your account identifier in Snowflake:
      Admin → Accounts → hover your account → copy the account identifier
      Format is usually: orgname-accountname  (e.g. myorg-bgg123)
    """
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database='BGG_DB',
        schema='RAW'
    )


def load_games(conn, games: list[dict]):
    """Truncate and reload BGG_GAMES."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BGG_DB.RAW.BGG_GAMES")

    sql = """
        INSERT INTO BGG_DB.RAW.BGG_GAMES
            (GAME_ID, NAME, YEAR_PUBLISHED, MIN_PLAYERS, MAX_PLAYERS,
             MIN_PLAYTIME, MAX_PLAYTIME, AVG_RATING, BAYES_RATING,
             NUM_RATINGS, COMPLEXITY_WEIGHT, BGG_RANK, _LOADED_AT)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = [(
        g['game_id'], g['name'], g['year_published'],
        g['min_players'], g['max_players'],
        g['min_playtime'], g['max_playtime'],
        g['avg_rating'], g['bayes_rating'],
        g['num_ratings'], g['complexity_weight'],
        g['bgg_rank'], g['_loaded_at']
    ) for g in games]

    cursor.executemany(sql, rows)
    print(f"  Loaded {len(rows)} rows into BGG_GAMES")
    cursor.close()


def load_categories(conn, categories: list[dict]):
    """Truncate and reload BGG_CATEGORIES."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BGG_DB.RAW.BGG_CATEGORIES")

    sql = """
        INSERT INTO BGG_DB.RAW.BGG_CATEGORIES (GAME_ID, CATEGORY, _LOADED_AT)
        VALUES (%s, %s, %s)
    """
    rows = [(c['game_id'], c['category'], c['_loaded_at']) for c in categories]
    cursor.executemany(sql, rows)
    print(f"  Loaded {len(rows)} rows into BGG_CATEGORIES")
    cursor.close()


def load_mechanics(conn, mechanics: list[dict]):
    """Truncate and reload BGG_MECHANICS."""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE BGG_DB.RAW.BGG_MECHANICS")

    sql = """
        INSERT INTO BGG_DB.RAW.BGG_MECHANICS (GAME_ID, MECHANIC, _LOADED_AT)
        VALUES (%s, %s, %s)
    """
    rows = [(m['game_id'], m['mechanic'], m['_loaded_at']) for m in mechanics]
    cursor.executemany(sql, rows)
    print(f"  Loaded {len(rows)} rows into BGG_MECHANICS")
    cursor.close()
