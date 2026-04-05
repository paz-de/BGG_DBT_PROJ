"""
run_pipeline.py
Entry point for the BGG data pipeline.
Run this file to extract from BGG API and load into Snowflake RAW schema.
"""

from src.bgg_api import get_hot_game_ids, extract_all_games
from src.snowflake_loader import get_connection, load_games, load_categories, load_mechanics


def run():
    print("=" * 40)
    print("  BGG Data Pipeline — Phase 2")
    print("=" * 40)

    # Step 1: Get game IDs from BGG hot list
    print("\n[1/3] Fetching game IDs from BGG hot list...")
    game_ids = get_hot_game_ids()
    print(f"  → {len(game_ids)} games to process")

    # Step 2: Extract full game details from BGG API
    print("\n[2/3] Extracting game details from BGG API...")
    games, categories, mechanics = extract_all_games(game_ids)
    print(f"  → Extracted: {len(games)} games | "
          f"{len(categories)} category rows | "
          f"{len(mechanics)} mechanic rows")

    # Step 3: Load into Snowflake
    print("\n[3/3] Loading into Snowflake RAW schema...")
    conn = get_connection()
    try:
        load_games(conn, games)
        load_categories(conn, categories)
        load_mechanics(conn, mechanics)
        conn.commit()
        print("\n✅ Pipeline complete! Check BGG_DB.RAW in Snowflake.")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
