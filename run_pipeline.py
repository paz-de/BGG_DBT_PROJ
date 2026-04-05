"""
run_pipeline.py
Entry point for the BGG data pipeline.
Run this file to extract from BGG API and load into Snowflake RAW schema.
"""

from src.csv_reader import read_games, read_mechanics, read_themes, read_subcategories
from src.snowflake_loader_new import get_connection, load_games, load_mechanics, load_themes, load_subcategories


def run():
    print("=" * 40)
    print("  BGG Data Pipeline — Phase 2")
    print("=" * 40)

    # Step 1: Read and prepare CSVs
    print("\n[1/2] Reading CSVs and preparing data...")
    games_df        = read_games()
    mechanics_df    = read_mechanics()
    themes_df       = read_themes()
    subcategories_df = read_subcategories()

    # Step 2: Load into Snowflake
    print("\n[2/2] Loading into Snowflake RAW schema...")
    conn = get_connection()
    try:
        load_games(conn, games_df)
        load_mechanics(conn, mechanics_df)
        load_themes(conn, themes_df)
        load_subcategories(conn, subcategories_df)
        print("\n✅ Pipeline complete! Check BGG_DB.RAW in Snowflake.")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
