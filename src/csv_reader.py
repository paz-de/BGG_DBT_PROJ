"""
csv_reader.py
Reads the Kaggle BGG dataset CSVs and prepares DataFrames for loading.

Wide binary-flag tables (mechanics, themes, subcategories) are unpivoted
using pandas melt — turning hundreds of columns into clean long-format rows.
"""

import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path("data")  # put your CSVs in a /data folder in the project root


def _loaded_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_games() -> pd.DataFrame:
    """
    Read games.csv and select + rename the columns we care about.
    Drops rows where BGGId is null, converts rank columns from object to numeric
    (BGG stores 'Not Ranked' as a string — we coerce those to NaN → None).
    """
    print("  Reading games.csv...")
    df = pd.read_csv(DATA_DIR / "games.csv")

    # Columns to keep and their new names
    col_map = {
        'BGGId':                'BGG_ID',
        'Name':                 'NAME',
        'YearPublished':        'YEAR_PUBLISHED',
        'GameWeight':           'GAME_WEIGHT',
        'AvgRating':            'AVG_RATING',
        'BayesAvgRating':       'BAYES_AVG_RATING',
        'NumUserRatings':       'NUM_USER_RATINGS',
        'MinPlayers':           'MIN_PLAYERS',
        'MaxPlayers':           'MAX_PLAYERS',
        'MfgPlayTime':          'MFG_PLAY_TIME',
        'ComMinPlaytime':       'COM_MIN_PLAYTIME',
        'ComMaxPlaytime':       'COM_MAX_PLAYTIME',
        'NumOwned':             'NUM_OWNED',
        'NumWant':              'NUM_WANT',
        'NumWish':              'NUM_WISH',
        'NumExpansions':        'NUM_EXPANSIONS',
        'Kickstarted':          'KICKSTARTED',
        'Family':               'FAMILY',
        'Rank:boardgame':       'RANK_BOARDGAME',
        'Rank:strategygames':   'RANK_STRATEGY',
        'Rank:familygames':     'RANK_FAMILY',
        'Rank:thematic':        'RANK_THEMATIC',
        'Rank:wargames':        'RANK_WAR',
        'Rank:partygames':      'RANK_PARTY',
        'Rank:abstracts':       'RANK_ABSTRACT',
        'Rank:cgs':             'RANK_CGS',
        'Rank:childrensgames':  'RANK_CHILDRENS',
    }

    # Only keep columns that actually exist in this version of the CSV
    existing_cols = {k: v for k, v in col_map.items() if k in df.columns}
    df = df[list(existing_cols.keys())].rename(columns=existing_cols)

    # Rank columns come in as strings ('Not Ranked' or a number) — coerce to numeric
    rank_cols = [c for c in df.columns if c.startswith('RANK_')]
    for col in rank_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['BGG_ID'] = pd.to_numeric(df['BGG_ID'], errors='coerce')
    df = df.dropna(subset=['BGG_ID'])
    df['BGG_ID'] = df['BGG_ID'].astype(int)

    df['_LOADED_AT'] = _loaded_at()

    print(f"    → {len(df):,} games loaded")
    return df


def _unpivot_binary_file(filename: str, id_col: str, value_col_name: str) -> pd.DataFrame:
    """
    Generic unpivot for wide binary-flag CSVs (mechanics, themes, subcategories).

    Input shape:  BGGId | MechanicA | MechanicB | MechanicC ...
                  101   |     1     |     0     |     1     ...

    Output shape: BGG_ID | <value_col_name>
                  101    | MechanicA
                  101    | MechanicC
    """
    print(f"  Reading {filename}...")
    df = pd.read_csv(DATA_DIR / filename)

    # All columns except BGGId are binary flag columns
    flag_cols = [c for c in df.columns if c != id_col]

    # Melt wide → long
    melted = df.melt(id_vars=[id_col], value_vars=flag_cols,
                     var_name=value_col_name, value_name='flag')

    # Keep only rows where the flag is 1 (game has this mechanic/theme/subcategory)
    melted = melted[melted['flag'] == 1].drop(columns='flag')

    melted = melted.rename(columns={id_col: 'BGG_ID'})
    melted['BGG_ID'] = pd.to_numeric(melted['BGG_ID'], errors='coerce')
    melted = melted.dropna(subset=['BGG_ID'])
    melted['BGG_ID'] = melted['BGG_ID'].astype(int)

    melted['_LOADED_AT'] = _loaded_at()
    melted = melted.reset_index(drop=True)

    print(f"    → {len(melted):,} rows after unpivot")
    return melted


def read_mechanics() -> pd.DataFrame:
    return _unpivot_binary_file('mechanics.csv', 'BGGId', 'MECHANIC')


def read_themes() -> pd.DataFrame:
    return _unpivot_binary_file('themes.csv', 'BGGId', 'THEME')


def read_subcategories() -> pd.DataFrame:
    return _unpivot_binary_file('subcategories.csv', 'BGGId', 'SUBCATEGORY')
