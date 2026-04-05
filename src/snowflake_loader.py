"""
snowflake_loader.py
Handles the Snowflake connection and bulk loading of DataFrames into RAW schema.
Uses write_pandas() for efficient bulk inserts — much faster than executemany for
large datasets. Strategy is truncate-and-reload (full refresh each run).
"""

import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


def get_connection():
    """
    Create and return a Snowflake connection using credentials from .env.
    To find your account identifier in Snowflake:
      Admin → Accounts → hover your account name → copy the account identifier
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


def _truncate_and_load(conn, df: pd.DataFrame, table_name: str):
    """
    Truncate a RAW table and bulk load a DataFrame into it using write_pandas.
    Column names in the DataFrame must match the Snowflake table exactly.
    """
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE BGG_DB.RAW.{table_name}")
    cursor.close()

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database='BGG_DB',
        schema='RAW',
        auto_create_table=False,
        overwrite=False
    )

    if success:
        print(f"  ✅ {table_name}: {num_rows:,} rows loaded ({num_chunks} chunk(s))")
    else:
        raise RuntimeError(f"write_pandas failed for {table_name}")


def load_games(conn, df: pd.DataFrame):
    _truncate_and_load(conn, df, 'BGG_GAMES')


def load_mechanics(conn, df: pd.DataFrame):
    _truncate_and_load(conn, df, 'BGG_MECHANICS')


def load_themes(conn, df: pd.DataFrame):
    _truncate_and_load(conn, df, 'BGG_THEMES')


def load_subcategories(conn, df: pd.DataFrame):
    _truncate_and_load(conn, df, 'BGG_SUBCATEGORIES')
