[README.md](https://github.com/user-attachments/files/26537883/README.md)
# BGG Data Engineering Portfolio Project

> **What makes a great board game?** An end-to-end data pipeline built on a modern cloud stack to find out.

🔗 [View the Tableau Public Dashboard](https://public.tableau.com/views/WhatMakesaGreatBoardGame/Dashboard1?:language=en-GB&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

---

## Overview

This project builds a complete data pipeline from raw BoardGameGeek data through to an interactive dashboard, using a modern data stack. It was built as a portfolio project to demonstrate hands-on skills across Python ingestion, Snowflake, dbt, and Tableau.

**The question it answers:** What patterns exist in the highest-rated board games? Does complexity drive rating? Which mechanics dominate the hobby?

---

## Architecture

```
Kaggle BGG Dataset (CSV)
        ↓
Python (pandas) — data cleaning + unpivoting
        ↓
Snowflake RAW schema — raw landing tables
        ↓
dbt — staging → intermediate → mart layers
        ↓
Tableau Public — interactive dashboard
```

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, pandas |
| Data Warehouse | Snowflake |
| Transformation | dbt Cloud |
| Visualisation | Tableau Public |
| Version Control | GitHub |

---

## Data Source

[BoardGameGeek Dataset — Kaggle](https://www.kaggle.com/datasets/threnjen/board-games-database-from-boardgamegeek)

20,000+ board games with ratings, complexity scores, mechanics, themes, and subcategories.

---

## Project Structure

```
├── src/
│   ├── csv_reader.py          # Reads and unpivots Kaggle CSVs using pandas
│   └── snowflake_loader.py    # Bulk loads DataFrames into Snowflake RAW
├── models/
│   ├── staging/               # One model per raw table, cleaning and typing only
│   ├── intermediate/          # Joins and aggregations
│   └── mart/                  # Final tables consumed by Tableau
├── sql/
│   └── create_raw_tables.sql  # DDL for Snowflake RAW schema
├── run_pipeline.py            # Pipeline entry point
└── requirements.txt
```

---

## dbt Model Layers

**Staging** — clean and rename raw columns, filter nulls, cast types. No joins.

**Intermediate** — join games to mechanics and themes, calculate counts and averages per mechanic/theme.

**Mart** — final denormalised tables ready for Tableau:
- `mart_top_games` — all ranked games with complexity bands
- `mart_mechanic_summary` — mechanic popularity and average ratings
- `mart_theme_summary` — theme-level aggregates
- `mart_complexity_bands` — distribution of games across complexity tiers

---

## Key Findings

- **Gloomhaven** and **Pandemic Legacy: Season 1** are the highest rated games on BGG
- Heavier games (complexity 3.5+) tend to cluster at higher ratings — but the relationship isn't linear
- **Dice Rolling** and **Hand Management** are the most common mechanics across all games
- The majority of games are Light or Medium-Light complexity — Heavy games are a small but highly rated minority

---

## How to Run

1. Clone the repo
2. Create a `.env` file using `.env.example` as a template
3. Download the BGG dataset from Kaggle and place CSVs in a `data/` folder
4. Run `pip install -r requirements.txt`
5. Run `python run_pipeline.py`
6. Run `dbt run` in dbt Cloud

---

## Skills Demonstrated

- Python data ingestion and transformation (pandas melt for wide-to-long unpivoting)
- Snowflake DDL, schema design, and bulk loading with `write_pandas`
- dbt layered modelling (staging / intermediate / mart)
- dbt sources, refs, and model materialisation strategies
- Tableau dashboard design and publishing to Tableau Public
- Git version control and project documentation

## Data Lineage (DAG)

<img width="1397" height="710" alt="image" src="https://github.com/user-attachments/assets/140a7bc5-dd57-4a04-88a3-534add251253" />

The DAG shows the full dependency graph across 4 layers:

- **Sources (SRC)** — the four RAW tables loaded by Python: BGG_GAMES, BGG_MECHANICS, BGG_THEMES, BGG_SUBCATEGORIES
- **Staging (MDL)** — one model per source, cleaning and typing only, no joins
- **Intermediate (MDL)** — joins games to mechanics and themes, calculates counts and averages per mechanic and theme
- **Mart (MDL)** — final denormalised tables consumed by Tableau: top games, mechanic summary, theme summary, and complexity bands

dbt resolves the execution order automatically by parsing `{{ ref() }}` calls across all models.
