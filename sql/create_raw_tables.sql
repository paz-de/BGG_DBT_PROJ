-- Drop old tables if they exist, then recreate for Kaggle dataset structure
USE DATABASE BGG_DB;
USE SCHEMA RAW;

DROP TABLE IF EXISTS BGG_GAMES;
DROP TABLE IF EXISTS BGG_CATEGORIES;
DROP TABLE IF EXISTS BGG_MECHANICS;

-- Core game attributes (one row per game)
CREATE TABLE BGG_GAMES (
    BGG_ID              INTEGER,
    NAME                VARCHAR(500),
    YEAR_PUBLISHED      INTEGER,
    GAME_WEIGHT         FLOAT,
    AVG_RATING          FLOAT,
    BAYES_AVG_RATING    FLOAT,
    NUM_USER_RATINGS    INTEGER,
    MIN_PLAYERS         INTEGER,
    MAX_PLAYERS         INTEGER,
    MFG_PLAY_TIME       INTEGER,
    COM_MIN_PLAYTIME    INTEGER,
    COM_MAX_PLAYTIME    INTEGER,
    NUM_OWNED           INTEGER,
    NUM_WANT            INTEGER,
    NUM_WISH            INTEGER,
    NUM_EXPANSIONS      INTEGER,
    KICKSTARTED         INTEGER,
    FAMILY              VARCHAR(500),
    RANK_BOARDGAME      INTEGER,
    RANK_STRATEGY       INTEGER,
    RANK_FAMILY         INTEGER,
    RANK_THEMATIC       INTEGER,
    RANK_WAR            INTEGER,
    RANK_PARTY          INTEGER,
    RANK_ABSTRACT       INTEGER,
    RANK_CGS            INTEGER,
    RANK_CHILDRENS      INTEGER,
    _LOADED_AT          TIMESTAMP_TZ
);

-- Unpivoted mechanics (one row per game-mechanic combination)
CREATE TABLE BGG_MECHANICS (
    BGG_ID      INTEGER,
    MECHANIC    VARCHAR(300),
    _LOADED_AT  TIMESTAMP_TZ
);

-- Unpivoted themes (one row per game-theme combination)
CREATE TABLE BGG_THEMES (
    BGG_ID      INTEGER,
    THEME       VARCHAR(300),
    _LOADED_AT  TIMESTAMP_TZ
);

-- Unpivoted subcategories (one row per game-subcategory combination)
CREATE TABLE BGG_SUBCATEGORIES (
    BGG_ID          INTEGER,
    SUBCATEGORY     VARCHAR(300),
    _LOADED_AT      TIMESTAMP_TZ
);
