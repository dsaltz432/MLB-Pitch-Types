
attach "/Users/danielsaltz/Documents/MLB_Project/data/mlb-gameday/mlb-gameday.db" as toMerge;

BEGIN TRANSACTION;

CREATE TABLE atbats
(   pitcher_id          REAL,
    batter_id           REAL,
    p_throws            TEXT,
    b_stands            TEXT,
    batter_name         TEXT,
    pitcher_name        TEXT,
    event               TEXT,
    game_id             TEXT
);

INSERT INTO atbats
    SELECT pitcher, batter, p_throws, stand, batter_name, pitcher_name, event, gameday_link
    FROM toMerge.atbat
    WHERE toMerge.atbat.pitcher in (SELECT pitcher_id from pitches)
    AND substr(toMerge.atbat.gameday_link, 5, 4) in ('2015', '2016', '2017', '2018', '2019');

COMMIT;

detach toMerge;