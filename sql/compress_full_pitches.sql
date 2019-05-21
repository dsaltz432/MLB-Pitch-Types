
attach "/Users/danielsaltz/Documents/MLB_Project/data/brooks-baseball/brooks-baseball.db" as toMerge;

BEGIN TRANSACTION;

CREATE TABLE pitches
(   pitcher_id          INT     NOT NULL,
    game_id             TEXT    NOT NULL,
    b_stands            TEXT    NOT NULL,
    pitch_type_code     TEXT    NOT NULL,
    pitch_type_desc     TEXT    NOT NULL,
    velo                REAL    NOT NULL,
    count               REAL    NOT NULL,
    PRIMARY KEY(pitcher_id, game_id, pitch_type_code, b_stands)
);

INSERT INTO pitches
    SELECT pitcher_id, game_id, type_of_batters,
           pitch_type_code, pitch_type_desc, velo, count
    FROM toMerge.full_pitches
    WHERE type_of_data='pitch_info'
    AND pitcher_id != '519381'
    AND substr(game_id, 5, 4) in ('2015', '2016', '2017', '2018', '2019');

UPDATE pitches
    SET b_stands = 'R'
    WHERE b_stands = 'RHB';

UPDATE pitches
    SET b_stands = 'L'
    WHERE b_stands = 'LHB';

COMMIT;

detach toMerge