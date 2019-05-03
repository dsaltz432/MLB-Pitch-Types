import sqlite3
import logging
import constants

logger = logging.getLogger(constants.LOGGER_NAME)


def create_connection():
    """ create a database connection to the SQLite database
        specified by constants.DB_FILE_NAME
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(constants.DB_FILE_NAME)
        return conn
    except sqlite3.Error as e:
        logging.info(e)

    return None


def create_connection_to_pitch_fx_db():
    """ create a database connection to the SQLite database
        specified by constants.PITCH_FX_DB_FILE_NAME
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(constants.PITCH_FX_DB_FILE_NAME)
        return conn
    except sqlite3.Error as e:
        logging.info(e)

    return None


def create_urls_table(conn):
    """ create urls table if it doesn't exist already
        :param conn: the Connection object
    """
    try:
        sql_create_urls_table = """ CREATE TABLE IF NOT EXISTS urls (
                                            url                   TEXT  NOT NULL,
                                            expanded_table_url    TEXT  NOT NULL,
                                            pitcher_id            INT   NOT NULL,
                                            pitcher_name          TEXT  NOT NULL,
                                            game_id               TEXT  NOT NULL,
                                            day                   INT   NOT NULL,
                                            month                 INT   NOT NULL,
                                            year                  TEXT  NOT NULL,
                                            PRIMARY KEY(pitcher_id, game_id) 
                                        ); """
        create_table(conn, sql_create_urls_table)

    except sqlite3.Error as e:
        logging.info(e)

    return None


def create_full_pitches_table(conn):
    """ create full_pitches table if it doesn't exist already
        :param conn: the Connection object
    """
    try:
        sql_create_full_pitches_table = """ CREATE TABLE IF NOT EXISTS full_pitches (
                                                    pitcher_id          INT     NOT NULL,
                                                    game_id             TEXT    NOT NULL,
                                                    type_of_batters     TEXT    NOT NULL,
                                                    type_of_data        TEXT    NOT NULL,
                                                    pitch_type_code     TEXT    NOT NULL,
                                                    pitch_type_desc     TEXT    NOT NULL,
                                                    velo                REAL    NOT NULL,
                                                    h_break             REAL    NOT NULL,
                                                    v_break             REAL    NOT NULL,
                                                    count               REAL    NOT NULL,
                                                    strikes             REAL    NOT NULL,
                                                    swings              REAL    NOT NULL,
                                                    whiffs              REAL    NOT NULL,
                                                    bib                 REAL    NOT NULL,
                                                    snip                REAL    NOT NULL,
                                                    lwts                REAL    NOT NULL,
                                                    PRIMARY KEY(pitcher_id, game_id, pitch_type_code, type_of_batters) 
                                                ); """
        create_table(conn, sql_create_full_pitches_table)

    except sqlite3.Error as e:
        logging.info(e)

    return None


def create_pitch_frequencies_table(conn):
    """ create pitch_frequencies table if it doesn't exist already
        :param conn: the Connection object
    """
    try:
        sql_create_pitch_frequencies_table = """ CREATE TABLE IF NOT EXISTS pitch_frequencies (
                                                    pitcher_id          INT     NOT NULL,
                                                    p_throws            TEXT    NOT NULL,
                                                    type_of_batters     TEXT    NOT NULL,
                                                    percent_ch          REAL    NOT NULL,
                                                    percent_cu          REAL    NOT NULL,
                                                    percent_fa          REAL    NOT NULL,
                                                    percent_fc          REAL    NOT NULL,
                                                    percent_ff          REAL    NOT NULL,
                                                    percent_fs          REAL    NOT NULL,
                                                    percent_ft          REAL    NOT NULL,
                                                    percent_si          REAL    NOT NULL,
                                                    percent_sl          REAL    NOT NULL,
                                                    percent_other       REAL    NOT NULL,
                                                    velo_index          REAL    NOT NULL,
                                                    PRIMARY KEY(pitcher_id, p_throws, type_of_batters)
                                                ); """
        create_table(conn, sql_create_pitch_frequencies_table)

    except sqlite3.Error as e:
        logging.info(e)

    return None


def insert_rows_into_urls_table(url_table_rows):

    conn = create_connection()

    # Create urls table if it doesn't exist
    create_urls_table(conn)

    logging.info("trying to insert {} rows into full_pitches table".format(len(url_table_rows)))

    try:

        sql = "insert into urls (url, expanded_table_url, pitcher_id, pitcher_name, game_id, day, month, year) \
               values (?,?,?,?,?,?,?,?)"

        cursor = conn.cursor()
        cursor.executemany(sql, url_table_rows)
        conn.commit()
        conn.close()

        logging.info("inserted {} rows into table".format(len(url_table_rows)))
        return True

    except sqlite3.Error as e:
        logging.info(e)
        return False


def insert_rows_into_full_pitches_table(full_pitches_table_rows):

    conn = create_connection()

    # Create full_pitches table if it doesn't exist
    create_full_pitches_table(conn)

    logging.info("trying to insert {} rows into full_pitches table".format(len(full_pitches_table_rows)))

    try:

        sql = "insert into full_pitches (pitcher_id, game_id, type_of_batters, type_of_data, pitch_type_code, " \
              "pitch_type_desc, velo, h_break, v_break, count, strikes, swings, whiffs, bib, snip, lwts) \
               values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        cursor = conn.cursor()
        cursor.executemany(sql, full_pitches_table_rows)
        conn.commit()
        conn.close()

        logging.info("inserted {} rows into table".format(len(full_pitches_table_rows)))
        return True

    except sqlite3.Error as e:
        logging.info(e)
        return False


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        cursor.close()
    except sqlite3.Error as e:
        logging.info(e)


def get_data_from_urls_table(year, month):
    """ select url data from the urls table given a year and month
    :param year: a year
    :param month: a month
    :return: A list (url, pitcher_id, game_id) retrieved for a given year and month
    """

    logging.info("trying to select data from urls table")

    try:
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("select url, pitcher_id, game_id from urls where year=? and month=?", (year, month,))
        rows = cursor.fetchall()
        logging.info("selected {} rows from urls table".format(len(rows)))

        cursor.close()
        conn.close()
        return rows

    except sqlite3.Error as e:
        logging.info(e)
        return None
