import pandas
import db_utils


def generate_weighted_totals_table():
    conn = db_utils.create_connection()

    # Get the total number of pitches for each pitch type for every pitcher
    pitch_type_totals = pandas.read_sql_query("select pitcher_id, type_of_batters, pitch_type_code,"
                                              " sum(count) as pitch_type_total"
                                              " from full_pitches where type_of_data='pitch_info'"
                                              " group by pitcher_id, type_of_batters, pitch_type_code;", conn)
    full_pitches = pandas.read_sql_query("select * from full_pitches where type_of_data='pitch_info';", conn)

    # Join pitch_type_total to each pitcher for each pitch type
    frequency_table = pandas.merge(pitch_type_totals, full_pitches, how='left',
                                   left_on=['pitcher_id', 'type_of_batters', 'pitch_type_code'],
                                   right_on=['pitcher_id', 'type_of_batters', 'pitch_type_code'])
    frequency_table.to_sql("frequency_table", conn, if_exists="replace")

    # Weight pitcher's stats by the number of pitches thrown in that appearance
    #    compared to total pitches thrown for that pitch type
    weighted_totals = pandas.read_sql_query("""select pitcher_id, type_of_batters, pitch_type_code, pitch_type_total,
                                                sum(velo*(count/pitch_type_total)) as velo,
                                                sum(h_break*(h_break/pitch_type_total)) as h_break,
                                                sum(v_break*(v_break/pitch_type_total)) as v_break,
                                                sum(strikes*(strikes/pitch_type_total)) as strikes,
                                                sum(swings*(swings/pitch_type_total)) as swings,
                                                sum(whiffs*(whiffs/pitch_type_total)) as whiffs,
                                                sum(bib*(bib/pitch_type_total)) as bib,
                                                sum(snip*(snip/pitch_type_total)) as snip,
                                                sum(lwts*(lwts/pitch_type_total)) as lwts
                                                from frequency_table 
                                                group by pitcher_id, type_of_batters, 
                                                pitch_type_code, pitch_type_total;""", conn)
    cursor = conn.cursor()
    cursor.execute("drop table frequency_table;")
    cursor.close()

    # Add the total number of pitches as a column in weighted_totals table
    weighted_totals.to_sql("weighted_totals", conn, if_exists="replace")
    total_count_table = pandas.read_sql_query("select pitcher_id, type_of_batters, sum(pitch_type_total) as total_count"
                                              " from weighted_totals group by pitcher_id, type_of_batters;", conn)
    weighted_totals = pandas.merge(weighted_totals, total_count_table, how='left',
                                   left_on=['pitcher_id', 'type_of_batters'],
                                   right_on=['pitcher_id', 'type_of_batters'])

    # Add p_throws as a column - pull from pitchFx.db database
    pitch_fx_conn = db_utils.create_connection_to_pitch_fx_db()
    weighted_totals = pandas.merge(weighted_totals,
                                   pandas.read_sql_query("select distinct pitcher as pitcher_id, p_throws from atbat;",
                                                         pitch_fx_conn))
    pitch_fx_conn.close()
    weighted_totals.to_sql("weighted_totals", conn, if_exists="replace")

    # Filter out pitchers who have minimal appearances
    min_total_count = 100
    cursor = conn.cursor()
    cursor.execute("delete from weighted_totals where total_count < ?", (min_total_count,))
    cursor.close()

    conn.close()


def generate_pitch_frequencies_table():
    conn = db_utils.create_connection()
    db_utils.create_pitch_frequencies_table(conn)

    cursor = conn.cursor()
    cursor.execute("select distinct pitcher_id, p_throws, type_of_batters from weighted_totals;")
    rows = cursor.fetchall()
    cursor.close()

    pitch_frequencies_rows = []
    for (pitcher_id, p_throws, type_of_batters) in rows:
        percent_ch = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "CH")
        percent_cs = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "CS")
        percent_cu = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "CU")
        percent_fa = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "FA")
        percent_fc = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "FC")
        percent_ff = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "FF")
        percent_fo = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "FO")
        percent_fs = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "FS")
        percent_ft = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "FT")
        percent_kc = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "KC")
        percent_kn = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "KN")
        percent_sb = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "SB")
        percent_si = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "SI")
        percent_sl = get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, "SL")

        pitch_frequencies_rows.append((pitcher_id, p_throws, type_of_batters, percent_ch, percent_cs, percent_cu,
                                       percent_fa, percent_fc, percent_ff, percent_fo, percent_fs, percent_ft,
                                       percent_kc, percent_kn, percent_sb, percent_si, percent_sl))

    sql = "insert into pitch_frequencies (pitcher_id, p_throws, type_of_batters, percent_ch, percent_cs, percent_cu," \
          " percent_fa, percent_fc, percent_ff, percent_fo, percent_fs, percent_ft, percent_kc, percent_kn," \
          " percent_sb, percent_si, percent_sl) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    cursor = conn.cursor()
    cursor.executemany(sql, pitch_frequencies_rows)
    conn.commit()
    conn.close()


def get_pitch_frequency(conn, pitcher_id, p_throws, type_of_batters, pitch_type_code):
    cursor = conn.cursor()
    cursor.execute("select pitch_type_total/total_count from weighted_totals where pitcher_id = {}"
                   " and p_throws = %s and type_of_batters = %s and pitch_type_code = %s"
                   .format(pitcher_id, p_throws, type_of_batters, pitch_type_code))

    result = cursor.fetchone()

    if result is None:
        percent_thrown = 0
    else:
        percent_thrown = result[0]

    cursor.close()
    return percent_thrown


# We want to divide the dataset into 4 parts: RHP vs. RHB, RHP vs. LHB, LHP vs. RHB, LHP vs. LHB
# def divide_data_by_dexterity():
#     x = 1
    # totals_LvL = pandas.read_sql_query("select * from weighted_totals where p_throws='L' and type_of_batters='LHB';", conn)
    # totals_LvR = pandas.read_sql_query("select * from weighted_totals where p_throws='L' and type_of_batters='LHB';", conn)
    # totals_RvL = pandas.read_sql_query("select * from weighted_totals where p_throws='R' and type_of_batters='RHB';", conn)
    # totals_RvR = pandas.read_sql_query("select * from weighted_totals where p_throws='R' and type_of_batters='RHB';", conn)

