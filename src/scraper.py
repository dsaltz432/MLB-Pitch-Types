import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
import db_utils
import logging
import constants

logger = logging.getLogger(constants.LOGGER_NAME)


def generate_urls_table():
    # Create thread pool
    pool = Pool(20)

    for year in constants.YEARS:
        for month in constants.MONTHS:
            logging.info("Started scraping for year={}, month={}".format(year, month))

            # Execute get_url_requests for every combination of year/month/day
            params = [(year, month, day) for day in constants.DAYS]
            pool_results = pool.map(get_url_requests, params)

            # Flatten the list of lists into a single list of all results
            url_table_rows = [item for sublist in pool_results for item in sublist]
            logger.info("Found {} rows from pool_results".format(len(url_table_rows)))

            # Write results to urls table in db
            if url_table_rows:
                db_utils.insert_rows_into_urls_table(url_table_rows)

            logging.info("Finished scraping for year={}, month={}".format(year, month))

    # Clean up thread pool
    pool.terminate()
    pool.join()


def generate_full_pitches_table():
    # Create thread pool
    pool = Pool(20)

    for year in constants.YEARS:
        for month in constants.MONTHS:
            logging.info("Started scraping for year={}, month={}".format(year, month))

            # Execute get_url_requests for all days in that year/month
            params = db_utils.get_data_from_urls_table(year, month)
            pool_results = pool.map(get_full_pitches_from_url, params)

            # Flatten the list of lists into a single list of all results
            full_pitches_table_rows = [item for sublist in pool_results for item in sublist]
            logger.info("Found {} rows from pool_results".format(len(full_pitches_table_rows)))

            # Write results to full_pitches table in db
            if full_pitches_table_rows:
                db_utils.insert_rows_into_full_pitches_table(full_pitches_table_rows)

            logging.info("Finished scraping for year={}, month={}".format(year, month))

    # Clean up thread pool
    pool.terminate()
    pool.join()


def get_url_requests(params):
    (year, month, day) = params
    url_table_rows = []
    url = "{}?month={}&day={}&year={}&prevDate={}&league=mlb".format(constants.BASE_URL, month, day, year, month + day)
    game_ids = get_game_ids_from_url(url)
    if game_ids:
        for game_id in game_ids:
            url = "{}?month={}&day={}&year={}&prevDate={}&league=mlb&game={}" \
                .format(constants.BASE_URL, month, day, year, month + day, game_id)
            pitchers = get_pitchers_from_url(url)
            if pitchers:
                for (pitcher_id, pitcher_name) in pitchers:
                    url = "{}?month={}&day={}&year={}&game={}&pitchSel={}&prevDate={}&prevGame={}&league=mlb" \
                        .format(constants.BASE_URL, month, day, year, game_id, pitcher_id, month+day, game_id)

                    expanded_table_url = "{}?pitchSel={}&game={}&s_type=&h_size=500&v_size=700" \
                        .format(constants.EXPANDED_TABLE_BASE_URL, pitcher_id, game_id)

                    url_table_rows.append((url, expanded_table_url, pitcher_id,
                                           pitcher_name, game_id, day, month, year))

    return url_table_rows


def get_game_ids_from_url(url):
    games = []
    page = requests.get(url)

    if page.status_code != 200:
        logging.info("ERROR in get_game_ids_from_url! Status code is ", page.status_code)
    else:
        soup = BeautifulSoup(page.content, 'html.parser')
        select_tag = soup.find("select", {"name": "game"})
        if select_tag:
            for option in soup.find("select", {"name": "game"}).findAll("option"):
                games.append(option["value"])

    return games


def get_pitchers_from_url(url):
    pitchers = []
    page = requests.get(url)

    if page.status_code != 200:
        logging.info("ERROR in get_pitcher_ids_from_url! Status code is ", page.status_code)
    else:
        soup = BeautifulSoup(page.content, 'html.parser')
        select_tag = soup.find("select", {"name": "pitchSel"})
        if select_tag:
            for option in select_tag.findAll("option"):
                pitcher_id = option["value"]
                pitcher_name = option.contents[0].split("-")[0].strip()
                pitchers.append((pitcher_id, pitcher_name))

    return pitchers


def get_full_pitches_from_url(params):
    (url, pitcher_id, game_id) = params
    url_lefty = url + "&sp_type=2"
    url_righty = url + "&sp_type=3"
    url_types = [(url_lefty, "LHB"), (url_righty, "RHB")]
    pitch_stats = []

    for url, type_of_batters in url_types:
        page = requests.get(url)
        if page.status_code != 200:
            logging.info("ERROR in get_full_pitches_from_url! Status code is ", page.status_code)
        else:
            soup = BeautifulSoup(page.content, 'html.parser')
            table = soup.find("table")
            if table:
                table_rows = table.find_all("tr")

                type_of_data = table_rows[0].find_all("td")[0].text
                if "Automatic MLBAM Gameday Algorithm" in type_of_data:
                    type_of_data = "mlb_am"
                elif "PITCH INFO" in type_of_data:
                    type_of_data = "pitch_info"
                else:
                    type_of_data = "NA"

                # iterate from the 3rd <tr> to the 2nd to last <tr>
                for i in range(2, len(table_rows)-1):
                    td_list = table_rows[i].find_all("td")
                    pitch_type_code = td_list[0].text.split("(")[0].strip()
                    pitch_type_desc = td_list[0].text.split("(")[1].replace(")", "")
                    velo = td_list[1].text.split("(")[0].strip()
                    h_break = td_list[2].text
                    v_break = td_list[3].text
                    count = td_list[4].text
                    strikes = td_list[5].text.split("/")[0].strip()
                    swings = td_list[6].text.split("/")[0].strip()
                    whiffs = td_list[7].text.split("/")[0].strip()
                    bib = td_list[8].text.split("(")[0].strip()
                    snip = td_list[9].text.split("/")[0].strip()
                    lwts = td_list[10].text

                    pitch_stats.append((pitcher_id, game_id, type_of_batters, type_of_data, pitch_type_code,
                                        pitch_type_desc, velo, h_break, v_break, count, strikes, swings,
                                        whiffs, bib, snip, lwts))

    return pitch_stats
