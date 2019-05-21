import logging
import constants
import scraper
import db_utils

logger = logging.getLogger(constants.LOGGER_NAME)


def main():
    logging.info("starting driver...")

    # 1. Web Scraping
    # scraper.generate_urls_table()
    # scraper.generate_full_pitches_table()

    # 2. Data Manipulation
    # db_utils.create_combined_db()
    # db_utils.add_mlb_gameday_data_to_db()


if __name__ == '__main__':
    from sys import stdout

    file_handler = logging.FileHandler(filename="../logs/" + constants.LOGGER_NAME)
    stdout_handler = logging.StreamHandler(stdout)
    handlers = [file_handler, stdout_handler]

    logging.basicConfig(format="[%(asctime)s,%(msecs)d] [%(levelname)-6s] --- %(message)s (%(filename)s:%(lineno)s)",
                        datefmt="%H:%M:%S",
                        handlers=handlers,
                        level=logging.INFO)

    main()
