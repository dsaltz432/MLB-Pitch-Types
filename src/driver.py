import logging
import constants
import scraper
import data_manipulator
import clustering

logger = logging.getLogger(constants.LOGGER_NAME)


def main():
    logging.info("starting driver...")

    # 1. Web Scraping
    # scraper.generate_urls_table()
    # scraper.generate_full_pitches_table()

    # 2. Data Manipulation
    # data_manipulator.generate_weighted_totals_table()
    # data_manipulator.generate_pitch_frequencies_table()
    # data_manipulator.create_normalized_table()

    # 3. Clustering
    p_throws = "R"
    type_of_batters = "RHB"
    k = 4
    df = clustering.get_normalized_df_for_matchup(p_throws, type_of_batters)
    # clustering.plot_analysis_finding_k(df, p_throws, type_of_batters)
    clustered = clustering.run_kmeans(df, k)
    clustering.plot_clusters_heatmap(clustered, k, p_throws, type_of_batters)
    # clustering.analyze_variance(clustered, p_throws, type_of_batters)
    # clustering.analyze_correlation(clustered, p_throws, type_of_batters)


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
