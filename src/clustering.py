import logging
import constants
import pandas
import db_utils
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
from sklearn.feature_selection import VarianceThreshold
import seaborn as sns
import numpy as np
from kneed import KneeLocator


logger = logging.getLogger(constants.LOGGER_NAME)


def get_normalized_df_for_matchup(p_throws, type_of_batters):
    conn = db_utils.create_connection()
    df = pandas.read_sql_query("select * from normalized where p_throws='{}' and type_of_batters='{}';"
                               .format(p_throws, type_of_batters), conn)
    conn.close()
    return df


def plot_analysis_finding_k(df, p_throws, type_of_batters):
    if "pitcher_id" in df.columns:
        df = df.drop(["pitcher_id", "p_throws", "type_of_batters", "index"], axis=1)
    sum_of_squared_distances = []
    k_range = range(1, 15)
    for k in k_range:
        km = KMeans(n_clusters=k)
        km = km.fit(df)
        sum_of_squared_distances.append(km.inertia_)

    plt.plot(k_range, sum_of_squared_distances, 'bx-')
    plt.xlabel("k")
    plt.ylabel("Sum of Squared Distances")
    plt.title("Finding k for {} vs. {}".format(p_throws, type_of_batters))

    # Add line for when decreasing slows down
    y = sum_of_squared_distances
    x = range(1, len(y) + 1)
    kn = KneeLocator(x, y, curve='convex', direction='decreasing')
    plt.vlines(kn.knee, plt.ylim()[0], plt.ylim()[1], linestyles='dashed')
    print("\nSum of Squared Distances for kn.knee={}: {}".format(kn.knee, sum_of_squared_distances[kn.knee]))
    plt.show()


def run_kmeans(df, k):

    # Remove unimportant fields
    df = df.drop(["pitcher_id", "p_throws", "type_of_batters", "index"], axis=1)

    # Normalize table
    df = (df - df.min()) / (df.max() - df.min())

    # Run kmeans
    kmeans = KMeans(n_clusters=k, random_state=0).fit(df)

    # Add cluster as a column
    df['cluster'] = kmeans.labels_

    return df


def plot_clusters_heatmap(clustered, k, p_throws, type_of_batters):
    grouped = clustered.groupby(['cluster'], sort=True).mean().round(3)

    y_values = []
    for i in range(0, k):
        y_values.append("Cluster {}".format(i))

    data = [go.Heatmap(z=grouped.values.tolist(), y=y_values, x=grouped.columns, colorscale='Viridis')]
    layout = go.Layout(title="Average Clusters for {} vs. {}".format(p_throws, type_of_batters))
    fig = go.Figure(data=data, layout=layout)

    file_name = "../output/heatmap-{}_vs_{}.html".format(p_throws, type_of_batters)
    plotly.offline.plot(fig, filename=file_name)


def analyze_variance(clustered, p_throws, type_of_batters):
    # Drop cluster field
    df = clustered.drop(["cluster"], axis=1)

    # Apply variance threshold of .01
    print("\nLowest variance before filtering for {} vs. {}:".format(p_throws, type_of_batters))
    print(df.var().sort_values())
    threshold = .01
    threshold_obj = VarianceThreshold(threshold=threshold).fit(df)
    filtered_out_columns = [column for column in df.columns if column not in df.columns[threshold_obj.get_support()]]

    if filtered_out_columns:
        print("\nFiltering out columns for variance lower than {}: {}".format(threshold, filtered_out_columns))
        df = df[df.columns[threshold_obj.get_support(indices=True)]]
        print("\nLowest variance after filtering: ")
        print(df.var().sort_values())

        # Re-plot finding_k and sum of squares
        plot_analysis_finding_k(df, p_throws, type_of_batters)
    else:
        print("\nNo columns filtered based on low variance")


def analyze_correlation(clustered, p_throws, type_of_batters):
    # Drop cluster field
    df = clustered.drop(["cluster"], axis=1)

    # Plot heat map
    sns.set(rc={'figure.figsize': (12, 7)})
    fig = sns.heatmap(df.corr(), annot=True, fmt=".2f")
    for tick in fig.get_xticklabels():
        tick.set_rotation(35)
        tick.set_fontsize(10)
    plt.title("Correlation Matrix for {} vs. {}".format(p_throws, type_of_batters))
    plt.show()


def get_redundant_pairs(df):
    """Get diagonal and lower triangular pairs of correlation matrix"""
    pairs_to_drop = set()
    for i in range(0, df.shape[1]):
        for j in range(0, i+1):
            pairs_to_drop.add((df.columns[i], df.columns[j]))
    return pairs_to_drop



# pca = PCA(n_components=3)
# principalComponents = pca.fit_transform(df)
# principalDf = pandas.DataFrame(data = principalComponents, columns = ['principal component 1', 'principal component 2', 'principal component 3'])

