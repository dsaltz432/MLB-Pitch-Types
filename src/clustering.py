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
from kneed import KneeLocator
from sklearn.decomposition import PCA
from sklearn.decomposition import PCA as sklearnPCA
import numpy as np


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
    plt.figure(figsize=(14, 8))
    fig = sns.heatmap(grouped, annot=True, fmt=".2f", cmap='coolwarm')

    y_values = []
    for i in range(0, k):
        y_values.append("Cluster {}".format(i))
    fig.set_yticklabels(y_values)

    for tick in fig.get_xticklabels():
        tick.set_rotation(35)
        tick.set_fontsize(10)

    plt.title("Cluster Centers for {} vs. {}".format(p_throws, type_of_batters))
    plt.show()


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
    plt.figure(figsize=(12, 7))
    fig = sns.heatmap(df.corr(), annot=True, fmt=".2f")
    for tick in fig.get_xticklabels():
        tick.set_rotation(35)
        tick.set_fontsize(10)
    plt.title("Correlation Matrix for {} vs. {}".format(p_throws, type_of_batters))
    plt.show()


def analyze_pca(clustered, p_throws, type_of_batters):

    # X = iris.data
    y = clustered['cluster']

    # Drop cluster field
    X = clustered.drop(["cluster"], axis=1)

    # pca = PCA(0.90)
    # pca.fit(X)

    # transformed = pca.transform(X)
    # print("original shape:   ", X.shape)
    # print("transformed shape:", transformed.shape)
    #
    # print("components: ")
    # print(pca.components_)
    # print("Explained variance: ")
    # print(pca.explained_variance_)
    #
    # plt.plot(np.cumsum(pca.explained_variance_ratio_))
    # plt.xlabel('number of components')
    # plt.ylabel('cumulative explained variance')
    # plt.show()

    pca = PCA()
    pca.fit(X, y)
    x_new = pca.transform(X)

    plt.xlim(-1, 1)
    plt.ylim(-1, 1)
    plt.xlabel("PC{}".format(1))
    plt.ylabel("PC{}".format(2))
    plt.grid()

    score = x_new[:, 0:2]
    coeff = np.transpose(pca.components_[0:2, :])
    labels = None

    xs = score[:, 0]
    ys = score[:, 1]
    scalex = 1.0/(xs.max() - xs.min())
    scaley = 1.0/(ys.max() - ys.min())
    plt.scatter(xs * scalex, ys * scaley, c=y)
    for i in range(coeff.shape[0]):
        plt.arrow(0, 0, coeff[i, 0], coeff[i, 1], color='r', alpha=0.5)
        if labels is None:
            plt.text(coeff[i, 0] * 1.15, coeff[i, 1] * 1.15, "Var"+str(i+1), color='g', ha='center', va='center')
        else:
            plt.text(coeff[i, 0] * 1.15, coeff[i, 1] * 1.15, labels[i], color='g', ha='center', va='center')

    plt.show()


