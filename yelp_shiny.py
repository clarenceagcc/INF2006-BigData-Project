from shiny import App, ui, render, reactive
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors

def load_data(csv_file):
    try:
        df = pd.read_csv(csv_file)
        return df
    except FileNotFoundError:
        return None

def plot_avg_stars_distribution():
    avg_stars = load_data("filtered_datasets/user_dataset/avg_stars_distribution.csv")
    if avg_stars is None:
        return None
    fig, ax = plt.subplots()
    ax.hist(avg_stars["avgStars"], bins=20, range=[1, 5])
    ax.set_title("Distribution of Average Stars (Max 5 Stars)")
    ax.set_xlabel("Average Stars")
    ax.set_ylabel("Count")
    return fig

def plot_average_compliments():
    compliment_avgs = load_data("filtered_datasets/user_dataset/average_compliments.csv")
    if compliment_avgs is None:
        return None
    fig, ax = plt.subplots()
    ax.bar(compliment_avgs["Compliment Type"], compliment_avgs["Average Count"])
    ax.set_title("Average Compliments per User")
    ax.set_xlabel("Compliment Type", rotation=45, ha="right")
    ax.set_ylabel("Average Count")
    plt.tight_layout()
    return fig

def plot_yelp_users_joining_over_time():
    yelping_years = load_data("filtered_datasets/user_dataset/yelp_users_joining_over_time.csv")
    if yelping_years is None:
        return None
    fig, ax = plt.subplots()
    ax.plot(yelping_years["yelpingYear"], yelping_years["count"])
    ax.set_title("Users Joining Yelp Over Time")
    ax.set_xlabel("Year")
    ax.set_ylabel("Count")
    ax.set_xlim(2004, 2023)
    return fig

def plot_yearly_user_growth():
    pandas_df = load_data("filtered_datasets/user_dataset/yearly_user_growth.csv")
    if pandas_df is None:
        return None
    fig, ax = plt.subplots()
    ax.plot(pandas_df["yelpingYear"], pandas_df["growth"])
    ax.set_title("Overall Year-on-Year User Growth")
    ax.set_xlabel("Year")
    ax.set_ylabel("Growth (%)")
    ax.yaxis.set_major_formatter(lambda y, _: f'{y:.0%}')
    return fig

def plot_yearly_user_growth_after_2008():
    pandas_df = load_data("filtered_datasets/user_dataset/yearly_user_growth_after_2008.csv")
    if pandas_df is None:
        return None
    fig, ax = plt.subplots()
    ax.plot(pandas_df["yelpingYear"], pandas_df["growth"])
    ax.set_title("Year-on-Year User Growth After 2008")
    ax.set_xlabel("Year")
    ax.set_ylabel("Growth (%)")
    ax.yaxis.set_major_formatter(lambda y, _: f'{y:.0%}')
    return fig

def plot_no_review_vs_user_growth():
    merged_df = load_data("filtered_datasets/user_dataset/no_review_vs_user_growth.csv")
    if merged_df is None:
        return None
    fig, ax = plt.subplots()
    ax.plot(merged_df["yelpingYear"], merged_df["proportion"], label="Proportion")
    ax.plot(merged_df["yelpingYear"], merged_df["growth"], label="Growth")
    ax.set_title("Correlation Between User Who Writes No Review to User Growth")
    ax.set_xlabel("Year")
    ax.set_ylabel("Percentage")
    ax.yaxis.set_major_formatter(lambda y, _: f'{y:.0%}')
    ax.legend()
    return fig

def plot_yearly_user_review_growth_comparison():
    merged_df = load_data("filtered_datasets/user_dataset/yearly_user_review_growth_comparison.csv")
    if merged_df is None:
        return None
    fig, ax = plt.subplots()
    ax.plot(merged_df["yelpingYear"], merged_df["review_growth"], label="Review Growth")
    ax.plot(merged_df["yelpingYear"], merged_df["user_growth"], label="User Growth")
    ax.set_title("Yearly User and Review Growth Comparison")
    ax.set_xlabel("Year")
    ax.set_ylabel("Percentage Growth")
    ax.yaxis.set_major_formatter(lambda y, _: f'{y:.0%}')
    ax.legend()
    return fig

def plot_kmeans_clustering():
    try:
        pandas_df = load_data("filtered_datasets/user_dataset/kmeans_predictions.csv")
        centers_df = load_data("filtered_datasets/user_dataset/kmeans_cluster_centers.csv")
        if pandas_df is None or centers_df is None:
            return None
        fig, ax = plt.subplots()
        scatter = ax.scatter(pandas_df["reviewCount"], pandas_df["avgStars"], c=pandas_df["prediction"], cmap='viridis')
        ax.scatter(centers_df["reviewCount"], centers_df["avgStars"], marker='*', s=200, color='red', label='Cluster Centers')
        ax.set_title("K-Means Clustering of Yelp Users")
        ax.set_xlabel("Review Count")
        ax.set_ylabel("Average Stars")
        ax.legend()
        return fig
    except FileNotFoundError:
        return None

def plot_correlation_heatmap():
    try:
        columns_df = load_data("filtered_datasets/user_dataset/correlation_columns.csv")
        data_df = load_data("filtered_datasets/user_dataset/correlation_matrix_data.csv")
        if columns_df is None or data_df is None:
            return None
        columns = columns_df["columns"].tolist()
        selected_data = data_df[columns]
        correlation_matrix = selected_data.corr()
        fig, ax = plt.subplots()
        cax = ax.matshow(correlation_matrix, cmap='RdBu')
        fig.colorbar(cax)
        ax.set_xticks(range(len(correlation_matrix.columns)))
        ax.set_yticks(range(len(correlation_matrix.columns)))
        ax.set_xticklabels(correlation_matrix.columns, rotation=90)
        ax.set_yticklabels(correlation_matrix.columns)
        ax.set_title("Correlation Matrix of Yelp User Data")
        return fig
    except FileNotFoundError:
        return None

app_ui = ui.page_fluid(
    ui.h2("Yelp User Dataset Analysis"),
    ui.layout_columns(
        ui.output_plot("avg_stars"),
        ui.output_plot("avg_compliments"),
    ),
    ui.layout_columns(
        ui.output_plot("yelp_users_time"),
        ui.output_plot("yearly_growth"),
    ),
    ui.layout_columns(
        ui.output_plot("yearly_growth_after_2008"),
        ui.output_plot("no_review_growth"),
    ),
    ui.layout_columns(
        ui.output_plot("user_review_growth"),
        ui.output_plot("kmeans_cluster"),
    ),
    ui.layout_columns(
        ui.output_plot("correlation_matrix"),
    ),
)

def server(input, output, session):
    @output
    @render.plot
    def avg_stars():
        return plot_avg_stars_distribution()

    @output
    @render.plot
    def avg_compliments():
        return plot_average_compliments()

    @output
    @render.plot
    def yelp_users_time():
        return plot_yelp_users_joining_over_time()

    @output
    @render.plot
    def yearly_growth():
        return plot_yearly_user_growth()

    @output
    @render.plot
    def yearly_growth_after_2008():
        return plot_yearly_user_growth_after_2008()

    @output
    @render.plot
    def no_review_growth():
        return plot_no_review_vs_user_growth()

    @output
    @render.plot
    def user_review_growth():
        return plot_yearly_user_review_growth_comparison()

    @output
    @render.plot
    def kmeans_cluster():
        return plot_kmeans_clustering()

    @output
    @render.plot
    def correlation_matrix():
        return plot_correlation_heatmap()

app = App(app_ui, server)