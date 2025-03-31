import streamlit as st
import pandas as pd
import plotly.express as px

# Function to load and display data (same as before)
def display_data(csv_file, title):
    try:
        df = pd.read_csv(csv_file)
        st.subheader(title)
        st.dataframe(df)
        return df
    except FileNotFoundError:
        st.error(f"File not found: {csv_file}")
        return None

# Function to display plots (modified for wider layout)
def display_plot(plot_title, plot_function, col):
    with col:
        st.subheader(plot_title)
        st.plotly_chart(plot_function(), use_container_width=True)

# Main Streamlit App
def main():
    st.set_page_config(layout="wide") # Set the layout to wide

    st.title("Yelp User Dataset Analysis")

    # Define Plot Functions (same as before)
    def plot_avg_stars_distribution():
        avg_stars = pd.read_csv("filtered_datasets/user_dataset/avg_stars_distribution.csv")
        fig = px.histogram(avg_stars, x="avgStars", title="Distribution of Average Stars (Max 5 Stars)", nbins=20)
        fig.update_xaxes(range=[1, 5])
        return fig

    def plot_average_compliments():
        compliment_avgs = pd.read_csv("filtered_datasets/user_dataset/average_compliments.csv")
        fig = px.bar(compliment_avgs, x="Compliment Type", y="Average Count", title="Average Compliments per User")
        fig.update_xaxes(tickangle=45)
        return fig

    def plot_yelp_users_joining_over_time():
        yelping_years = pd.read_csv("filtered_datasets/user_dataset/yelp_users_joining_over_time.csv")
        fig = px.line(yelping_years, x="yelpingYear", y="count", title="Users Joining Yelp Over Time")
        fig.update_xaxes(range=[2004, 2023])
        return fig

    def plot_yearly_user_growth():
        pandas_df = pd.read_csv("filtered_datasets/user_dataset/yearly_user_growth.csv")
        fig = px.line(pandas_df, x="yelpingYear", y="growth", title="Overall Year-on-Year User Growth")
        fig.update_yaxes(ticksuffix="%")
        return fig

    def plot_yearly_user_growth_after_2008():
        pandas_df = pd.read_csv("filtered_datasets/user_dataset/yearly_user_growth_after_2008.csv")
        fig = px.line(pandas_df, x="yelpingYear", y="growth", title="Year-on-Year User Growth After 2008")
        fig.update_yaxes(ticksuffix="%")
        return fig

    def plot_no_review_vs_user_growth():
        merged_df = pd.read_csv("filtered_datasets/user_dataset/no_review_vs_user_growth.csv")
        fig = px.line(merged_df, x="yelpingYear", y=["proportion", "growth"], title="Correlation Between User Who Writes No Review to User Growth", labels={"value": "Percentage", "yelpingYear": "Year"})
        fig.update_yaxes(ticksuffix="%")
        return fig

    def plot_yearly_user_review_growth_comparison():
        merged_df = pd.read_csv("filtered_datasets/user_dataset/yearly_user_review_growth_comparison.csv")
        fig = px.line(merged_df, x="yelpingYear", y=["review_growth", "user_growth"], title="Yearly User and Review Growth Comparison", labels={"value": "Percentage Growth", "yelpingYear": "Year"})
        fig.update_yaxes(ticksuffix="%")
        return fig

    def plot_kmeans_clustering():
        try:
            pandas_df = pd.read_csv("filtered_datasets/user_dataset/kmeans_predictions.csv")
            centers_df = pd.read_csv("filtered_datasets/user_dataset/kmeans_cluster_centers.csv")

            feature_cols = ["reviewCount", "avgStars", "friendCount", "fans"]

            fig = px.scatter(
                pandas_df,
                x="reviewCount",
                y="avgStars",
                color="prediction",
                title="K-Means Clustering of Yelp Users",
                hover_data=feature_cols,
            )

            fig.add_scatter(
                x=centers_df["reviewCount"],
                y=centers_df["avgStars"],
                mode="markers",
                marker=dict(size=10, symbol="star", color="black"),
                name="Cluster Centers",
            )

            return fig

        except FileNotFoundError:
            st.error("Data or centers file not found.")
            return None

    def plot_correlation_heatmap():
        try:
            columns_df = pd.read_csv("filtered_datasets/user_dataset/correlation_columns.csv")
            columns = columns_df["columns"].tolist()
            data_df = pd.read_csv("filtered_datasets/user_dataset/correlation_matrix_data.csv")
            selected_data = data_df[columns]
            correlation_matrix = selected_data.corr()

            fig = px.imshow(correlation_matrix, title="Correlation Matrix of Yelp User Data", text_auto=".2f", color_continuous_scale="RdBu")
            return fig

        except FileNotFoundError:
            st.error("Data or columns file not found.")
            return None

    # Grid Layout for Plots
    col1, col2 = st.columns(2)
    display_plot("Average Stars Distribution Plot", plot_avg_stars_distribution, col1)
    display_plot("Average Compliments Plot", plot_average_compliments, col2)

    col3, col4 = st.columns(2)
    display_plot("Users Joining Yelp Over Time Plot", plot_yelp_users_joining_over_time, col3)
    display_plot("Yearly User Growth Plot", plot_yearly_user_growth, col4)

    col5, col6 = st.columns(2)
    display_plot("Yearly User Growth After 2008 Plot", plot_yearly_user_growth_after_2008, col5)
    display_plot("No Review vs. User Growth Plot", plot_no_review_vs_user_growth, col6)

    col7, col8 = st.columns(2)
    display_plot("Yearly User and Review Growth Comparison Plot", plot_yearly_user_review_growth_comparison, col7)
    display_plot("K-Means Clustering Plot", plot_kmeans_clustering, col8)

    col9, _ = st.columns(2) # _ is used because we do not need the second column in this row.
    display_plot("Correlation Matrix of Yelp User Data", plot_correlation_heatmap, col9)

if __name__ == "__main__":
    main()