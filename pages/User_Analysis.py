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
def display_plot(plot_title, plot_function, col, description=None):
    with col:
        st.subheader(plot_title)
        st.plotly_chart(plot_function(), use_container_width=True)
    if description:  # Only display description if it's provided
        col.write(description) # or col.markdown(f"<small>{description}</small>", unsafe_allow_html=True) for smaller text

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
    display_plot("Average Stars Distribution Plot", plot_avg_stars_distribution, col1, "This histogram reveals the overall tendency of users to assign certain average star ratings. We can observe the concentration of users at different average rating levels, which may indicate biases or general user satisfaction trends within the platform. The distribution ranges from 1 to 5 stars, as expected.")
    display_plot("Average Compliments Plot", plot_average_compliments, col2, "This chart allows us to compare the average usage of different compliment types among users. By examining the average compliment count per type, we can gain insights into user interaction and feedback patterns, revealing which aspects of other users' contributions (e.g., reviews) are most commonly acknowledged or appreciated.")

    col3, col4 = st.columns(2)
    display_plot("Users Joining Yelp Over Time Plot", plot_yelp_users_joining_over_time, col3, "This chart reveals the historical pattern of user growth on Yelp. We can observe periods of rapid user acquisition, plateaus, and potential declines in new user registrations. The shape of the curve provides insights into Yelp's adoption rate and user base expansion over the years. The y-axis represents the number or percentage of users, and the x-axis shows the years.")
    display_plot("Yearly User Growth Plot", plot_yearly_user_growth, col4, "This chart provides a perspective on the rate of user growth on Yelp. By observing the percentage change in users year-over-year, we can identify periods of rapid acceleration or deceleration in user acquisition. Declining growth rates may indicate market saturation or changing user engagement trends. The y-axis is user growth percentage, and the x-axis is the year.")

    col5, col6 = st.columns(2)
    display_plot("Yearly User Growth After 2008 Plot", plot_yearly_user_growth_after_2008, col5, "The Yearly User Growth After 2008 Plot displays the percentage change in Yelp's user base from one year to the next, starting from 2008. The x-axis represents the years (2008 onwards), and the y-axis shows the corresponding percentage growth or decline in the user count. This visualization focuses on the growth pattern of Yelp's user base in its later years.")
    display_plot("No Review vs. User Growth Plot", plot_no_review_vs_user_growth, col6, "The No Review vs. User Growth Plot presents a comparison between the percentage of users who do not write reviews and the overall user growth pattern of Yelp over the years. The x-axis represents the years, and the y-axis shows the percentage. The plot includes separate lines representing the percentage of users who don't write reviews and the overall user growth percentage, allowing for a visual assessment of their relationship")

    col7, col8 = st.columns(2)
    display_plot("Yearly User and Review Growth Comparison Plot", plot_yearly_user_review_growth_comparison, col7, "This chart investigates the relationship between user growth and content generation on Yelp. By comparing the year-over-year growth rates of users and reviews, we can gain insights into whether user acquisition is keeping pace with content creation. Discrepancies between the two growth rates may indicate changes in user engagement or platform activity. The y-axis represents the percentage growth, and the x-axis represents the year.")
    display_plot("K-Means Clustering Plot", plot_kmeans_clustering, col8, "This chart visualizes the output of a K-Means clustering analysis performed on users. By plotting users based on their characteristics, such as review count and average stars, we can identify distinct user segments or groups. The color-coding of the points reveals the clusters identified by the algorithm, providing insights into user behavior patterns and commonalities. The plot helps to understand how users naturally group together based on their activity and rating tendencies.")

    col9, _ = st.columns(2) # _ is used because we do not need the second column in this row.
    display_plot("Correlation Matrix of Yelp User Data", plot_correlation_heatmap, col9, "This heatmap explores the relationships between various user characteristics on Yelp. By examining the correlation coefficients, we can identify which features tend to move together (positive correlation), move in opposite directions (negative correlation), or are independent of each other (no correlation). For example, we can observe the correlation between the number of reviews a user has written ('reviewCount') and their 'useful,' 'funny,' and 'cool' ratings. This analysis helps to understand user behavior patterns and the factors that influence user activity and reputation within the Yelp community.")

if __name__ == "__main__":
    main()