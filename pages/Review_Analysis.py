import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import plotly.tools as tls
import plotly.graph_objects as go

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

    st.title("Yelp Review Dataset Analysis")


    def plot_average_star_distribution():
        star_pd = pd.read_csv("filtered_datasets/review_dataset/star_distribution.csv")
        star_pd['stars'] = pd.to_numeric(star_pd['stars'])
        star_pd['review_count'] = pd.to_numeric(star_pd['review_count'])

        fig = go.Figure(data=[go.Bar(x=star_pd['stars'], y=star_pd['review_count'])])
        fig.update_layout(
            title='Average Star Distribution',
            xaxis_title='Star Rating',
            yaxis_title='Number of Reviews'
        )
        return fig
    
    def plot_average_useful_funny_cool():
        # Load the data (assuming you have 'avg_useful_funny_cool.csv' in the correct location)
        averages_by_star = pd.read_csv("filtered_datasets/review_dataset/avg_useful_funny_cool.csv")

        # Melt the dataframe
        averages_melted = averages_by_star.melt(id_vars='stars', value_vars=['avg_useful', 'avg_funny', 'avg_cool'],
                                                var_name='Category', value_name='Average')

        # Plotting with Plotly
        fig = go.Figure()

        for category in averages_melted['Category'].unique():
            category_data = averages_melted[averages_melted['Category'] == category]
            fig.add_trace(go.Bar(
                x=category_data['stars'],
                y=category_data['Average'],
                name=category
            ))

        fig.update_layout(
            title='Average Useful, Funny, Cool per Star Rating',
            xaxis_title='Star Rating',
            yaxis_title='Average Rating',
            barmode='group' # Group the bars
        )

        return fig
    

    def plot_average_review_length():
        avg_review_length_pd = pd.read_csv("filtered_datasets/review_dataset/avg_review_length.csv")

        fig = go.Figure(data=[
            go.Bar(x=avg_review_length_pd['stars'], y=avg_review_length_pd['avg_review_length'])
        ])

        fig.update_layout(
            title='Average Review Length per Star Rating',
            xaxis_title='Star Rating',
            yaxis_title='Average Review Length',
            template="plotly_dark" # Optional, for a dark theme similiar to magma.
        )

        return fig
    
    def plot_review_length_distribution():
        # Load the data from the CSV file
        review_length_pd = pd.read_csv("filtered_datasets/review_dataset/review_length_stats.csv")

        # Reconstruct original review length samples
        reconstructed_lengths = review_length_pd.loc[review_length_pd.index.repeat(review_length_pd['count'])]['review_length']

        # Sample the data (e.g., 10% sample)
        if len(reconstructed_lengths) > 10000: #Only sample if there are many entries.
            sampled_lengths = reconstructed_lengths.sample(frac=0.1)
        else:
            sampled_lengths = reconstructed_lengths

        # Plotting with Plotly Express
        fig = px.histogram(
            sampled_lengths,
            x="review_length",
            nbins=30,
            title="Review Length Distribution",
            labels={"review_length": "Review Length", "count": "Frequency"},
            color_discrete_sequence=["blue"],
            marginal="rug"
        )

        fig.update_layout(
            bargap=0.1
        )

        return fig

    
    def plot_correlation_matrix():
        # Load the correlation matrix from CSV
        corr_matrix = pd.read_csv("filtered_datasets/review_dataset/correlation_matrix.csv", index_col=0) # Index col is set to 0, because the first column is the index.

        # Plot the correlation matrix with Plotly
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.index,
            colorscale='RdBu',  # Similar to coolwarm
            zmin=-1,  # Ensure the color scale covers the full range of correlations
            zmax=1,
        ))

        fig.update_layout(
            title='Correlation Matrix',
            xaxis_nticks=len(corr_matrix.columns),
            yaxis_nticks=len(corr_matrix.index)
        )

        return fig
    
    def plot_top_businesses():
        # Load the data from the CSV file
        top_businesses_pd = pd.read_csv("filtered_datasets/review_dataset/top_businesses.csv")

        # Ensure sorting (redundant if already sorted in CSV, but good practice)
        top_businesses_pd = top_businesses_pd.sort_values(by="count", ascending=False)

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=top_businesses_pd['name'],
                y=top_businesses_pd['count'],
                marker_color='blue' # Set bar color to blue
            )
        ])

        fig.update_layout(
            title='Top 5 Most Reviewed Businesses',
            xaxis_title='Business Name',
            yaxis_title='Review Count',
            xaxis_tickangle=-45 # Rotate x-axis labels by -45 degrees
        )

        return fig
    
    def plot_top_users():
        # Load the data from the CSV file
        top_users_pd = pd.read_csv("filtered_datasets/review_dataset/top_users.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=top_users_pd['name'],
                y=top_users_pd['count'],
                marker_color='purple'  # Set bar color to purple
            )
        ])

        fig.update_layout(
            title='Top 5 Most Active Users',
            xaxis_title='User Name',
            yaxis_title='Review Count',
            xaxis_tickangle=-45  # Rotate x-axis labels by -45 degrees
        )

        return fig
    
    def plot_sentiment_distribution():
        # Load the data from the CSV file
        sentiment_counts_pd = pd.read_csv("filtered_datasets/review_dataset/sentiment_counts.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Pie(
                labels=sentiment_counts_pd['sentiment'],
                values=sentiment_counts_pd['count'],
                marker_colors=['green', 'blue', 'red']  # Set colors
            )
        ])

        fig.update_layout(
            title='Sentiment Distribution'
        )

        return fig
    # Grid Layout for Plots
    col1, col2 = st.columns(2)
    display_plot("Average Stars Distribution Plot", plot_average_star_distribution, col1)
    display_plot("Average Useful Funny Cool per Star Rating", plot_average_useful_funny_cool, col2)

    col3, col4 = st.columns(2)
    display_plot("Average Review Length", plot_average_review_length, col3)
    display_plot("Correlation Matrix", plot_correlation_matrix, col4)

    col5, col6 = st.columns(2)
    display_plot("Top 5 Most Reviewed Businesses", plot_top_businesses, col5)
    display_plot("Review Length Distribution", plot_review_length_distribution, col6)

    col7, col8 = st.columns(2)
    display_plot("Top 5 Most Active Users", plot_top_users, col7)
    display_plot("Review Sentiment Distribution", plot_sentiment_distribution, col8)

if __name__ == "__main__":
    main()