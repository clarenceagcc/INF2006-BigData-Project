import streamlit as st
import pandas as pd
import plotly.express as px
import pandas as pd
import plotly.tools as tls
import plotly.graph_objects as go
from scipy.stats import gaussian_kde
import numpy as np

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

    st.title("Yelp Business, Checkin and Tip Joined Dataset Analysis")


    def plot_average_ratings_by_state():
        # Load the data from the CSV file
        avg_ratings_by_state_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/avg_ratings_by_state.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=avg_ratings_by_state_pd['state'],
                y=avg_ratings_by_state_pd['stars']
            )
        ])

        fig.update_layout(
            title='Average Business Rating by State',
            xaxis_title='State',
            yaxis_title='Average Stars',
            xaxis_tickangle=-45,
        )

        return fig
    
    def plot_top_10_reviewed_businesses():
        # Load the data from the CSV file
        top_reviewed_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/top_rated_businesses.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                y=top_reviewed_pd['name'],  # Note: y and x are swapped for horizontal bar
                x=top_reviewed_pd['review_count'],
                orientation='h' # Make plot horizontal.
            )
        ])

        fig.update_layout(
            title='Top 10 Most Rated Businesses',
            xaxis_title='Review Count',
            yaxis_title='Business Name',
            yaxis=dict(autorange="reversed") # Invert y-axis
        )

        return fig
    def plot_distribution_of_business_ratings():
        # Load the raw data from the CSV file
        business_ratings_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/distribution_of_ratings_non.csv")  # Replace "your_raw_data.csv" with the actual filename.

        # Calculate KDE
        kde = gaussian_kde(business_ratings_pd['stars'])
        x_vals = np.linspace(business_ratings_pd['stars'].min(), business_ratings_pd['stars'].max(), 200)
        y_vals = kde(x_vals)

        # Calculate histogram
        hist, bins = np.histogram(business_ratings_pd['stars'], bins=5)

        # Create plot
        fig = go.Figure()

        # Add histogram bars
        fig.add_trace(go.Bar(
            x=(bins[:-1] + bins[1:]) / 2,
            y=hist,
            name='Histogram',
            marker_color='lightblue'
        ))

        # Add KDE curve
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals * hist.max() * 0.9,
            name='KDE',
            line_color='steelblue'
        ))

        fig.update_layout(
            title='Distribution of Business Ratings',
            xaxis_title='Stars',
            yaxis_title='Count',
            bargap=0.1
        )

        return fig
    
    def plot_tips_over_years():
        # Load the data from the CSV file
        tip_trend_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/tips_per_year.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Scatter(
                x=tip_trend_pd['year'],
                y=tip_trend_pd['text'],
                mode='lines+markers', # Show both lines and markers
                marker=dict(symbol='circle')
            )
        ])

        fig.update_layout(
            title='Tips Over Years',
            xaxis_title='Year',
            yaxis_title='Number of Tips',
        )

        return fig
    
    def plot_top_10_business_categories():
        # Load the data from the CSV file
        top_categories_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/top_categories_by_count.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=top_categories_pd['categories'], # use category column
                y=top_categories_pd['count'], # use count column
                marker_color='skyblue'
            )
        ])

        fig.update_layout(
            title='Top 10 Business Categories',
            xaxis_title='Category',
            yaxis_title='Number of Businesses',
            xaxis_tickangle=-45
        )

        return fig
    
    def plot_numeric_metrics_correlation():
        # Load the data from the CSV file
        corr_matrix_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/correlation_metrics.csv", index_col=0)

        # Reverse the order of the y-axis labels
        y_labels = corr_matrix_pd.index[::-1]  # Reverse the index

        # Plotting with Plotly
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix_pd.values[::-1],  # Reverse the z-values as well
            x=corr_matrix_pd.columns,
            y=y_labels,  # Use the reversed y-labels
            colorscale='RdBu',
            zmin=-1,
            zmax=1,
            #annotation_text=corr_matrix_pd.round(2).values[::-1], #reverse the annotations also.
            hoverongaps=False
        ))

        fig.update_layout(
            title='Correlation Between Numeric Metrics',
            xaxis_nticks=len(corr_matrix_pd.columns),
            yaxis_nticks=len(y_labels)  # Use the reversed y-labels count
        )

        return fig
    
    def plot_checkins_by_weekday():
        # Load the data from the CSV file
        weekday_counts_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/checkins_by_weekday.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=weekday_counts_pd['date'], #Use day column
                y=weekday_counts_pd['count'] #use checkins column
            )
        ])

        fig.update_layout(
            title='Check-ins by Day of the Week',
            xaxis_title='Day',
            yaxis_title='Number of Check-ins',
            xaxis_tickangle=-45
        )

        return fig
    
    
    def plot_top_10_tip_length_by_state():
        # Load the data from the CSV file
        tip_length_by_state_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/avg_tip_length_by_state.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=tip_length_by_state_pd['state'],
                y=tip_length_by_state_pd['tip_length'], #Use average_tip_length column
                marker_color='mediumslateblue'
            )
        ])

        fig.update_layout(
            title='Top 10 States by Average Tip Length',
            xaxis_title='State',
            yaxis_title='Average Tip Length (Characters)',
            xaxis_tickangle=-45
        )

        return fig
    
    def plot_average_stars_for_top_categories():
        # Load the data from the CSV file
        avg_stars_pd = pd.read_csv("filtered_datasets/buisness_checkin_tip_dataset/avg_stars_by_top_categories.csv")

        # Plotting with Plotly
        fig = go.Figure(data=[
            go.Bar(
                x=avg_stars_pd['category'], # Use category column
                y=avg_stars_pd['stars'], # Use average_stars column
                marker_color='coral'
            )
        ])

        fig.update_layout(
            title='Average Rating for Top 10 Business Categories',
            xaxis_title='Category',
            yaxis_title='Average Stars',
            xaxis_tickangle=-45
        )

        return fig


    # Grid Layout for Plots
    col1, col2 = st.columns(2)
    display_plot("Average Stars Distribution Plot", plot_average_ratings_by_state, col1)
    display_plot("Average Useful Funny Cool per Star Rating", plot_top_10_reviewed_businesses, col2)

    col3, col4 = st.columns(2)
    display_plot("Distribution of Business Ratings", plot_distribution_of_business_ratings, col3)
    display_plot("Correlation Matrix", plot_tips_over_years, col4)

    col5, col6 = st.columns(2)
    display_plot("Top 10 Buisness Categories", plot_top_10_business_categories, col5)
    display_plot("Correlation Matrix", plot_numeric_metrics_correlation, col6)

    col7, col8 = st.columns(2)
    display_plot("Top 5 Most Reviewed Businesses", plot_checkins_by_weekday, col7)

    col9, col10 = st.columns(2)
    display_plot("Top 5 Most Reviewed Businesses", plot_top_10_tip_length_by_state, col8)
    display_plot("Correlation Matrix", plot_average_stars_for_top_categories, col9)

if __name__ == "__main__":


    main()