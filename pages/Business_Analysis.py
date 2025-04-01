import streamlit as st
import pandas as pd
import plotly.express as px
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
def display_plot(plot_title, plot_function, col, description=None):
    with col:
        st.subheader(plot_title)
        st.plotly_chart(plot_function(), use_container_width=True)
    if description:  # Only display description if it's provided
        col.write(description) # or col.markdown(f"<small>{description}</small>", unsafe_allow_html=True) for smaller text

def plot_top_categories():
    business_dist = pd.read_json('filtered_datasets/business_dataset/business_distribution.json', lines=True)
    top_categories = business_dist.groupby('categories')['count'].sum().sort_values(ascending=False).head(10).reset_index()
    fig = go.Figure(data=[go.Bar(x=top_categories['count'], y=top_categories['categories'], orientation='h')])  # Note x and y are swapped, orientation='h'
    fig.update_layout(
        title='Top 10 Business Categories by Count',
        xaxis_title='Number of Businesses',
        yaxis_title='Category'
    )
    return fig

def plot_review_vs_star():
    review_vs_star = pd.read_json('filtered_datasets/business_dataset/review_vs_star.json', lines=True)
    fig = px.scatter(
        review_vs_star,
        x='review_count',
        y='stars',
        log_x=True, # Use log scale in Plotly
        color_discrete_sequence=['darkblue'], # Set color
        opacity=0.7,
        title='Review Count vs. Star Rating',
        labels={'review_count': 'Review Count', 'stars': 'Stars'}
    )
    return fig
def plot_closure_heatmap():
    heatmap_data = pd.read_json('filtered_datasets/business_dataset/closure_heatmap_data.json', lines=True)
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale='Reds'
    ))
    fig.update_layout(
        title='Heatmap of Closure Rates by City and Category',
        xaxis_title='Category',
        yaxis_title='City'
    )
    return fig
def plot_high_rated_hotspots():
    hotspots = pd.read_json('filtered_datasets/business_dataset/high_rated_hotspots.json', lines=True)
    fig = px.scatter_geo(
        hotspots,
        lat='latitude',
        lon='longitude',
        color='stars',
        hover_name='name',
        scope='usa',
        title='Hotspots of High-Rated Businesses',
        color_continuous_scale=px.colors.sequential.Reds, # Use a sequential color scale
        size_max=30
    )
    fig.update_layout(
        title='Hotspots of High-Rated Businesses',
        width=1000,  # Make the plot even wider for better spacing
        height=1110,  # Increase height to accommodate the y-axis labels
        geo=dict(
            lakecolor='white',  # Set background color for the map
            projection_scale=1  # Adjust scale to zoom in or out on the map
        )
    )
    return fig
def plot_growth_opportunities():
    growth_opp = pd.read_json('filtered_datasets/business_dataset/growth_opportunities.json', lines=True)
    pivot_growth = growth_opp.pivot(index='city', columns='categories', values='avg_review_count').fillna(0)

    fig = go.Figure(data=go.Heatmap(
        z=pivot_growth.values,
        x=pivot_growth.columns,
        y=pivot_growth.index,
        colorscale='YlGnBu',
        colorbar=dict(title='Avg Review Count')
    ))
    fig.update_layout(
        title='Potential Growth Opportunities (Avg Review Count by City & Category)',
        xaxis_title='Category',
        yaxis_title='City',
        width=1500,  # Make the plot even wider for better spacing
        height=2500,  # Increase height to accommodate the y-axis labels
        margin=dict(l=150, r=150, t=100, b=300),  # Increase bottom margin to avoid clipping
        font=dict(size=12),  # Adjust font size for labels
    )
    fig.update_xaxes(
        tickangle=90,  # Rotate x-axis labels 90 degrees to avoid overlap
        tickmode='array',  # Ensure all x-ticks are shown
        tickvals=list(range(len(pivot_growth.columns)))  # Ensure the ticks align with the columns
    )
    fig.update_yaxes(
        tickmode='array',  # Ensure all y-ticks are shown
        tickvals=list(range(len(pivot_growth.index)))  # Ensure the ticks align with the rows
    )
    
    return fig


# Main Streamlit App
def main():
    st.set_page_config(layout="wide") # Set the layout to wide

    st.title("Yelp Business Dataset Analysis")
    
    
    # Grid Layout for Plots
    col1, col2 = st.columns(2)
    display_plot("Top Categories by Count", plot_top_categories, col1, "This chart identifies the categories that appear most frequently, indicating the dominant types of businesses present. Comparing the counts across categories can reveal insights into market trends or the composition of the business ecosystem. The y-axis represents the business categories, and the x-axis represents the number of businesses.")
    display_plot("Review Count vs Star", plot_review_vs_star, col2, "This chart investigates the relationship between the popularity of a business (as measured by review count) and its average rating. By examining the scatter plot, we can look for patterns, such as whether businesses with more reviews tend to have higher or lower ratings, or if there is a wide range of ratings regardless of review count. The x-axis represents the review count, and the y-axis represents the average star rating.")

    col4, = st.columns(1)
    display_plot("Potential Growth Oppotunities", plot_growth_opportunities, col4, "This chart explores potential growth opportunities by analyzing the average number of reviews for different business categories within various cities. Higher average review counts can suggest stronger user engagement and potentially a more competitive market, while lower average review counts might indicate untapped opportunities or less competition. The heatmap allows for a quick comparison of review activity across cities and categories, helping to identify areas where businesses might consider expanding or focusing their efforts. The x-axis represents business categories, the y-axis represents cities, and the color scale represents the average review count.")

    col5, = st.columns(1)
    display_plot("Hotspots of the Highest-Rated Business", plot_high_rated_hotspots, col5, "This map aims to pinpoint geographic areas where businesses generally receive favorable ratings. By visualizing these hotspots, we can potentially identify regions with strong customer satisfaction or areas where certain types of businesses are particularly successful. Further analysis could explore the factors contributing to these high ratings in different locations, such as local culture, economic conditions, or service quality. The map of the United States shows the spatial distribution, and the color or intensity of the points is used to represent the level of positive business ratings.")

if __name__ == "__main__":
    main()