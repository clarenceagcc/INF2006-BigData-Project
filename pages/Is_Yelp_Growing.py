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
def display_plot(plot_title, plot_function):
    st.subheader(plot_title)
    st.plotly_chart(plot_function(), use_container_width=True)

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
def plot_daily_reviews_rolling_mean():
    # Load the data from the CSV file
    pandas_df = pd.read_csv("filtered_datasets/user_dataset/daily_new_reviews_rolling_mean.csv")

    # Plotting with Plotly
    fig = go.Figure()

    # Add rolling mean line
    fig.add_trace(go.Scatter(
        x=pandas_df['date'],
        y=pandas_df['rolling_mean'],
        mode='lines',
        name='Rolling Mean 30 Days',
        line=dict(color='royalblue', dash='dash')
    ))

    # Add daily count area
    fig.add_trace(go.Scatter(
        x=pandas_df['date'],
        y=pandas_df['count'],
        mode='lines',
        name='Daily Count',
        fill='tozeroy',
        line=dict(color='lavender')
    ))

    fig.update_layout(
        title='Daily Number of New Reviews',
        xaxis_title='Date',
        yaxis_title='Number of New Reviews',
        xaxis=dict(tickformat='%Y-%m-%d'), # Format date on x-axis
        legend=dict(x=0, y=1) # Position legend
    )

    return fig
def plot_yearly_user_review_growth_comparison():
    merged_df = pd.read_csv("filtered_datasets/user_dataset/yearly_user_review_growth_comparison.csv")
    fig = px.line(merged_df, x="yelpingYear", y=["review_growth", "user_growth"], title="Yearly User and Review Growth Comparison", labels={"value": "Percentage Growth", "yelpingYear": "Year"})
    fig.update_yaxes(ticksuffix="%")
    return fig

def plot_no_review_vs_user_growth():
    merged_df = pd.read_csv("filtered_datasets/user_dataset/no_review_vs_user_growth.csv")
    fig = px.line(merged_df, x="yelpingYear", y=["proportion", "growth"], title="Correlation Between User Who Writes No Review to User Growth", labels={"value": "Percentage", "yelpingYear": "Year"})
    fig.update_yaxes(ticksuffix="%")
    return fig

def main():
    st.title("Is Yelp Still Growing? A User-centric Analysis")
    
    display_plot("Users Joining Yelp Over Time Plot", plot_yelp_users_joining_over_time)
    st.write("This chart depicts the yearly count of new Yelp users.  We observe a rapid increase in user acquisition between 2008 and 2015, suggesting a period of strong growth and market penetration. However, post-2015, the number of new users entering the platform has consistently decreased, indicating a potential plateau or saturation in Yelp's user base.")
    display_plot("Overall Year-On-Year Growth", plot_yearly_user_growth)
    st.write("Due to extreme growth in the beginning, it is hard to see the effect in more recent years. Elbow is around 2008, let's zoom in after that to analyze further.")
    display_plot("Overall Year-On-Year Growth", plot_yearly_user_growth_after_2008)
    st.write("""
    As we can see, the growth is slowing down to about 10% YoY in 2017. 
    Is 10% Year-on-year growth good or not? We cannot expect to get 
    conclusion only from yelp data, we need to compare with other similar 
    social media platform growth and also yelp's target.

    For many social media, user growth declining is expected due to 
    market saturation.

    Usually, after user acquisition is slowing down, other than opening on a
    new market or hyperlocalization, companies will put more efforts to 
    retain existing customer.

    We will check whether user engagement in yelp is increasing.
    For this analysis, we limit the definition of engagement in yelp as giving review.
    We will explore:

    * Overall number of review growth
    * Per user average number of review
    """)
    display_plot("Daily No. of New Reviews", plot_daily_reviews_rolling_mean)
    st.write("""
    Building on the earlier observation of slowing user growth, this graph examines user engagement through the daily number of new reviews.  We see a clear upward trend in review volume, aligning with the period of peak user acquisition.  However, while user growth may be slowing, the graph suggests that existing users remain active in contributing reviews, as the daily review count maintains a relatively high level after 2018.
    """)
    display_plot("User Growth and Review Growth", plot_yearly_user_review_growth_comparison)
    st.write("As the gap between review growth and user growth widens, we get a sense that there has been more user giving reviews. This is a good sign, but we need to analyze further to find out whether more users or just the same user who keep writing reviews.")
    display_plot("No Review Vs User Growth", plot_no_review_vs_user_growth)
    st.write("This graph illustrates the relationship between the proportion of users who don't write reviews and Yelp's user growth from 2010 to 2022.  We observe a significant inverse correlation.  As the proportion of non-reviewing users decreases, user growth initially declines but later turns negative.  Both variables exhibit a dramatic drop in the later years, suggesting a potential shift in user behavior or platform dynamics.")
    st.write("In conclusion, our analysis reveals a complex picture of Yelp's user base and engagement. While Yelp experienced significant user growth in its early years, this growth has slowed considerably, indicating potential market saturation.  Despite this, user engagement, as measured by review activity, remained relatively high for a period, suggesting that existing users are still contributing. However, the recent correlation between declining user growth and an increasing proportion of non-reviewing users points to a potential challenge.  Yelp may need to focus on strategies to re-engage users and attract new audiences to counteract these trends.")
if __name__ == "__main__":
    main()