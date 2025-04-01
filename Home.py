import streamlit as st

st.title("Yelp Data Analysis Home Page")

st.markdown(
    """
    <style>
    .medium-font {
        font-size: 25px !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("<p class='medium-font'>Welcome to our Yelp Data Analysis Dashboard!</p>", unsafe_allow_html=True)
st.markdown("<p class='medium-font'>Explore the insights we've uncovered:</p>", unsafe_allow_html=True)

st.markdown(
    """
    -   [Business, Checkin and Tip Joined Analysis](Buisness_Checkin_Tip_Combined_Analysis)
    -   [Review Sentiment Analysis](Review_Analysis)
    -   [User Behavior Analysis](User_Analysis)
    -   [Business Analysis](Business_Analysis)
    """,
    unsafe_allow_html=True
)

st.markdown("<p class='medium-font'>More indepth Analysis of Yelp</p>", unsafe_allow_html=True)

st.markdown(
    """
    -   [Is Yelp Growing? A User-Centric Analysis](Is_Yelp_Growing)
    """,
    unsafe_allow_html=True
)

st.write("For more information about the project, visit our GitHub Repository:")
st.markdown(
    "[GitHub Repository](https://github.com/clarenceagcc/INF2006-BigData-Project)",
    unsafe_allow_html=True
)