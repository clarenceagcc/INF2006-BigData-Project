from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import functions as F
import pandas as pd

# Configuration
s3_correlation_matrix_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/correlation_matrix.csv"
s3_top_users_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/avg_stars_by_elite.csv"
s3_top_businesses_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/avg_stars.csv"
s3_star_distribution_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/compliment_avgs.csv"
s3_sentiment_analysis_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/yelping_years.csv"
s3_avg_userful_funny_cool_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/daily_user_counts.csv"
s3_avg_review_length_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/yearly_user_growth.csv"
s3_review_length_stats_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/review_dataset/yearly_user_growth_after_2008.csv"

def calculate_star_distribution(df, spark):
    """Calculates and saves the star distribution."""
    star_distribution = df.groupBy('stars').count().withColumnRenamed("count", "review_count")
    star_distribution.show() # Keep show() for debugging
    star_distribution.write.csv(s3_star_distribution_path, header=True, mode="overwrite")
    print(f"Star distribution saved to: {s3_star_distribution_path}")

def calculate_avg_useful_funny_cool(df, spark):
    """Calculates and saves the average useful/funny/cool per star rating."""
    avg_useful_funny_cool = df.groupBy('stars').agg(
        F.avg('useful').alias('avg_useful'),
        F.avg('funny').alias('avg_funny'),
        F.avg('cool').alias('avg_cool')
    )
    avg_useful_funny_cool.show() # Keep show() for debugging
    avg_useful_funny_cool.write.csv(s3_avg_userful_funny_cool_path, header=True, mode="overwrite")
    print(f"Avg useful/funny/cool saved to: {s3_avg_userful_funny_cool_path}")

def calculate_avg_review_length(df, spark):
    """Calculates and saves the average review length per star rating."""
    df_with_length = df.withColumn("review_length", F.length('text'))
    avg_review_length = df_with_length.groupBy('stars').agg(
        F.avg('review_length').alias('avg_review_length')
    )
    avg_review_length.show() # Keep show() for debugging
    avg_review_length.write.csv(s3_avg_review_length_path, header=True, mode="overwrite")
    print(f"Avg review length saved to: {s3_avg_review_length_path}")

def calculate_review_length_stats(df, spark):
    """Calculates and saves review length statistics."""
    df_with_length = df.withColumn("review_length", F.length(col("text")))

    # Sample the data (for fair processing)
    sampled_df = df_with_length.sample(fraction=0.1, withReplacement=False)

    # Group by review_length and count occurrences
    agg_df = sampled_df.groupBy("review_length").count()

    # Sort the aggregated DataFrame before saving
    sorted_agg_df = agg_df.orderBy("review_length")

    # Save as a single CSV file with "review_length" and "count" columns
    sorted_agg_df.coalesce(1).write.csv(s3_review_length_stats_path, header=True, mode="overwrite")
    print(f"Review length stats saved to: {s3_review_length_stats_path}")

def calculate_correlation_matrix(df, spark):
    """Calculates and saves the correlation matrix."""
    # Sample a fraction of the DataFrame
    sampled_df = df.sample(fraction=0.1, withReplacement=False).select("stars", "useful", "funny", "cool")

    # Convert sampled DataFrame to Pandas for correlation computation
    sampled_pd = sampled_df.toPandas()

    # Compute correlation matrix
    corr_matrix = sampled_pd.corr()

    # Convert Pandas DataFrame back to Spark DataFrame
    corr_matrix_spark = spark.createDataFrame(corr_matrix.reset_index().rename(columns={"index": "column"}))

    # Save the correlation matrix to a CSV file
    corr_matrix_spark.write.csv(s3_correlation_matrix_path, header=True, mode="overwrite")
    print(f"Correlation matrix saved to: {s3_correlation_matrix_path}")

def calculate_top_businesses(df, df2, spark):  # Add df2 as a parameter
    """Calculates and saves the top 5 most reviewed businesses."""
    # Aggregate review counts per business and sort in descending order
    top_businesses_df = df.groupBy("business_id").count().orderBy(F.col("count").desc()).limit(5)

    # Join with df2 to get business names
    top_businesses_with_names_df = (
        top_businesses_df
        .join(df2, on="business_id", how="inner")
        .select("business_id", "name", "count")
        .orderBy(F.col("count").desc())  # Ensure sorting before saving
    )

    # Save the sorted DataFrame as a single CSV file
    top_businesses_with_names_df.coalesce(1).write.csv(s3_top_businesses_path, header=True, mode="overwrite")
    print(f"Top businesses saved to: {s3_top_businesses_path}")

def calculate_top_users(df, df3, spark):  # Add df3
    """Calculates and saves the top 5 most active users."""
    # Group by 'user_id' and count reviews, then order by count in descending order and limit to top 5
    top_users_df = (
        df.groupBy("user_id")
        .count()
        .orderBy(F.col("count").desc())
        .limit(5)
    )

    # Join with df3 to get user names
    top_users_with_names_df = (
        top_users_df
        .join(df3, top_users_df.user_id == df3.userId, how="inner")
        .select("user_id", "name", "count")
        .orderBy(F.col("count").desc())  # Ensure sorting before saving
    )

    # Save the sorted DataFrame as a single CSV file
    top_users_with_names_df.coalesce(1).write.csv(s3_top_users_path, header=True, mode="overwrite")
    print(f"Top users saved to: {s3_top_users_path}")

def calculate_sentiment_analysis(df, spark):
    """Calculates and saves the sentiment analysis based on star ratings."""
    # Create a new column 'sentiment' based on the 'stars' column
    df_with_sentiment = df.withColumn("sentiment",
                                        F.when(F.col("stars") >= 4, "Positive")
                                        .when(F.col("stars") == 3, "Neutral")
                                        .otherwise("Negative"))

    # Group by sentiment and count the occurrences
    sentiment_counts_df = df_with_sentiment.groupBy("sentiment").count()

    # Save the aggregated DataFrame as a CSV
    sentiment_counts_df.write.csv(s3_sentiment_analysis_path, header=True, mode="overwrite")
    print(f"Sentiment analysis saved to: {s3_sentiment_analysis_path}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("YelpAnalysis").getOrCreate()
    file_paths = [
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output0-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output1-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output2-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output3-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output4-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output5-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output6-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output7-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output8-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output9-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output10-r-00000",
        "s3://sg.edu.sit.inf2006.clarence/output/review_data_processed/output0/output11-r-00000"
    ]
    # Load all 12 text files into one DataFrame (assuming they have the same schema)
    df_review = spark.read.option("delimiter", "\t").csv(file_paths, header=False, inferSchema=True)
    df_review = df_review.toDF('review_id', 'user_id', 'business_id', 'stars', 'useful', 'funny', 'cool', 'text', 'date')

    # Manually set column names
    df = df_review.toDF('review_id', 'user_id', 'business_id', 'stars', 'useful', 'funny', 'cool', 'text', 'date')

    # Show the first few rows to verify the data is loaded correctly
    df.show(10)

    # Get the total number of rows in the combined DataFrame
    total_rows = df.count()
    print(f"Total rows after combining the files: {total_rows}")

    file_paths = [
    "s3://sg.edu.sit.inf2006.clarence/output/buisness_data_processed/output0/business-r-00000",
    ]

    # Load business file into DataFrame

    df_business = spark.read.option("delimiter", "\t").csv(file_paths, header=False, inferSchema=True)
    
    calculate_star_distribution(df_review, spark)
    calculate_avg_useful_funny_cool(df_review, spark)
    calculate_avg_review_length(df_review, spark)
    calculate_review_length_stats(df_review, spark)
    calculate_correlation_matrix(df_review, spark)
    calculate_top_businesses(df_review, df_business, spark)
    calculate_top_users(df_review, df_business, spark)
    calculate_sentiment_analysis(df, spark)

    spark.stop()