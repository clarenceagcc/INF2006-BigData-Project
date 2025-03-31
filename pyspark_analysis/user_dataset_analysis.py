from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import functions as F
import pandas as pd

# Configuration
s3_correlation_matrix_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/correlation_matrix"
s3_avg_stars_by_elite_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/avg_stars_by_elite"
s3_avg_stars_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/avg_stars"
s3_compliment_avgs_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/compliment_avgs"
s3_yelping_years_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/yelping_years"
s3_daily_user_counts_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/daily_user_counts"
s3_yearly_growth_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/yearly_user_growth"
s3_yearly_growth_after_2008_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/yearly_user_growth_after_2008"
s3_no_review_growth_corr_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/no_review_growth_corr"
s3_daily_review_counts_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/daily_review_counts"
s3_user_review_growth_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/user_review_growth"
s3_kmeans_predictions_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/kmeans_predictions"
s3_kmeans_centers_path = "s3://sg.edu.sit.inf2006.clarence/output/filtered_datasets/user_dataset/kmeans_centers"

def calculate_kmeans_clustering(data, spark):
    # Feature Selection
    feature_cols = ["reviewCount", "avgStars", "friendCount", "fans"]

    # Vector Assembly
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled_data = assembler.transform(data)

    # Scaling
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaled_data = scaler.fit(assembled_data).transform(assembled_data)

    # K-Means Model
    kmeans = KMeans(k=3, seed=1)  # Choose k = 3 (number of clusters)
    model = kmeans.fit(scaled_data)
    predictions = model.transform(scaled_data)

    # Extract Cluster Centers
    centers = model.clusterCenters()

    # Save Predictions to CSV
    predictions.write.csv(s3_kmeans_predictions_path, mode="overwrite", header=True)
    print(f"K-Means predictions saved to: {s3_kmeans_predictions_path}")

    # Convert cluster centers to Pandas DataFrame
    centers_df = pd.DataFrame(centers, columns=feature_cols)

    # Convert Pandas DataFrame back to Spark DataFrame
    centers_spark = spark.createDataFrame(centers_df)

    # Save Cluster Centers to CSV
    centers_spark.write.csv(s3_kmeans_centers_path, mode="overwrite", header=True)
    print(f"K-Means cluster centers saved to: {s3_kmeans_centers_path}")

def calculate_user_review_growth(data, df_review, spark):
    # Extract the year from yelpingSinceDate (users)
    data = data.withColumn("yelpingYear", F.year("yelpingSinceDate"))

    # Extract the year from the review date (reviews)
    df_review = df_review.withColumn("reviewYear", F.year(F.to_date(F.col("date"), "dd-MM-yyyy")))

    # Group by year and count users
    yearly_user_counts = data.groupBy("yelpingYear").count().orderBy("yelpingYear")

    # Group by year and count reviews
    yearly_review_counts = df_review.groupBy("reviewYear").count().orderBy("reviewYear")

    # Convert to Pandas DataFrames
    pandas_users = yearly_user_counts.toPandas()
    pandas_reviews = yearly_review_counts.toPandas()

    # Calculate year-on-year user growth
    pandas_users["user_growth"] = pandas_users["count"].pct_change() * 100
    pandas_users = pandas_users.dropna()

    # Calculate year-on-year review growth
    pandas_reviews["review_growth"] = pandas_reviews["count"].pct_change() * 100
    pandas_reviews = pandas_reviews.dropna()

    # Merge DataFrames
    merged_df = pd.merge(pandas_users[["yelpingYear", "user_growth"]], pandas_reviews[["reviewYear", "review_growth"]], left_on="yelpingYear", right_on="reviewYear")

    # Filter for years after 2008
    merged_df = merged_df[merged_df["yelpingYear"] >= 2008]

    # Convert Pandas DataFrame back to Spark DataFrame
    user_review_growth_spark = spark.createDataFrame(merged_df)

    # Write to CSV
    user_review_growth_spark.write.csv(s3_user_review_growth_path, mode="overwrite", header=True)
    print(f"User review growth data saved to: {s3_user_review_growth_path}")

def calculate_daily_review_counts(df_review, spark):
    # Convert date string to date type
    df_review = df_review.withColumn("date", F.to_date(F.col("date"), "dd-MM-yyyy"))

    # Group by date and count reviews
    daily_review_counts = df_review.groupBy("date").count().orderBy("date")

    # Convert to Pandas DataFrame
    pandas_df = daily_review_counts.toPandas()

    # Calculate 30-day rolling mean
    pandas_df["rolling_mean"] = pandas_df["count"].rolling(window=30).mean()

    # Convert Pandas DataFrame back to Spark DataFrame
    daily_review_counts_spark = spark.createDataFrame(pandas_df)

    # Write to CSV
    daily_review_counts_spark.write.csv(s3_daily_review_counts_path, mode="overwrite", header=True)
    print(f"Daily review counts data saved to: {s3_daily_review_counts_path}")

def calculate_no_review_growth_correlation(data, spark):
    # Extract the year from yelpingSinceDate
    data = data.withColumn("yelpingYear", F.year("yelpingSinceDate"))

    # Calculate proportion of users with no reviews
    no_review_users = data.filter(F.col("reviewCount") == 0)
    yearly_no_review_counts = no_review_users.groupBy("yelpingYear").count().orderBy("yelpingYear")
    yearly_total_users = data.groupBy("yelpingYear").count().orderBy("yelpingYear")

    pandas_no_review = yearly_no_review_counts.toPandas()
    pandas_total_users = yearly_total_users.toPandas()

    pandas_no_review["proportion"] = pandas_no_review["count"] / pandas_total_users["count"] * 100

    # Calculate year-on-year user growth
    pandas_total_users["growth"] = pandas_total_users["count"].pct_change() * 100
    pandas_total_users = pandas_total_users.dropna()

    # Merge dataframes
    merged_df = pd.merge(pandas_no_review[["yelpingYear", "proportion"]], pandas_total_users[["yelpingYear", "growth"]], on="yelpingYear")

    # Filter for years after 2008
    merged_df = merged_df[merged_df["yelpingYear"] >= 2008]

    # Convert Pandas DataFrame back to Spark DataFrame
    no_review_growth_corr_spark = spark.createDataFrame(merged_df)

    # Write to CSV
    no_review_growth_corr_spark.write.csv(s3_no_review_growth_corr_path, mode="overwrite", header=True)
    print(f"No review growth correlation data saved to: {s3_no_review_growth_corr_path}")

def calculate_yearly_user_growth_after_2008(data, spark):
    # Extract the year from yelpingSinceDate
    data = data.withColumn("yelpingYear", F.year("yelpingSinceDate"))

    # Group by year and count users
    yearly_user_counts = data.groupBy("yelpingYear").count().orderBy("yelpingYear")

    # Convert to Pandas DataFrame
    pandas_df = yearly_user_counts.toPandas()

    # Calculate year-on-year growth
    pandas_df["growth"] = pandas_df["count"].pct_change() * 100

    # Remove the first row (NaN growth)
    pandas_df = pandas_df.dropna()

    # Filter for years after 2008
    pandas_df = pandas_df[pandas_df["yelpingYear"] > 2008]

    # Convert Pandas DataFrame back to Spark DataFrame
    yearly_growth_after_2008_spark = spark.createDataFrame(pandas_df)

    # Write to CSV
    yearly_growth_after_2008_spark.write.csv(s3_yearly_growth_after_2008_path, mode="overwrite", header=True)
    print(f"Yearly user growth data after 2008 saved to: {s3_yearly_growth_after_2008_path}")

def calculate_yearly_user_growth(data, spark):
    # Extract the year from yelpingSinceDate
    data = data.withColumn("yelpingYear", F.year("yelpingSinceDate"))

    # Group by year and count users
    yearly_user_counts = data.groupBy("yelpingYear").count().orderBy("yelpingYear")

    # Convert to Pandas DataFrame
    pandas_df = yearly_user_counts.toPandas()

    # Calculate year-on-year growth
    pandas_df["growth"] = pandas_df["count"].pct_change() * 100

    # Remove the first row (NaN growth)
    pandas_df = pandas_df.dropna()

    # Convert Pandas DataFrame back to Spark DataFrame
    yearly_growth_spark = spark.createDataFrame(pandas_df)

    # Write to CSV
    yearly_growth_spark.write.csv(s3_yearly_growth_path, mode="overwrite", header=True)
    print(f"Yearly user growth data saved to: {s3_yearly_growth_path}")

def calculate_daily_user_counts(data, spark):
    # Group by date and count new users
    data = data.withColumn(
        "yelpingSinceDate",
        to_date(col("yelpingSinceFormatted"), "dd-MM-yyyy")
    )

    daily_user_counts = data.groupBy("yelpingSince").count().orderBy("yelpingSinceDate")

    # Convert to Pandas DataFrame
    pandas_df = daily_user_counts.toPandas()

    # Calculate 30-day rolling mean
    pandas_df["rolling_mean"] = pandas_df["count"].rolling(window=30).mean()

    # Convert Pandas DataFrame back to Spark DataFrame
    daily_user_counts_spark = spark.createDataFrame(pandas_df)

    # Write to CSV
    daily_user_counts_spark.write.csv(s3_daily_user_counts_path, mode="overwrite", header=True)
    print(f"Daily user counts data saved to: {s3_daily_user_counts_path}")

def calculate_yelping_years(data, spark):
    # Convert date format
    data = data.withColumn(
        "yelpingSinceDate",
        to_date(col("yelpingSinceFormatted"), "dd-MM-yyyy")
    )

    # Users Joining Yelp Over Time
    yelping_years = data.groupBy(year("yelpingSinceDate").alias("yelpingYear")).count().orderBy("yelpingYear")

    # Write to CSV
    yelping_years.write.csv(s3_yelping_years_path, mode="overwrite", header=True)
    print(f"Yelping years data saved to: {s3_yelping_years_path}")

def calculate_compliment_avgs(data, spark):
    compliment_cols = ["complimentHot", "complimentCool", "complimentFunny", "complimentMore", "complimentWriter", "complimentPlain", "complimentPhotos", "complimentNote"]
    compliment_avgs = {col: data.agg(avg(col)).collect()[0][0] for col in compliment_cols}

    # Convert the dictionary to a list of tuples for DataFrame creation
    compliment_avgs_list = [(col, avg_val) for col, avg_val in compliment_avgs.items()]

    # Create a Spark DataFrame
    compliment_avgs_df = spark.createDataFrame(compliment_avgs_list, ["compliment_type", "average_count"])

    # Write to CSV
    compliment_avgs_df.write.csv(s3_compliment_avgs_path, mode="overwrite", header=True)
    print(f"Compliment averages saved to: {s3_compliment_avgs_path}")

def calculate_correlation_matrix(data, spark):
    numeric_cols = ["reviewCount", "avgStars", "useful", "funny", "cool", "eliteYears", "friendCount", "fans",
                    "complimentHot", "complimentCool", "complimentFunny", "complimentMore", "complimentWriter",
                    "complimentPlain", "complimentPhotos", "complimentNote"]
    numeric_data = data.select(numeric_cols).toPandas()
    correlation_matrix = numeric_data.corr()
    correlation_spark_df = spark.createDataFrame(correlation_matrix.reset_index().rename(columns={"index": "column_name"}))
    correlation_spark_df.write.csv(s3_correlation_matrix_path, mode="overwrite", header=True)
    print(f"Correlation matrix saved to: {s3_correlation_matrix_path}")

def calculate_avg_stars_by_elite(data):
    avg_stars_by_elite = data.groupBy("eliteYears").agg(avg("avgStars").alias("avg_stars"))
    avg_stars_by_elite.write.csv(s3_avg_stars_by_elite_path, mode="overwrite", header=True)
    print(f"Average stars by elite years saved to: {s3_avg_stars_by_elite_path}")

def calculate_avg_stars(data):
    avg_stars = data.select("avgStars").toPandas()
    spark.createDataFrame(avg_stars).write.csv(s3_avg_stars_path, mode="overwrite", header=True)
    print(f"Average stars saved to: {s3_avg_stars_path}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("YelpAnalysis").getOrCreate()
    schema = StructType([
        StructField("userId", StringType(), True),
        StructField("name", StringType(), True),  # Added name
        StructField("reviewCount", IntegerType(), True),
        StructField("avgStars", FloatType(), True),
        StructField("yelpingSinceFormatted", StringType(), True),
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
        StructField("eliteYears", IntegerType(), True),
        StructField("friendCount", IntegerType(), True),
        StructField("fans", IntegerType(), True),
        StructField("complimentHot", IntegerType(), True),
        StructField("complimentCool", IntegerType(), True),
        StructField("complimentFunny", IntegerType(), True),
        StructField("complimentMore", IntegerType(), True),
        StructField("complimentWriter", IntegerType(), True), # Changed to IntegerType
        StructField("complimentPlain", IntegerType(), True),
        StructField("complimentPhotos", IntegerType(), True),
        StructField("complimentNote", IntegerType(), True), # Changed to IntegerType
    ])

    data = spark.read.csv("s3://sg.edu.sit.inf2006.clarence/output/preprocessed_data/user_data_processed/part-r-00000", sep="\t", schema=schema)  # Replace with your data loading

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

    #calculate_correlation_matrix(data, spark)
    calculate_avg_stars_by_elite(data)
    calculate_avg_stars(data)
    calculate_compliment_avgs(data, spark)
    calculate_yelping_years(data, spark)
    calculate_daily_user_counts(data, spark)
    calculate_yearly_user_growth(data, spark)
    calculate_yearly_user_growth_after_2008(data, spark)
    calculate_no_review_growth_correlation(data, spark)
    calculate_daily_review_counts(df_review, spark)
    calculate_user_review_growth(data, df_review, spark)
    calculate_kmeans_clustering(data, spark)

    spark.stop()
