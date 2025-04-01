from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType  # Import all necessary types

# Configuration for Output Paths
s3_business_stats_path = "s3://your-bucket/output/business_analysis/business_stats.csv"
s3_category_stats_path = "s3://your-bucket/output/business_analysis/category_stats.csv"
s3_checkin_patterns_path = "s3://your-bucket/output/business_analysis/checkin_patterns.csv"
s3_tip_analysis_path = "s3://your-bucket/output/business_analysis/tip_analysis.csv"

# Schemas (as provided)
business_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("stars", FloatType(), True),
    StructField("review_count", StringType(), True),
    StructField("is_open", StringType(), True),
    StructField("categories", StringType(), True)
])

checkin_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("date", StringType(), True)
])

tip_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("tip_date", StringType(), True),
    StructField("compliment_count", StringType(), True)
])


def load_business_data(spark, input_path):
    """Loads the business data from CSV."""
    df_business = spark.read.csv(input_path, schema=business_schema, header=True, sep="\t")  # Adjust parameters as needed
    return df_business


def load_checkin_data(spark, input_path):
    """Loads the check-in data from CSV."""
    df_checkin = spark.read.csv(input_path, schema=checkin_schema, header=True, sep="\t")  # Adjust parameters as needed
    return df_checkin


def load_tip_data(spark, input_path):
    """Loads the tip data from CSV."""
    df_tip = spark.read.csv(input_path, schema=tip_schema, header=True, sep="\t")  # Adjust parameters as needed
    return df_tip


def analyze_business_data(business_df, checkin_df, tip_df, spark):
    """Analyzes the business data (e.g., basic stats)."""

    def join_and_aggregate_data(business_df, checkin_df, tip_df, spark):
        """Joins the DataFrames and calculates aggregations."""

        # Join the DataFrames
        full_df = business_df.join(checkin_df, on="business_id", how="left")
        full_df = full_df.join(tip_df, on="business_id", how="left")

        # Example Aggregations (replace/modify as needed)
        # It's crucial to adapt these to your precise analysis requirements
        # and ensure the output paths are correct.

        # 1. Star Distribution
        star_distribution = business_df.groupBy('stars').count().withColumnRenamed("count", "business_count")
        star_distribution.write.csv(s3_business_stats_path, header=True, mode="overwrite")
        print(f"Star distribution saved to: {s3_business_stats_path}")

        # 2. Average Ratings by State
        avg_ratings_by_state = business_df.groupBy('state').agg(F.avg('stars').alias('avg_stars')).orderBy(F.col('avg_stars').desc())
        avg_ratings_by_state.write.csv(s3_business_stats_path, header=True, mode="overwrite")  # Using a placeholder path
        print(f"Average ratings by state saved to: {s3_business_stats_path}")

        # 3. Business Counts by City
        top_cities = business_df.groupBy('city').count().orderBy(F.col('count').desc())
        top_cities.write.csv(s3_tip_analysis_path, header=True, mode="overwrite")
        print(f"Top cities saved to: {s3_tip_analysis_path}")

    
    join_and_aggregate_data(business_df, checkin_df, tip_df, spark)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("YelpBusinessAnalysis").getOrCreate()

    # Load data (replace with your actual paths)
    business_input_path = "s3://your-bucket/input/business_data.csv"
    checkin_input_path = "s3://your-bucket/input/checkin_data.csv"
    tip_input_path = "s3://your-bucket/input/tip_data.csv"

    df_business = load_business_data(spark, business_input_path)
    df_checkin = load_checkin_data(spark, checkin_input_path)
    df_tip = load_tip_data(spark, tip_input_path)

    # Perform analysis
    analyze_business_data(df_business, spark)
    spark.stop()