from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, when, avg

# Initialize Spark session
spark = SparkSession.builder.appName("YelpBusinessAnalysis").getOrCreate()

# Load cleaned JSON file from S3
business_df = spark.read.json("s3://sc.inf2006.kyree/cleaned_business.jsonl")

# Create exploded version of categories for reuse
business_exploded = business_df.withColumn("category", explode(split(col("categories"), ", ")))

# 1. Business Distribution by Category and Location
category_distribution = business_exploded.groupBy("city", "state", "category").count()
category_distribution.write.json("s3://sc.inf2006.kyree/output/category_distribution.json")

# 2. Review Count vs. Star Rating Relationship
review_stars = business_df.select("review_count", "stars")
review_stars.write.json("s3://sc.inf2006.kyree/output/review_vs_stars.json")

# 3. Business closure rates heatmap by city and category
closure_rates = business_exploded.groupBy("city", "category").agg(
    (count("*") - count(when(col("is_open") == True, True))).alias("closed_count"),
    count("*").alias("total_count")
)
closure_rates = closure_rates.withColumn("closure_rate", col("closed_count") / col("total_count"))
closure_rates.write.json("s3://sc.inf2006.kyree/output/closure_heatmap_data.json")

# 4. Geographic Hotspots of High-Rated Businesses
high_rated_geo = business_df.filter(col("stars") >= 4.5).select(
    "business_id", "name", "stars", "city", "state", "latitude", "longitude", "categories")
high_rated_geo.write.json("s3://sc.inf2006.kyree/output/high_rated_hotspots.json")

# 5. Identifying Growth Opportunities (Underserved Markets)
growth_opportunities = business_exploded.groupBy("city", "category").agg(
    count("business_id").alias("business_count"),
    avg("review_count").alias("avg_review_count"),
    avg("stars").alias("avg_stars")
)
growth_opportunities.write.json("s3://sc.inf2006.kyree/output/growth_opportunities.json")

# Stop Spark session
spark.stop()