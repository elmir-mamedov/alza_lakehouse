"""silver_layer.py
Bronze → Silver: Read raw JSON exports from PostgreSQL,
flatten and clean them into silver Delta tables.

Usage:
    python silver_layer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, collect_list, struct, map_from_entries,
    when, trim, regexp_replace, size, element_at,
    regexp_extract, concat_ws
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, FloatType

# ---------------------------------------------------------------------------
# Spark session with Delta Lake
# ---------------------------------------------------------------------------

spark = (SparkSession.builder
    .appName("AlzaSilverLayer")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. SILVER PRODUCTS — from productDetail endpoint
# ---------------------------------------------------------------------------

print("=" * 60)
print("Building silver_products...")
print("=" * 60)

# Read raw JSON (one JSON object per line)
raw_detail = spark.read.json("get_data/raw/product_detail.json")

# All useful fields live under "data"
df = raw_detail.select("data.*")

# --- Core fields ---
silver_products = (df
    .select(
        col("id").cast(IntegerType()).alias("product_id"),
        col("name"),
        col("code"),
        col("catalog_number"),
        col("gaPrice").cast(DoubleType()).alias("price"),
        col("cpriceNoCurrency").cast(DoubleType()).alias("original_price"),
        col("categoryId").cast(IntegerType()).alias("category_id"),
        col("categoryName").alias("category"),
        col("gaCategory").alias("parent_category"),
        col("rating").cast(FloatType()),
        col("ratingCount").cast(IntegerType()).alias("rating_count"),
        col("sales").cast(IntegerType()),
        col("warranty"),
        col("is_in_stock").cast(BooleanType()),
        col("avail").alias("availability_text"),
        col("spec").alias("spec_summary"),
        col("eshop"),
        col("producerId").cast(IntegerType()).alias("producer_id"),
    )
    # Drop rows with no product ID (bad responses)
    .filter(col("product_id").isNotNull())
    # Deduplicate
    .dropDuplicates(["product_id"])
)

# --- Add discount percentage ---
silver_products = silver_products.withColumn(
    "discount_pct",
    when(
        (col("original_price").isNotNull()) & (col("original_price") > 0),
        ((col("original_price") - col("price")) / col("original_price") * 100)
            .cast(FloatType())
    )
)

# --- Extract breadcrumb levels ---
# breadcrumb is an array of structs with nested category.name
breadcrumb_df = (df
    .select(
        col("id").cast(IntegerType()).alias("product_id"),
        col("breadcrumb")
    )
    .filter(col("breadcrumb").isNotNull())
    .filter(size(col("breadcrumb")) > 0)
    .select(
        "product_id",
        # Level 0 = section (e.g. "Household and Personal Appliances")
        element_at(col("breadcrumb"), 1)["category"]["name"]
            .alias("breadcrumb_section"),
        # Level 1 = category (e.g. "Coffee Makers and Presses")
        when(size(col("breadcrumb")) >= 2,
             element_at(col("breadcrumb"), 2)["category"]["name"])
            .alias("breadcrumb_category"),
        # Level 2 = subcategory (e.g. "Lever")
        when(size(col("breadcrumb")) >= 3,
             element_at(col("breadcrumb"), 3)["category"]["name"])
            .alias("breadcrumb_subcategory"),
    )
)

silver_products = silver_products.join(breadcrumb_df, on="product_id", how="left")

# --- Extract specs (parameterGroups) into a flat map ---
# Explode parameter groups → params → values, then pivot into a map
specs_df = (df
    .select(
        col("id").cast(IntegerType()).alias("product_id"),
        explode(col("parameterGroups")).alias("group")
    )
    .select(
        "product_id",
        explode(col("group.params")).alias("param")
    )
    .select(
        "product_id",
        col("param.name").alias("param_name"),
        element_at(col("param.values"), 1)["desc"].alias("param_value")
    )
    .filter(col("param_name").isNotNull() & col("param_value").isNotNull())
    # Group back to one row per product with a map of specs
    .groupBy("product_id")
    .agg(
        map_from_entries(
            collect_list(struct("param_name", "param_value"))
        ).alias("specs")
    )
)

silver_products = silver_products.join(specs_df, on="product_id", how="left")

# --- Write to Delta ---
print(f"silver_products count: {silver_products.count()}")
silver_products.printSchema()
silver_products.show(5, truncate=40)

silver_products.write.format("delta").mode("overwrite").save("data/silver/products")
print("✅ silver_products written to data/silver/products")


# ---------------------------------------------------------------------------
# 2. SILVER REVIEW STATS — from reviewStats endpoint
# ---------------------------------------------------------------------------

print("\n" + "=" * 60)
print("Building silver_review_stats...")
print("=" * 60)

raw_stats = spark.read.json("get_data/raw/review_stats.json")

silver_review_stats = (raw_stats
    .select(
        # Extract commodity_id from self.href URL
        regexp_extract(col("self.href"), r"commodities/(\d+)/", 1)
            .cast(IntegerType()).alias("product_id"),
        col("name"),
        col("ratingAverage").cast(FloatType()).alias("avg_rating"),
        col("ratingCount").cast(IntegerType()).alias("rating_count"),
        col("reviewCount").cast(IntegerType()).alias("review_count"),
        col("recommendationRate").cast(FloatType()).alias("recommendation_rate"),
        col("purchaseCountFormatted").alias("purchase_count_text"),
        col("showPurchaseCount").cast(BooleanType()),
        # Ratings breakdown
        col("ratings"),
        # Complaint info
        col("complaint.rate").cast(FloatType()).alias("complaint_rate"),
        col("complaint.description").alias("complaint_description"),
    )
    .filter(col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

# Explode ratings breakdown into separate columns
ratings_exploded = (raw_stats
    .select(
        regexp_extract(col("self.href"), r"commodities/(\d+)/", 1)
            .cast(IntegerType()).alias("product_id"),
        explode(col("ratings")).alias("r")
    )
    .select(
        "product_id",
        col("r.value").cast(IntegerType()).alias("star"),
        col("r.count").cast(IntegerType()).alias("star_count"),
    )
)

# Pivot: one column per star level
ratings_pivot = (ratings_exploded
    .groupBy("product_id")
    .pivot("star", [1, 2, 3, 4, 5])
    .sum("star_count")
)

# Rename pivoted columns
for i in range(1, 6):
    ratings_pivot = ratings_pivot.withColumnRenamed(str(i), f"stars_{i}")

silver_review_stats = (silver_review_stats
    .drop("ratings")
    .join(ratings_pivot, on="product_id", how="left")
)

print(f"silver_review_stats count: {silver_review_stats.count()}")
silver_review_stats.printSchema()
silver_review_stats.show(5, truncate=40)

silver_review_stats.write.format("delta").mode("overwrite").save("data/silver/review_stats")
print("✅ silver_review_stats written to data/silver/review_stats")


# ---------------------------------------------------------------------------
# 3. SILVER REVIEWS — from reviews endpoint
# ---------------------------------------------------------------------------

print("\n" + "=" * 60)
print("Building silver_reviews...")
print("=" * 60)

raw_reviews = spark.read.json("get_data/raw/reviews.json")

# Reviews are nested inside "value" array
# Extract product_id from the self link of each response
silver_reviews = (raw_reviews
    .select(
        regexp_extract(col("self.href"), r"commodities/(\d+)/", 1)
            .cast(IntegerType()).alias("product_id"),
        explode(col("value")).alias("review")
    )
    .select(
        "product_id",
        col("review.name").alias("author"),
        col("review.rating").cast(FloatType()).alias("rating"),
        col("review.description").alias("review_text"),
        concat_ws("; ", col("review.positives")).alias("pros"),
        concat_ws("; ", col("review.negatives")).alias("cons"),
        col("review.reviewDate").alias("review_date"),
        col("review.likeCount").cast(IntegerType()).alias("like_count"),
        col("review.isTranslated").cast(BooleanType()).alias("is_translated"),
        col("review.commodityName").alias("product_name"),
    )
    .dropDuplicates(["author", "review_date", "product_name"])
)

# Clean review text
silver_reviews = silver_reviews.withColumn(
    "review_text",
    trim(regexp_replace(col("review_text"), r"\s+", " "))
)

print(f"silver_reviews count: {silver_reviews.count()}")
silver_reviews.printSchema()
silver_reviews.show(5, truncate=60)

silver_reviews.write.format("delta").mode("overwrite").save("data/silver/reviews")
print("✅ silver_reviews written to data/silver/reviews")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print("\n" + "=" * 60)
print("SILVER LAYER COMPLETE")
print("=" * 60)
print(f"  data/silver/products     — {silver_products.count()} rows")
print(f"  data/silver/review_stats — {silver_review_stats.count()} rows")
print(f"  data/silver/reviews      — {silver_reviews.count()} rows")

spark.stop()