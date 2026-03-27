from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg, col, desc, year, month
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder \
    .appName("ETL to ClickHouse") \
    .config("spark.jars", "/home/sanechka/BDA/BigDataSpark/postgresql-42.7.3.jar") \
    .getOrCreate()

print("Spark version:", spark.version)

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

print("Reading data from PostgreSQL...")
df = spark.read.jdbc(url=jdbc_url, table="public.mock_data", properties=properties)
print("Total rows:", df.count())


print("\n=== Creating reports ===")

top_products = df.groupBy("product_name", "product_category") \
    .agg(
        _sum("sale_quantity").alias("total_quantity"),
        _sum("sale_total_price").alias("total_revenue")
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

print("\n=== Top 10 Products ===")
top_products.show(truncate=False)

revenue_by_category = df.groupBy("product_category") \
    .agg(_sum("sale_total_price").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

print("\n=== Revenue by Category ===")
revenue_by_category.show(truncate=False)

product_ratings = df.select("product_name", "product_rating", "product_reviews") \
    .distinct() \
    .orderBy(desc("product_rating")) \
    .limit(10)

print("\n=== Product Ratings ===")
product_ratings.show(truncate=False)

top_customers = df.groupBy("customer_email", "customer_first_name", "customer_last_name") \
    .agg(
        _sum("sale_total_price").alias("total_spent"),
        count("id").alias("purchase_count")
    ) \
    .orderBy(desc("total_spent")) \
    .limit(10)

print("\n=== Top 10 Customers ===")
top_customers.show(truncate=False)

customers_by_country = df.groupBy("customer_country") \
    .agg(countDistinct("customer_email").alias("customer_count")) \
    .orderBy(desc("customer_count"))

print("\n=== Customers by Country (top 10) ===")
customers_by_country.show(10, truncate=False)

monthly_trends = df.withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .groupBy("year", "month") \
    .agg(
        _sum("sale_total_price").alias("total_revenue"),
        _sum("sale_quantity").alias("total_quantity"),
        count("id").alias("order_count")
    ) \
    .orderBy("year", "month")

print("\n=== Monthly Trends ===")
monthly_trends.show(20, truncate=False)

print("\n=== Saving reports to CSV ===")

top_products.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/top_products")
revenue_by_category.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/revenue_by_category")
product_ratings.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/product_ratings")
top_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/top_customers")
customers_by_country.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/customers_by_country")
monthly_trends.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/monthly_trends")

print("\n All reports saved to ./spark_data/ directory!")

print("\n=== Reports created successfully! ===")

spark.stop()