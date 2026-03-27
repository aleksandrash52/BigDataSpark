from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, col, desc, year, month
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
print(f"Total rows: {df.count()}")

top_products = df.groupBy("product_name", "product_category") \
    .agg(
        _sum("sale_quantity").alias("total_quantity"),
        _sum("sale_total_price").alias("total_revenue")
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

revenue_by_category = df.groupBy("product_category") \
    .agg(_sum("sale_total_price").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

product_ratings = df.select("product_name", "product_rating", "product_reviews") \
    .distinct() \
    .orderBy(desc("product_rating")) \
    .limit(10)

top_customers = df.groupBy("customer_email", "customer_first_name", "customer_last_name") \
    .agg(
        _sum("sale_total_price").alias("total_spent"),
        count("id").alias("purchase_count")
    ) \
    .orderBy(desc("total_spent")) \
    .limit(10)

customers_by_country = df.groupBy("customer_country") \
    .agg(countDistinct("customer_email").alias("customer_count")) \
    .orderBy(desc("customer_count"))

monthly_trends = df.withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .groupBy("year", "month") \
    .agg(
        _sum("sale_total_price").alias("total_revenue"),
        _sum("sale_quantity").alias("total_quantity"),
        count("id").alias("order_count")
    ) \
    .orderBy("year", "month")

print("\n=== Writing to ClickHouse ===")

try:
    import clickhouse_connect
    
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='spark_user', password='spark123')
    
    client.command("CREATE DATABASE IF NOT EXISTS reports")
    
    client.command("DROP TABLE IF EXISTS reports.top_products")
    client.command("DROP TABLE IF EXISTS reports.revenue_by_category")
    client.command("DROP TABLE IF EXISTS reports.product_ratings")
    client.command("DROP TABLE IF EXISTS reports.top_customers")
    client.command("DROP TABLE IF EXISTS reports.customers_by_country")
    client.command("DROP TABLE IF EXISTS reports.monthly_trends")
    
    client.command("""
        CREATE TABLE reports.top_products (
            product_name String,
            product_category String,
            total_quantity Int64,
            total_revenue Float64
        ) ENGINE = MergeTree()
        ORDER BY total_revenue
    """)
    
    client.command("""
        CREATE TABLE reports.revenue_by_category (
            product_category String,
            total_revenue Float64
        ) ENGINE = MergeTree()
        ORDER BY total_revenue
    """)
    
    client.command("""
        CREATE TABLE reports.product_ratings (
            product_name String,
            product_rating Float64,
            product_reviews Int64
        ) ENGINE = MergeTree()
        ORDER BY product_rating
    """)
    
    client.command("""
        CREATE TABLE reports.top_customers (
            customer_email String,
            customer_first_name String,
            customer_last_name String,
            total_spent Float64,
            purchase_count Int64
        ) ENGINE = MergeTree()
        ORDER BY total_spent
    """)
    
    client.command("""
        CREATE TABLE reports.customers_by_country (
            customer_country String,
            customer_count Int64
        ) ENGINE = MergeTree()
        ORDER BY customer_count
    """)
    
    client.command("""
        CREATE TABLE reports.monthly_trends (
            year Int32,
            month Int32,
            total_revenue Float64,
            total_quantity Int64,
            order_count Int64
        ) ENGINE = MergeTree()
        ORDER BY (year, month)
    """)
    
    print("Inserting top_products...")
    data = [(row.product_name, row.product_category, row.total_quantity, row.total_revenue) for row in top_products.collect()]
    client.insert("reports.top_products", data, column_names=['product_name', 'product_category', 'total_quantity', 'total_revenue'])
    
    print("Inserting revenue_by_category...")
    data = [(row.product_category, row.total_revenue) for row in revenue_by_category.collect()]
    client.insert("reports.revenue_by_category", data, column_names=['product_category', 'total_revenue'])
    
    print("Inserting product_ratings...")
    data = [(row.product_name, row.product_rating, row.product_reviews) for row in product_ratings.collect()]
    client.insert("reports.product_ratings", data, column_names=['product_name', 'product_rating', 'product_reviews'])
    
    print("Inserting top_customers...")
    data = [(row.customer_email, row.customer_first_name, row.customer_last_name, row.total_spent, row.purchase_count) for row in top_customers.collect()]
    client.insert("reports.top_customers", data, column_names=['customer_email', 'customer_first_name', 'customer_last_name', 'total_spent', 'purchase_count'])
    
    print("Inserting customers_by_country...")
    data = [(row.customer_country, row.customer_count) for row in customers_by_country.collect() if row.customer_country]
    client.insert("reports.customers_by_country", data, column_names=['customer_country', 'customer_count'])
    
    print("Inserting monthly_trends...")
    data = [(row.year, row.month, row.total_revenue, row.total_quantity, row.order_count) for row in monthly_trends.collect()]
    client.insert("reports.monthly_trends", data, column_names=['year', 'month', 'total_revenue', 'total_quantity', 'order_count'])
    
    print("\nAll reports written to ClickHouse!")
    
except Exception as e:
    print(f"ClickHouse error: {e}")
    print("Reports were printed to console but not saved to ClickHouse.")

print("\nSaving reports to CSV...")
top_products.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/top_products")
revenue_by_category.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/revenue_by_category")
product_ratings.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/product_ratings")
top_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/top_customers")
customers_by_country.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/customers_by_country")
monthly_trends.coalesce(1).write.mode("overwrite").option("header", "true").csv("./spark_data/monthly_trends")

print("Reports saved to CSV in ./spark_data/")

spark.stop()
