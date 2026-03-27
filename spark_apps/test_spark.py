from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test Spark") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

print("Spark version:", spark.version)

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="public.mock_data", properties=properties)

print("Total rows in mock_data:", df.count())
print("\nFirst 5 rows:")
df.show(5)

print("\nRevenue by product category:")
revenue_by_category = df.groupBy("product_category") \
    .sum("sale_total_price") \
    .withColumnRenamed("sum(sale_total_price)", "total_revenue")

revenue_by_category.show()

spark.stop()