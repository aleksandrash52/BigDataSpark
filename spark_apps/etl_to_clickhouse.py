from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg, col, desc, year, month, countDistinct, round as spark_round
import socket

try:
    driver_host = socket.gethostbyname('jupyter_lab2')
except:
    driver_host = "localhost"

spark = SparkSession.builder \
    .appName("ETL to ClickHouse from Star Schema") \
    .config("spark.driver.host", driver_host) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

# Подключение к PostgreSQL
jdbc_url = "jdbc:postgresql://172.19.0.5:5432/postgres"
db_params = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

print("Загрузка модели 'Звезда' из PostgreSQL...")
fact_sales = spark.read.jdbc(url=jdbc_url, table="analytics.fact_sales", properties=db_params)
dim_customer = spark.read.jdbc(url=jdbc_url, table="analytics.dim_customer", properties=db_params)
dim_product = spark.read.jdbc(url=jdbc_url, table="analytics.dim_product", properties=db_params)
dim_store = spark.read.jdbc(url=jdbc_url, table="analytics.dim_store", properties=db_params)
dim_supplier = spark.read.jdbc(url=jdbc_url, table="analytics.dim_supplier", properties=db_params)

print(f"Фактов продаж: {fact_sales.count()}")

df_product_sales = fact_sales.join(dim_product, "product_id")

# Топ-10 продуктов по выручке
top_products = df_product_sales.groupBy("product_name", "product_category") \
    .agg(
        _sum("quantity").alias("total_quantity"),
        _sum("total_amount").alias("total_revenue")
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

# Выручка по категориям
revenue_by_category = df_product_sales.groupBy("product_category") \
    .agg(_sum("total_amount").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))

# Рейтинг и отзывы по продуктам
product_ratings = df_product_sales.select("product_name", "product_rating", "product_reviews") \
    .distinct() \
    .orderBy(desc("product_rating"))

df_customer_sales = fact_sales.join(dim_customer, "customer_id")

top_customers = df_customer_sales.groupBy("customer_id", "first_name", "last_name", "customer_email", "country") \
    .agg(
        _sum("total_amount").alias("total_spent"),
        count("sale_id").alias("purchases_count"),
        spark_round(avg("total_amount"), 2).alias("avg_check")  # средний чек
    ) \
    .orderBy(desc("total_spent")) \
    .limit(10)

# Распределение клиентов по странам
customers_by_country = df_customer_sales.groupBy("country") \
    .agg(countDistinct("customer_id").alias("customers_count")) \
    .orderBy(desc("customers_count"))


monthly_trends = fact_sales.withColumn("year", year("sale_date")) \
    .withColumn("month", month("sale_date")) \
    .groupBy("year", "month") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _sum("quantity").alias("total_quantity"),
        count("sale_id").alias("order_count"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value")  # средний чек заказа
    ) \
    .orderBy("year", "month")


df_store_sales = fact_sales.join(dim_store, "store_id")

top_stores = df_store_sales.groupBy("store_name", "store_city", "store_country") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        count("sale_id").alias("sales_count"),
        spark_round(avg("total_amount"), 2).alias("avg_check")  # средний чек по магазину
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(5)


df_supplier_sales = fact_sales.join(dim_product, "product_id").join(dim_supplier, "supplier_id")

top_suppliers = df_supplier_sales.groupBy("supplier_name", "supplier_country") \
    .agg(
        _sum("total_amount").alias("total_revenue"),
        spark_round(avg("product_price"), 2).alias("avg_product_price"),
        count("sale_id").alias("sales_count")
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(5)


product_quality = df_product_sales.groupBy("product_id", "product_name", "product_rating") \
    .agg(
        _sum("quantity").alias("total_sold"),
        count("sale_id").alias("num_transactions")
    )

# Продукты с наивысшим рейтингом
top_rated = product_quality.filter(col("product_rating").isNotNull()) \
    .orderBy(desc("product_rating")) \
    .limit(5)

# Продукты с наименьшим рейтингом
lowest_rated = product_quality.filter(col("product_rating").isNotNull()) \
    .orderBy("product_rating") \
    .limit(5)

# Корреляция между рейтингом и объёмом продаж
correlation_df = product_quality.filter(col("product_rating").isNotNull()) \
    .select("product_rating", "total_sold")
correlation = correlation_df.stat.corr("product_rating", "total_sold")
print(f"Корреляция рейтинга с объёмом продаж: {correlation:.4f}")

# Продукты с наибольшим количеством отзывов
df_raw_product = df_product_sales.select("product_name", "product_reviews").distinct()
most_reviewed = df_raw_product.orderBy(desc("product_reviews")).limit(5)


try:
    import clickhouse_connect
    client = clickhouse_connect.get_client(host='bigdata_spark_clickhouse', port=8123, username='default', password='clickhouse123')
    
    client.command("CREATE DATABASE IF NOT EXISTS reports")
    
    # Удаляем старые таблицы
    tables = ["top_products", "revenue_by_category", "product_ratings", 
              "top_customers", "customers_by_country", "monthly_trends",
              "top_stores", "top_suppliers", "top_rated_products", 
              "lowest_rated_products", "most_reviewed_products", "rating_correlation"]
    for t in tables:
        client.command(f"DROP TABLE IF EXISTS reports.{t}")
    
    # Создаём таблицы
    client.command("""
        CREATE TABLE reports.top_products (
            product_name String, product_category String,
            total_quantity Int64, total_revenue Float64
        ) ENGINE = MergeTree() ORDER BY total_revenue
    """)
    
    client.command("""
        CREATE TABLE reports.top_customers (
            customer_id Int64, first_name String, last_name String,
            customer_email String, country String, total_spent Float64,
            purchases_count Int64, avg_check Float64
        ) ENGINE = MergeTree() ORDER BY total_spent
    """)
    
    client.command("""
        CREATE TABLE reports.monthly_trends (
            year Int32, month Int32, total_revenue Float64,
            total_quantity Int64, order_count Int64, avg_order_value Float64
        ) ENGINE = MergeTree() ORDER BY (year, month)
    """)
    
    client.command("""
        CREATE TABLE reports.top_stores (
            store_name String, store_city String, store_country String,
            total_revenue Float64, sales_count Int64, avg_check Float64
        ) ENGINE = MergeTree() ORDER BY total_revenue
    """)
    
    client.command("""
        CREATE TABLE reports.rating_correlation (
            metric_name String, value Float64
        ) ENGINE = MergeTree() ORDER BY metric_name
    """)
    
    # Вставляем корреляцию
    client.insert("reports.rating_correlation", [("pearson_correlation_rating_vs_sales", correlation)],
                  column_names=['metric_name', 'value'])
    
    # Вставка остальных данных
    data_top_products = [(row.product_name, row.product_category, row.total_quantity, row.total_revenue) 
                         for row in top_products.collect()]
    client.insert("reports.top_products", data_top_products,
                  column_names=['product_name', 'product_category', 'total_quantity', 'total_revenue'])
    
    print("Все отчёты успешно записаны в ClickHouse!")
    
except Exception as e:
    print(f"Ошибка ClickHouse: {e}")
    print("Отчёты сохранены в CSV (см. spark_data/)")

# Сохраняем в CSV как резервную копию
top_products.coalesce(1).write.mode("overwrite").option("header", "true").csv("/home/jovyan/work/spark_data/top_products")
top_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv("/home/jovyan/work/spark_data/top_customers")
monthly_trends.coalesce(1).write.mode("overwrite").option("header", "true").csv("/home/jovyan/work/spark_data/monthly_trends")
top_stores.coalesce(1).write.mode("overwrite").option("header", "true").csv("/home/jovyan/work/spark_data/top_stores")

spark.stop()
