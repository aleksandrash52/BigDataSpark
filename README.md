# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark


## Описание работы

Реализован ETL-пайплайн с использованием Apache Spark, который трансформирует 
данные из CSV-файлов в модель "Звезда" в PostgreSQL, а затем формирует 
аналитические витрины в ClickHouse.


## Отчёты (витрины данных)

1. Продукты — топ-10 товаров по выручке, выручка по категориям, рейтинг продуктов
2. Клиенты — топ-10 клиентов по тратам, распределение по странам, средний чек
3. Время — месячные тренды продаж, средний размер заказа по месяцам
4. Магазины — топ-5 магазинов по выручке, средний чек по магазину
5. Поставщики — топ-5 поставщиков по выручке, средняя цена товаров
6. Качество продукции — продукты с высшим/низшим рейтингом, корреляция рейтинга 
   и продаж, топ по отзывам


## Алгоритм запуска

### 1. Запуск инфраструктуры

docker-compose up -d

### 2. Проверка загрузки данных в PostgreSQL

docker exec -it bigdata_spark_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM public.mock_data;"

Ожидаемый результат: 10000

### 3. Создание отчётов в ClickHouse

docker exec -it jupyter_lab2 spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/drivers/postgresql-42.7.3.jar \
  /home/jovyan/work/etl_to_clickhouse.py


## Проверка результатов

### PostgreSQL (модель "Звезда")

docker exec -it bigdata_spark_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM analytics.fact_sales;"


### ClickHouse (отчёты)

docker exec -it bigdata_spark_clickhouse clickhouse-client --user default --password clickhouse123 --query "SELECT * FROM reports.top_products LIMIT 10 FORMAT Pretty"


## Остановка и очистка

docker-compose down -v
