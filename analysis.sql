SELECT * FROM public.mock_data LIMIT 10;

SELECT COUNT(DISTINCT id) FROM public.mock_data; 


SELECT COUNT(DISTINCT sale_customer_id) AS customers_count FROM public.mock_data;
SELECT COUNT(DISTINCT sale_seller_id) AS sellers_count FROM public.mock_data;
SELECT COUNT(DISTINCT sale_product_id) AS products_count FROM public.mock_data;
SELECT COUNT(DISTINCT store_name) AS stores_count FROM public.mock_data;
SELECT COUNT(DISTINCT supplier_name) AS suppliers_count FROM public.mock_data;


SELECT DISTINCT product_category FROM public.mock_data;
SELECT DISTINCT pet_category FROM public.mock_data;

SELECT COUNT(*) FROM public.mock_data;

SELECT MIN(id), MAX(id) FROM public.mock_data;

SELECT COUNT(DISTINCT id) FROM public.mock_data;

SELECT id, customer_first_name, customer_last_name, sale_date
FROM public.mock_data
ORDER BY id DESC
LIMIT 10;

SELECT COUNT(DISTINCT customer_email) FROM public.mock_data;

SELECT 
    (id - 1) / 1000 + 1 AS file_number,
    COUNT(*) AS rows_per_file
FROM public.mock_data
GROUP BY file_number
ORDER BY file_number;

SELECT DISTINCT id FROM public.mock_data ORDER BY id LIMIT 20;

SELECT 
    CASE 
        WHEN id BETWEEN 1 AND 100 THEN '1-100'
        WHEN id BETWEEN 101 AND 200 THEN '101-200'
        WHEN id BETWEEN 201 AND 300 THEN '201-300'
        WHEN id BETWEEN 301 AND 400 THEN '301-400'
        WHEN id BETWEEN 401 AND 500 THEN '401-500'
        WHEN id BETWEEN 501 AND 600 THEN '501-600'
        WHEN id BETWEEN 601 AND 700 THEN '601-700'
        WHEN id BETWEEN 701 AND 800 THEN '701-800'
        WHEN id BETWEEN 801 AND 900 THEN '801-900'
        ELSE '901-1000'
    END AS id_range,
    COUNT(*) as count
FROM public.mock_data
GROUP BY id_range
ORDER BY id_range;

SELECT COUNT(DISTINCT sale_customer_id) FROM public.mock_data;

SELECT 
    sale_customer_id,
    COUNT(DISTINCT customer_email) as emails_count
FROM public.mock_data
GROUP BY sale_customer_id
HAVING COUNT(DISTINCT customer_email) > 1
LIMIT 10;