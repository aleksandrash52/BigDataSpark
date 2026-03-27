SELECT 
    (SELECT COUNT(*) FROM analytics.dim_customer) as customers_count,
    (SELECT COUNT(*) FROM analytics.dim_seller) as sellers_count,
    (SELECT COUNT(*) FROM analytics.dim_product) as products_count,
    (SELECT COUNT(*) FROM analytics.dim_store) as stores_count,
    (SELECT COUNT(*) FROM analytics.dim_supplier) as suppliers_count,
    (SELECT COUNT(*) FROM analytics.fact_sales) as sales_count;


SELECT 
    st.store_name,
    COUNT(f.sale_id) as sales_count,
    SUM(f.total_amount) as total_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_store st ON f.store_id = st.store_id
GROUP BY st.store_name
ORDER BY total_revenue DESC
LIMIT 10;


SELECT 
    c.first_name || ' ' || c.last_name as customer_name,
    c.customer_email,
    COUNT(f.sale_id) as purchases_count,
    SUM(f.total_amount) as total_spent
FROM analytics.fact_sales f
JOIN analytics.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_email
ORDER BY total_spent DESC
LIMIT 10;


SELECT 
    p.product_category,
    COUNT(f.sale_id) as sales_count,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_category
ORDER BY total_revenue DESC;


SELECT 
    c.pet_type,
    COUNT(DISTINCT c.customer_id) as customers_count,
    COUNT(f.sale_id) as purchases_count,
    SUM(f.total_amount) as total_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.pet_type
ORDER BY total_revenue DESC;