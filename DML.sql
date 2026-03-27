INSERT INTO analytics.dim_customer (
    customer_email, first_name, last_name, age, country, 
    postal_code, pet_type, pet_name, pet_breed
)
SELECT DISTINCT
    customer_email,
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM public.mock_data
ON CONFLICT (customer_email) DO NOTHING;



INSERT INTO analytics.dim_seller (
    seller_email, first_name, last_name, country, postal_code
)
SELECT DISTINCT
    seller_email,
    seller_first_name,
    seller_last_name,
    seller_country,
    seller_postal_code
FROM public.mock_data
ON CONFLICT (seller_email) DO NOTHING;



INSERT INTO analytics.dim_product (
    product_name, product_brand, product_price, product_category,
    product_weight, product_color, product_size, product_material
)
SELECT DISTINCT
    product_name,
    product_brand,
    product_price,
    product_category,
    product_weight,
    product_color,
    product_size,
    product_material
FROM public.mock_data;



INSERT INTO analytics.dim_store (
    store_name, store_location, store_city, store_state, 
    store_country, store_phone, store_email
)
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM public.mock_data
ON CONFLICT (store_name) DO NOTHING;



INSERT INTO analytics.dim_supplier (
    supplier_name, supplier_contact, supplier_email, supplier_phone,
    supplier_address, supplier_city, supplier_country
)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM public.mock_data
ON CONFLICT (supplier_name) DO NOTHING;



INSERT INTO analytics.fact_sales (
    customer_id, seller_id, product_id, store_id, supplier_id,
    sale_date, quantity, total_amount
)
SELECT
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    sup.supplier_id,
    m.sale_date,
    m.sale_quantity,
    m.sale_total_price
FROM public.mock_data m
JOIN analytics.dim_customer c ON m.customer_email = c.customer_email
JOIN analytics.dim_seller s ON m.seller_email = s.seller_email
JOIN analytics.dim_product p ON m.product_name = p.product_name 
    AND m.product_brand = p.product_brand 
    AND m.product_price = p.product_price
    AND m.product_category = p.product_category
    AND m.product_color = p.product_color
    AND m.product_size = p.product_size
JOIN analytics.dim_store st ON m.store_name = st.store_name
JOIN analytics.dim_supplier sup ON m.supplier_name = sup.supplier_name;