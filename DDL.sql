CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_email VARCHAR(255) UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INT,
    country VARCHAR(100),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100)
);


CREATE TABLE analytics.dim_seller (
    seller_id SERIAL PRIMARY KEY,
    seller_email VARCHAR(255) UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(50)
);



CREATE TABLE analytics.dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    product_brand VARCHAR(100),
    product_price NUMERIC(10,2),
    product_category VARCHAR(100),
    product_weight NUMERIC(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_material VARCHAR(100)
);



CREATE TABLE analytics.dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255) UNIQUE,
    store_location VARCHAR(100),
    store_city VARCHAR(100),
    store_state VARCHAR(50),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(255)
);



CREATE TABLE analytics.dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) UNIQUE,
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);



CREATE TABLE analytics.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES analytics.dim_customer(customer_id),
    seller_id INT REFERENCES analytics.dim_seller(seller_id),
    product_id INT REFERENCES analytics.dim_product(product_id),
    store_id INT REFERENCES analytics.dim_store(store_id),
    supplier_id INT REFERENCES analytics.dim_supplier(supplier_id),
    sale_date DATE,
    quantity INT,
    total_amount NUMERIC(10,2)
);