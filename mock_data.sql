CREATE TABLE IF NOT EXISTS public.mock_data (
    id INT,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INT,
    customer_email VARCHAR(255),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(255),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(50),
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_price NUMERIC(10,2),
    product_quantity INT,
    sale_date DATE,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price NUMERIC(10,2),
    store_name VARCHAR(255),
    store_location VARCHAR(100),
    store_city VARCHAR(100),
    store_state VARCHAR(50),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(255),
    pet_category VARCHAR(100),
    product_weight NUMERIC(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);


COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (1).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (2).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (3).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (4).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (5).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (6).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (7).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (8).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (9).csv' DELIMITER ',' CSV HEADER;
COPY public.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA.csv' DELIMITER ',' CSV HEADER;


SELECT COUNT(*) FROM public.mock_data;