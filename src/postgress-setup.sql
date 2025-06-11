psql --version;
psql -U postgres
postgres=# CREATE USER DEVELOPER WITH PASSWORD 'my_password';
postgres=# \du -- To list all the users
postgres=# SELECT * FROM pg_roles WHERE UPPER(rolname) = 'DEVELOPER';
postgres=# CREATE DATABASE INGESTION;
postgres=# GRANT CONNECT ON DATABASE INGESTION TO DEVELOPER;
postgres=# GRANT USAGE ON SCHEMA PUBLIC TO DEVELOPER;
GRANT CONNECT ON DATABASE ingestion TO developer;
GRANT CREATE ON DATABASE ingestion TO developer;
GRANT TEMPORARY ON DATABASE ingestion TO developer;

GRANT CONNECT ON DATABASE ingestion TO developer;
GRANT CREATE ON DATABASE ingestion TO developer;
GRANT TEMPORARY ON DATABASE ingestion TO developer;
SELECT current_user;

-- postgres=# GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA PUBLIC TO DEVELOPER;
-- postgres=# GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE <schema.table> TO DEVELOPER;

--Database is like a container and Schema is like a folder that group objects within them. 
postgres=# \list -- To list the databases
postgres=# SELECT * FROM pg_database; -- To list the databases
postgres=# SELECT * FROM information_schema.schemata; -- schema info
postgres=# SELECT schema_name FROM information_schema.schemata; -- To list down schema Name
postgres=# \dn -- To list down schema Name (here in postgres Schema = namespace that holds tables,views and other data objects)

postgres=# \c ingestion; --connect to db (only one connect per session)
ingestion=# CREATE SCHEMA streams; --CREATE SCHEMA for db
ingestion=# CREATE SCHEMA batch;
postgres=# CREATE SCHEMA opts_hub;
postgres=# CREATE SCHEMA mart;
postgres=# CREATE SCHEMA staging;
GRANT USAGE ON SCHEMA streams TO DEVELOPER;
GRANT USAGE ON SCHEMA batch TO DEVELOPER;
GRANT USAGE ON SCHEMA opts_hub TO DEVELOPER;
GRANT USAGE ON SCHEMA mart TO DEVELOPER;
GRANT USAGE ON SCHEMA staging TO DEVELOPER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO DEVELOPER;
GRANT USAGE ON SCHEMA public TO DEVELOPER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO DEVELOPER;

-- Tables Created

CREATE TABLE batch.users (
    user_id INT PRIMARY KEY,                         -- Unique user ID
    name VARCHAR(255),                                -- User's full name
    email VARCHAR(255) UNIQUE NOT NULL,               -- User's email (unique and not null)
    birthdate DATE,                                   -- User's birthdate
    registration_date DATE,                           -- User's registration date
    address TEXT,                                     -- User's address (can be longer than VARCHAR)
    phone_number VARCHAR(50),                         -- User's phone number (supports format with extensions)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Created timestamp, auto set to current time
    updated_at TIMESTAMP
);

CREATE TABLE batch.aisles (
    aisle_id INT PRIMARY KEY,  
    aisle TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Created timestamp, auto set to current time
    updated_at TIMESTAMP
);

CREATE TABLE batch.departments (
    department_id INT PRIMARY KEY,  
    department TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Created timestamp, auto set to current time
    updated_at TIMESTAMP
);

CREATE TABLE batch.products (
    product_id INT PRIMARY KEY,            -- Primary key for products table
    product_name TEXT NOT NULL,            -- Name of the product
    aisle_id INT,                          -- Foreign key referencing aisles
    department_id INT,                     -- Foreign key referencing departments
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Created timestamp, auto set to current time
    updated_at TIMESTAMP,
    CONSTRAINT fk_aisles FOREIGN KEY (aisle_id) REFERENCES batch.aisles(aisle_id),
    CONSTRAINT fk_departments FOREIGN KEY (department_id) REFERENCES batch.departments(department_id)
);

CREATE TABLE streams.orders(
order_id BIGINT PRIMARY KEY, -- Unique order ID
user_id INT NOT NULL, -- ID of the user placing the order
eval_set TEXT, -- 'train', 'prior', etc. (evaluation set)
order_number INT, -- Order number for a user
order_dow INT, -- Hour of the day as FLOAT
order_hour_of_day INT CHECK(order_hour_of_day >= 0 AND order_hour_of_day < 24), -- Days since the prior order,
days_since_prior_order FLOAT,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Created timestamp, auto set to current time
updated_at TIMESTAMP,
CONSTRAINT fk_users FOREIGN KEY (user_id) REFERENCES batch.users (user_id)  ON DELETE CASCADE
);

CREATE TABLE streams.user_order_activity_stream (
    order_id INT, 
    product_id INT, 
    add_to_cart_order INT,               -- Position in the cart
    reordered INT,                        -- Whether the product was reordered (1 for yes, 0 for no)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Created timestamp, auto set to current time
    updated_at TIMESTAMP,
    CONSTRAINT pk_uoas PRIMARY KEY (order_id,product_id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES streams.orders(order_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES batch.products(product_id)
);
--ALTER TABLE streams.user_order_activity_stream
--ADD CONSTRAINT pk_uoas PRIMARY KEY (order_id,product_id);

-- Check the tables created
ingestion=# \dt batch.*
ingestion=# \dt streams.*

ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE streams.orders TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE streams.user_order_activity_stream TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.aisles TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.departments TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.products TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.users TO developer; 
--ingestion=# GRANT SELECT ON ALL TABLES IN SCHEMA batch TO DEVELOPER;
--ingestion=# GRANT SELECT ON ALL TABLES IN SCHEMA streams TO DEVELOPER;

--To disconnect from the current session in psql, use the \q command:

--Added Audit columns for STAGING
--ALTER TABLE batch.users
--ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
--ALTER TABLE batch.users
--ADD COLUMN updated_at TIMESTAMP ;


SELECT * FROM information_schema.columns WHERE table_name = 'orders';
SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    character_maximum_length,  
    numeric_precision, 
    numeric_scale
    FROM information_schema.columns WHERE table_name = 'orders';

--Verify Privileges:
--You can verify the permissions granted to the user with:
\dp streams.orders

--ALTER TABLE streams.order_products RENAME TO user_order_activity_stream
--ALTER TABLE streams.orders
--ALTER COLUMN order_id SET DATA TYPE BIGINT;
--ALTER TABLE streams.orders
--ALTER COLUMN order_hour_of_day SET DATA TYPE INT;
--ALTER TABLE streams.orders
--ADD COLUMN days_since_prior_order FLOAT;
-- Does the user have usage on schema?
SELECT n.nspname AS schema,
       r.rolname AS role,
       has_schema_privilege(r.rolname, n.nspname, 'USAGE') AS usage,
       has_schema_privilege(r.rolname, n.nspname, 'CREATE') AS can_create
FROM pg_namespace n
JOIN pg_roles r ON r.rolname = 'developer'
WHERE n.nspname IN ('batch', 'staging', 'streams','opts_hub');

-- Grant CREATE privileges so dbt can write to these schemas:
GRANT CREATE ON SCHEMA batch TO developer;
GRANT CREATE ON SCHEMA staging TO developer;
GRANT CREATE ON SCHEMA streams TO developer;

GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA batch TO developer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO developer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA streams TO developer;

-- PERMISSION if the schema is not the default target schema in dbt
GRANT CREATE ON SCHEMA opts_hub TO developer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA opts_hub TO developer;
GRANT USAGE ON SCHEMA opts_hub TO developer;
GRANT CREATE ON SCHEMA opts_hub TO developer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA opts_hub TO developer;
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE opts_hub.dim_user TO developer;
ALTER TABLE opts_hub.dim_user OWNER TO developer;
--To check the current encoding in PostgreSQL:
ingestion=# SHOW server_encoding;
CREATE DATABASE your_db_name ENCODING='UTF8';

-- Explicit Encoding Conversion During Data Ingestion
--UPDATE your_table
--SET your_column = convert(your_column, 'WIN1252', 'UTF8')
--WHERE your_column LIKE '%some problematic character%';

--Ensure that your PostgreSQL client is also set to use UTF-8 for input/output:
SHOW client_encoding;
SET client_encoding = 'UTF8';
--psql -U your_user -d your_db --set=client_encoding=UTF8

  




-- As dbt is not a database engine, just a SQL transformation tool
--Auto-increment behavior is a database feature, not a SQL standard
--dbt models generate SQL queries but don’t control how the database stores or manages the data internally.
--dbt incremental models generate INSERT or MERGE SQL to add or update data.
--Hence create dimension tables with surrogate keys in your warehouse ahead of time.
--No, it’s not possible to create an auto-incrementing ID in dbt. In fact, auto-incrementing keys in general are a bad idea.
--CREATE TABLE opts_hub.dim_user (
--    dim_user_id SERIAL PRIMARY KEY,
--    user_id BIGINT NOT NULL,
--    user_hkey VARCHAR NOT NULL UNIQUE,
--    user_name VARCHAR,
--    email VARCHAR,
--    phone_number VARCHAR,
--    address VARCHAR,
--    birthdate DATE,
--    registration_date DATE,
--    valid_from TIMESTAMP NOT NULL,
--    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31'
--);
