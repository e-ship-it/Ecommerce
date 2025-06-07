psql --version;
psql -U postgres
postgres=# CREATE USER DEVELOPER WITH PASSWORD 'devTeam';
postgres=# \du -- To list all the users
postgres=# SELECT * FROM pg_roles WHERE UPPER(rolname) = 'DEVELOPER';
postgres=# CREATE DATABASE INGESTION;
postgres=# GRANT CONNECT ON DATABASE INGESTION TO DEVELOPER;
postgres=# GRANT USAGE ON SCHEMA PUBLIC TO DEVELOPER;

postgres=# CREATE SCHEMA "STREAMING DATA"; 
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
postgres=# GRANT USAGE ON SCHEMA streams TO DEVELOPER;
postgres=# GRANT USAGE ON SCHEMA batch TO DEVELOPER;
-- Tables Created

CREATE TABLE batch.users (
    user_id INT PRIMARY KEY,                         -- Unique user ID
    name VARCHAR(255),                                -- User's full name
    email VARCHAR(255) UNIQUE NOT NULL,               -- User's email (unique and not null)
    birthdate DATE,                                   -- User's birthdate
    registration_date DATE,                           -- User's registration date
    address TEXT,                                     -- User's address (can be longer than VARCHAR)
    phone_number VARCHAR(50)                         -- User's phone number (supports format with extensions)
);

CREATE TABLE batch.aisles (
    aisle_id INT PRIMARY KEY,  
    aisle TEXT NOT NULL
);

CREATE TABLE batch.departments (
    department_id INT PRIMARY KEY,  
    department TEXT NOT NULL 
);

CREATE TABLE batch.products (
    product_id INT PRIMARY KEY,            -- Primary key for products table
    product_name TEXT NOT NULL,            -- Name of the product
    aisle_id INT,                          -- Foreign key referencing aisles
    department_id INT,                     -- Foreign key referencing departments
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
CONSTRAINT fk_users FOREIGN KEY (user_id) REFERENCES batch.users (user_id)  ON DELETE CASCADE
);

CREATE TABLE streams.order_products (
    order_id INT, 
    product_id INT, 
    add_to_cart_order INT,               -- Position in the cart
    reordered INT,                        -- Whether the product was reordered (1 for yes, 0 for no)
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES streams.orders(order_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES batch.products(product_id)
);

-- Check the tables created
ingestion=# \dt batch.*
ingestion=# \dt streams.*

ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE streams.orders TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE streams.order_products TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.aisles TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.departments TO developer;
ingestion=# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE batch.products TO developer;
--ingestion=# GRANT SELECT ON ALL TABLES IN SCHEMA batch TO DEVELOPER;
--ingestion=# GRANT SELECT ON ALL TABLES IN SCHEMA streams TO DEVELOPER;

--To disconnect from the current session in psql, use the \q command:


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


ALTER TABLE streams.orders
ALTER COLUMN order_id SET DATA TYPE BIGINT;
ALTER TABLE streams.orders
ALTER COLUMN order_hour_of_day SET DATA TYPE INT;
ALTER TABLE streams.orders
ADD COLUMN days_since_prior_order FLOAT;