
-- Create schema
CREATE SCHEMA IF NOT EXISTS ALT_SCHOOL;


-- create and populate tables
create table if not exists ALT_SCHOOL.PRODUCTS
(
    id  serial primary key,
    name varchar not null,
    price numeric(10, 2) not null
);


COPY ALT_SCHOOL.PRODUCTS (id, name, price)
FROM '/data/products.csv' DELIMITER ',' CSV HEADER;

-- setup customers table following the example above

-- TODO: Provide the DDL statment to create this table ALT_SCHOOL.CUSTOMERS
-- DDL statment to create table ALT_SCHOOL.CUSTOMERS
CREATE TABLE IF NOT EXISTS ALT_SCHOOL.CUSTOMERS
(
    customer_id uuid NOT NULL PRIMARY KEY,
    device_id uuid NOT NULL,
    location varchar NOT NULL,
    currency varchar
);

-- TODO: provide the command to copy the customers data in the /data folder into ALT_SCHOOL.CUSTOMERS
-- Copy customers data
COPY ALT_SCHOOL.CUSTOMERS (customer_id, device_id, location, currency)
FROM '/data/customers.csv' DELIMITER ',' CSV HEADER;


-- TODO: complete the table DDL statement
-- Create orders table
CREATE TABLE IF NOT EXISTS ALT_SCHOOL.ORDERS
(
    order_id uuid NOT NULL PRIMARY KEY,
    customer_id uuid NOT NULL,
    status varchar NOT NULL,
    checked_out_at timestamp
);


-- provide the command to copy orders data into POSTGRES
-- command to copy the orders data in the /data folder into ALT_SCHOOL.ORDERS
-- Copy orders data
COPY ALT_SCHOOL.ORDERS (order_id, customer_id, status, checked_out_at)
FROM '/data/orders.csv' DELIMITER ',' CSV HEADER;

-- DDL statment to create table ALT_SCHOOL.LINE_ITEMS
-- Create line_items table
CREATE TABLE IF NOT EXISTS ALT_SCHOOL.LINE_ITEMS
(
    line_item_id bigint NOT NULL PRIMARY KEY,
    order_id uuid NOT NULL,
    item_id bigint NOT NULL,
    quantity bigint NOT NULL
);


-- provide the command to copy ALT_SCHOOL.LINE_ITEMS data into POSTGRES
-- Copy line_items data
COPY ALT_SCHOOL.LINE_ITEMS (line_item_id, order_id, item_id, quantity)
FROM '/data/line_items.csv' DELIMITER ',' CSV HEADER;

-- setup the events table following the examle provided
-- DDL statment to create table ALT_SCHOOL.EVENTS
-- Create events table
CREATE TABLE IF NOT EXISTS ALT_SCHOOL.EVENTS
(
    event_id bigint NOT NULL PRIMARY KEY,
    customer_id uuid NOT NULL,
    event_data jsonb NOT NULL,
    event_timestamp timestamp
);

-- command to copy the events data in the /data folder into ALT_SCHOOL.EVENTS
-- Copy events data
COPY ALT_SCHOOL.EVENTS (event_id, customer_id, event_data, event_timestamp)
FROM '/data/events.csv' DELIMITER ',' CSV HEADER;




