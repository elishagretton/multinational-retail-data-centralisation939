-- Querying orders_tables

/* Cast columns to new types
+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+
*/

SELECT * FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(22);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

/* Finding primary keys */

WITH order_columns AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'orders_table'
),
dim_columns AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name LIKE 'dim\_%' ESCAPE '\'
)
SELECT column_name
FROM order_columns
WHERE column_name NOT LIKE 'dim\_%' ESCAPE '\'
  AND column_name NOT IN (SELECT column_name FROM dim_columns);
  
-- View column names
SELECT * FROM orders_table;

/* Primary keys and corresponding tables to set:
- date_uuid: dim_date_times
- user_uuid: dim_users
- card_number: dim_card_details
- store_code: dim_store_details
- product_code: dim_products */
