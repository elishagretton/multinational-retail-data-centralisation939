/* TASK 1 CHECK PREV TYPES
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
A. Cast columns of orders_table to new types */

SELECT * FROM orders_table;

-- Change data type of date_uuid column to UUID
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Change data type of user_uuid column to UUID
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Change data type of card_number column to VARCHAR(120123)
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);

-- Change data type of store_code column to VARCHAR(120123)
ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

-- Change data type of product_code column to VARCHAR(120123)
ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

-- Change data type of product_quantity column to SMALLINT
ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

/* TASK 2 CHECK PREV TYPES
+----------------+--------------------+--------------------+
| dim_user_table | current data type  | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR(255)       |
| last_name      | TEXT               | VARCHAR(255)       |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(?)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |

A. Cast columns of dim_users to new types */

SELECT * FROM dim_users;

-- Change data type of first_name column to VARCHAR(255)
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(14);

-- Change data type of last_name column to VARCHAR(255)
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(15);

-- Change data type of date_of_birth column to DATE
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;

-- Change data type of country_code column to VARCHAR(255)
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2);

-- Change data type of user_uuid column to UUID
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Change data type of join_date column to DATE
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::date;

/* TASK 3
A. There are two latitude columns in the store details table. 
   Using SQL, merge one of the columns into the other so you have one latitude column.
 - Already removed lat column in Python
 +---------------------+-------------------+------------------------+
| store_details_table | current data type |   required data type   |
+---------------------+-------------------+------------------------+
| longitude           | TEXT              | FLOAT                  |
| locality            | TEXT              | VARCHAR(255)           |
| store_code          | TEXT              | VARCHAR(?)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
| latitude            | TEXT              | FLOAT                  |
| country_code        | TEXT              | VARCHAR(?)             |
| continent           | TEXT              | VARCHAR(255)           |
+---------------------+-------------------+------------------------+

B. Cast columns of dim_store_details to new types */

SELECT * FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(20);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(11);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::date;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(11);
ALTER TABLE dim_store_details
ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(7);

SELECT * FROM dim_store_details;

/* C. There is a row that represents the business's website .
 - Change the location column values where they're null to N/A. */

-- Find row
SELECT *
FROM dim_store_details
WHERE store_type = 'Web Portal';

-- Change location column values
UPDATE dim_store_details
SET locality = NULL,
    country_code = NULL,
	continent = NULL
WHERE store_type = 'Web Portal' AND (locality = 'N/A' OR country_code = 'GB' OR continent = 'Europe');

/* TASK 4 INCOMPLETE
A. Remove from product_price column
*/
-- DONE: STARTING TYPES CORRECTED
SELECT * FROM dim_products;

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';

/* B. Create weight_class column */
-- all set to correct start types
SELECT * FROM dim_products;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

-- !!! TO FIX: WEIGHT CONVERSION IS NOT WORKING PROPERLY!!!!
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::double precision;

UPDATE dim_products
SET weight_class = CASE
    WHEN new_weight < 2 THEN 'Light'
    WHEN new_weight >= 2 AND new_weight < 40 THEN 'Mid_Sized'
	WHEN new_weight >= 40 AND new_weight < 140 THEN 'Heavy'
	WHEN new_weight >= 140 THEN 'Truck_Required'
END;

/* TASK 5 INCOMPLETE
A. Rename removed column to still_available
*/
SELECT weight, new_weight FROM dim_products WHERE weight = '440g';

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

/* B. Set data types. */

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;

-- TO FIX: fix weight 
-- ERROR:  invalid input syntax for type double precision: "9GO9NZ5JTL"

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::double precision;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(13);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::date;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'true' THEN TRUE ELSE FALSE END;

-- to do: fix prev and then run this
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);

/* TASK 6 COMPLETE
A. Set data types

Now update the date table with the correct types:

+-----------------+-------------------+--------------------+
| dim_date_times  | current data type | required data type |
+-----------------+-------------------+--------------------+
| month           | TEXT              | VARCHAR(?)         |
| year            | TEXT              | VARCHAR(?)         |
| day             | TEXT              | VARCHAR(?)         |
| time_period     | TEXT              | VARCHAR(?)         |
| date_uuid       | TEXT              | UUID   
*/

SELECT * FROM dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN timestamp TYPE VARCHAR(8);

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

/* TASK 7 COMPLETE
Now we need to update the last table for the card details.

Make the associated changes after finding out what the lengths of each variable should be:

+------------------------+-------------------+--------------------+
|    dim_card_details    | current data type | required data type |
+------------------------+-------------------+--------------------+
| card_number            | TEXT              | VARCHAR(?)         |
| expiry_date            | TEXT              | VARCHAR(?)         |
| date_payment_confirmed | TEXT              | DATE               |
+------------------------+-------------------+--------------------+
*/

SELECT * FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;
