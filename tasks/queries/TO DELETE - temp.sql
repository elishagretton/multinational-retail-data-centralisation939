DROP TABLE dim_users CASCADE;




/* TASK 4 
A. Remove from product_price column
*/
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
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
	WHEN weight >= 140 THEN 'Truck_Required'
END;

/* TASK 5 
A. Rename removed column to still_available
*/

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

/* B. Set data types. */

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;

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


/* TASK 5 
Setting primary keys 
*/



DELETE FROM dim_date_times WHERE date_uuid IS NULL;
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);



DELETE FROM dim_card_details WHERE card_number IS NULL;
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

DELETE FROM dim_store_details WHERE store_code IS NULL;
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

DELETE FROM dim_products WHERE product_code IS NULL;
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

/* TASK 6: Set foreign keys in orders_table 
- date_uuid: dim_date_times
- user_uuid: dim_users
- card_number: dim_card_details
- store_code: dim_store_details
- product_code: dim_products */

-- Assuming 'date_uuid' is the primary key in 'dim_date_times'
-- 'date_uuid' is the foreign key in 'orders_table'
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

-- DONE


-- TO FIX: CARD_NUMBER NOT IN DIM_CARD_DETAILS
-- NEED TO ADD INTO
/* ERROR:  insert or update on table "orders_table" violates foreign key constraint "fk_orders_card_details"
DETAIL:  Key (card_number)=(4971858637664481) is not present in table "dim_card_details".
SQL state: 23503 */
INSERT INTO dim_card_details (card_number)
VALUES ('4971858637664481');

INSERT INTO dim_card_details (card_number)
VALUES ('4222069242355461965');

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

SELECT * FROM orders_table WHERE card_number = '4971858637664481';
SELECT * FROM dim_card_details
-- TO FIX: OTHER Table: - store_code: dim_store_details
/* ERROR:  insert or update on table "orders_table" violates foreign key constraint "fk_orders_store_details"
DETAIL:  Key (store_code)=(WEB-1388012W) is not present in table "dim_store_details".
SQL state: 23503 */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_details
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

-- NEXT: - product_code: dim_products
/* ERROR:  insert or update on table "orders_table" violates foreign key constraint "fk_orders_product_codes"
DETAIL:  Key (product_code)=(g5-411324A) is not present in table "dim_products".
SQL state: 23503 */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_codes
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);