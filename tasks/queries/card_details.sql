-- Querying dim_card_details

/* Cast to types
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
ALTER COLUMN card_number TYPE VARCHAR(22);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(19);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

/* Set primary keys */
DELETE FROM dim_card_details WHERE card_number IS NULL;
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

/* Set foreign key */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);