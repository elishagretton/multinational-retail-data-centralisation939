-- Querying dim_store_details

/* Cast columns to new types 
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
*/
SELECT * FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING NULLIF(staff_numbers, '')::SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::date;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);
ALTER TABLE dim_store_details
ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

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


/* Set primary key */
DELETE FROM dim_store_details WHERE store_code IS NULL;
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

/* Set foreign key */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_details
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

SELECT * FROM dim_store_details;