-- Querying dim_products
 
SELECT * FROM dim_products;

/* Remove '£' frm product_price */
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';

/* Create weight_class column */
SELECT * FROM dim_products;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::double precision;

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
	WHEN weight >= 140 THEN 'Truck_Required'
END;

/* Rename removed column to still_available */
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

/* Set data types. */
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::double precision;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(22);

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

/* Set primary key */
DELETE FROM dim_products WHERE product_code IS NULL;
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

/* Set foreign key */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_codes
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);