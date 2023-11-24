-- Querying dim_date_times

/* Cast columns to types */
SELECT * FROM dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN timestamp TYPE VARCHAR(8);

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(6);

ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

/* Set primary key */
DELETE FROM dim_date_times WHERE date_uuid IS NULL;
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

/* Set foreign key */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_details
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);
