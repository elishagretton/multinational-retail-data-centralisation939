-- Querying dim_users table

/* Cast columns to new types
+----------------+--------------------+--------------------+
| dim_user_table | current data type  | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR(255)       |
| last_name      | TEXT               | VARCHAR(255)       |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(?)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |
*/

SELECT * FROM dim_users;

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;

ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::date;

/* Set primary key */
DELETE FROM dim_users WHERE user_uuid IS NULL;
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

/* Set foreign key */
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user_uuids
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);