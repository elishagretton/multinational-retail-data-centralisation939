-- Generating data-driven insights from the tables
-- TO FIX: Task 7 + 9

-- Task 1: How many stores does the business have and what countries do they operate in?

SELECT
    country_code,
    COUNT(*) AS total_no_stores
FROM
    dim_store_details
WHERE
    country_code IN ('GB', 'DE', 'US')
GROUP BY
    country_code
ORDER BY
    total_no_stores DESC;
	
-- Task 2: Which locations have the most stores?

SELECT
    locality,
    COUNT(*) AS total_no_of_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_of_stores DESC
LIMIT 7;

-- Task 3: Which month produced the largest number of sales?

SELECT 
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    dim_date_times.month
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    dim_date_times.month
ORDER BY 
    total_sales DESC
LIMIT 6;

-- Task 4: How many sales are coming from online?

SELECT
    COUNT(orders_table.product_quantity) AS numbers_of_sales,
    SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE
        WHEN dim_store_details.locality IS NULL THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    location
ORDER BY
    numbers_of_sales;
	
-- Task 5: What percentage of sales come through each type of store?

SELECT
    dim_store_details.store_type AS store_type,
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    ROUND((SUM(orders_table.product_quantity * dim_products.product_price) /
        (SELECT 
            SUM(orders_table.product_quantity * dim_products.product_price)
        FROM 
            orders_table
        JOIN dim_products ON orders_table.product_code = dim_products.product_code) * 100)::numeric, 2) 
    AS "percentage_total(%)"
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    store_type
ORDER BY
    "percentage_total(%)" DESC;
	
-- Task 6: Which month in each year produced the highest cost of sales?

SELECT
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    dim_date_times.year AS year,
    dim_date_times.month AS month
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    year, month
ORDER BY
    total_sales DESC
LIMIT 10;

-- Task 7: What is our staff headcount?
/* TO FIX: 
- 325 are NULL
- when I went back to the code and extracted original data
- and did store_data.country_code.unique(): I got 'GB', 'DE', 'US', 'NULL' and other long strings that are not GB
- no occurence of 'GGB' even though I replaced this to 'GB' in data_cleaning code...
- not sure if extracting data wrong here?
*/
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code as country_code
FROM   
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;
	
-- Task 8: Which German store type is selling the most?

SELECT
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    dim_store_details.store_type AS store_type,
    dim_store_details.country_code AS country_code
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE 
    dim_store_details.country_code = 'DE'
GROUP BY
    store_type, country_code 
ORDER BY
    total_sales ASC;

-- Task 9: How quickly is the company making sales?
-- TO FIX: actual_time_taken is incorrect

WITH sales_date AS (
    SELECT 
        year,
        TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS sales_date_column,
        LAG(TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS')) OVER (PARTITION BY year ORDER BY TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS')) AS previous_sale_date
    FROM 
        dim_date_times
)
SELECT 
    year,
    '{"hours": ' || EXTRACT(HOUR FROM AVG(sales_date_column - previous_sale_date)) || ', "minutes": ' || EXTRACT(MINUTE FROM AVG(sales_date_column - previous_sale_date)) || ', "seconds": ' || EXTRACT(SECOND FROM AVG(sales_date_column - previous_sale_date)) || ', "milliseconds": ' || EXTRACT(MILLISECOND FROM AVG(sales_date_column - previous_sale_date)) || '}' AS actual_time_taken
FROM 
    sales_date
WHERE 
    previous_sale_date IS NOT NULL
GROUP BY 
    year
ORDER BY 
    AVG(sales_date_column - previous_sale_date) DESC
LIMIT 5;



