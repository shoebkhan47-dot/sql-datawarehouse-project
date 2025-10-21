--In gold layer we wiil create 3 views based on 1)CUSTOMERS, 2)PRODUCTS & 3)SALES for doing EDA & ADA ---> refer dataintegration picture
--we will make starschema in the 3 views 1  FACTS(SALES) table and 2 DIM(CUSTOMERS,PRODUCTS) tables

-- 1)CUSTOMERS
-- IN 1st column we keer ROW NUMBER order by cst_id to make it kind of primary key for all the tables combined(surrogate key)
-- we choose ROW NUMBER because it gives all the unique values and does not handles ties
-- Then consider all the columns one by one 
-- in gender column first try to get the values from 1st table i.e ci table then if thers n/a values in ci tables then take the value from ca tables
-- if there are null values ca table make them na using coalesce function
CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE 
WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid;

SELECT*FROM gold.dim_customers

--2)PRODUCTS
--We want all the columns from the tables related to product i.e silver.crm_prd_info as pn & silver.erp_px_cat_g1v2 pc as pc
-- we want all the values where the end date is NULL to get latest value for each product

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL -- if there already exists a view with the same name then we drop the view'V' then create the view again
DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance AS maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

SELECT*FROM gold.dim_products

--3)Sales
--We want all the columns from silver.crm_sales_details that are not in both the DIM views created
--But we want the surrogate keys from both the views i.e product_key from gold.dim_product & customer_key from gold.dim_customers
-- we want to display the SUrrogate keys in the FACTS view to identify the details about the product & customer 

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num as order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
ON sd.sls_prd_key=dp.product_number
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id=dc.customer_id
GO

SELECT*FROM gold.fact_sales
SELECT * from gold.dim_customers
SELECT * from gold.dim_products
