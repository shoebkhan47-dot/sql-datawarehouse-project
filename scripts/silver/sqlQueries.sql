---INSERTING VALUES INTO SILVER TABLE AFTER CORRECTING THE BRONZE TABLE
--- correcting bronze.crm_cust_info AND inserting into silver.crm_cust_info

INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
SELECT 
cst_id,
cst_key,
trim(cst_firstname) as cst_firsname,
trim(cst_lastname) as cst_lastname,
CASE
WHEN UPPER(trim(cst_marital_status)) = 'M' THEN 'Married'
WHEN UPPER(trim(cst_marital_status)) = 'S' THEN 'Single'
ELSE 'n/a'
end as cus_marital_status,
CASE
WHEN UPPER(trim(cst_gndr)) = 'M' THEN 'Male'
WHEN UPPER(trim(cst_gndr)) = 'F' THEN 'Female'
ELSE 'n/a'
end as cus_gndr,
cst_create_date
from
(SELECT *,
ROW_NUMBER() Over (PARTITION BY cst_id order by cst_create_date desc) as ranking_by_row_number
from bronze.crm_cust_info)
as t
WHERE ranking_by_row_number = 1 AND cst_id is NOT NULL;

SELECT*
from bronze.crm_cust_info;
 
--- checking validations on bronze table
SELECT cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having COUNT(*) >1;

SELECT cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

SELECT cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

SELECT cst_marital_status
from bronze.crm_cust_info
group by (cst_marital_status);

SELECT cst_gndr
from bronze.crm_cust_info
group by (cst_gndr);

--- checking validations are correct or not  on silver table (Just replace bronze by silver) 

select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having COUNT(*) >1;

SELECT cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

SELECT cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

SELECT cst_marital_status
from silver.crm_cust_info
group by (cst_marital_status);

SELECT cst_gndr
from silver.crm_cust_info
group by (cst_gndr);

--- correcting bronze.crm_prd_info AND inserting into silver.crm_prd_info

SELECT*FROM 
bronze.crm_prd_info
--theres no problem in first column i.e to duplicates or null values are there
-- In second column prd_key of bronze.crm_prd_info if we look at the data structure its used to join bronze.erp_px_cat_g1v2 and bronze.crm_sales_details
-- so we will adjust the values accordingly
-- in third column theres no problem i.e prd_nm = trim(prd_nm)
--IN fourth column prd_cost should not be less than 0 & not NULL check for that and if its NULL make is 0 zero ISNULL function
-- IN Fifth column prd_line we have 'M','R','S','T' if we dont know what it is we should ask the client or oue senior
--but in our case we have the information that 'M' = 'Mountain','R' = 'Road', 'S' = 'Other Sales', 'T' = 'Touring'
-- IN sixth column prd_start_dt theres no time so cast the column to DATE from datetime format
-- IN seventh column prd_end_dt there are lot of irregularies like end dt more than start dt so
-- use lead function and 1 day before start date to get end date i.e end dt = (lead(start dt))-1
-- create a new silver.crm_prf_info table because earlier table we didnt had extra column on cat_id also add one more column at the last as dwh_create_date
-- the last column will help us get date in silver table later on

SELECT*FROM bronze.crm_prd_info
SELECT*FROM bronze.erp_px_cat_g1v2
SELECT*FROM bronze.crm_sales_details


IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info(
    prd_id       INT,
	cat_id       NVARCHAR(50), 
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
INSERT INTO silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE
WHEN UPPER(trim(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(trim(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(trim(prd_line)) = 'S' THEN 'Other Sales'
WHEN UPPER(trim(prd_line)) = 'T' THEN 'Touring'
ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt as DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info

--Queries to check validations in bronze tables ----> do the same valitions and check for silver as well
--for 1st column
select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having COUNT(*) >1;
--for 2nd column
SELECT*FROM bronze.crm_prd_info
SELECT*FROM bronze.erp_px_cat_g1v2
SELECT*FROM bronze.crm_sales_details
--for 3rd column
SELECT prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)
--for 4th column
SELECT prd_cost
FROM bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null


--NOW we will do corrections  from 3rd table i.e bronze.crm_sales_details table and insert the data into silver table
-- For 1st column(sls_ord_num) check if there any any extra spaces
-- For 2nd column(sls_prd_key) check if any values exsiting in the column does not exist in the column (prd_key) of table (silver.crm_prd_info) and
-- For 3nd column(sls_cust_id) check if any values exsiting in the column does not exist in the column (cst_id) of table (silver.crm_cust_info)
-- The above i.e 2nd & 3rd columns values are checked with the silver table's column values because they are connected--> refer data integration image.

--For 4th column(sls_order_dt) its in int form so CAST it as varchar then to date (As CASTING directly INT to DATE into possible in sql)
-- The conditions its should satisfy are sls_order_dt !=<0 and LEN(sls_order_dt) = 8 otherwise it cant cast as DATE
-- SO whichever values are <=0 or LEN!=8 make them as NULL values remaining values CAST as DATE
--FOR 5th column(sls_ship_dt) column same as 4th column
-- FOR 6TH column(sls_due_dt) same as 4th & 5th column
-- For 7th column(sls_sales) check for conditions like IS NULL, <=0 and sls_sales != abs(sls_quantity*sls_price) here abs(absolute fun) is used to make all vaues +ve
-- For 8th column(sls_quantity) check for conditions like IS NULL, <=0 and sls_quantity != abs(sls_sales/sls_price)
-- For 9th column(sls_price) check for conditions like IS NULL, <=0 and sls_price != abs(sls_sales/sls_quantity)
select*from bronze.crm_sales_details
--checks
select sls_ord_num from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);-- For 1st column

select sls_prd_key from bronze.crm_sales_details
where sls_prd_key  not in (select prd_key from silver.crm_prd_info); -- For 2nd column (as there are no new values no need for any corrections)

select sls_cust_id from bronze.crm_sales_details
where sls_cust_id  not in (select cst_id from silver.crm_cust_info); --For 3rd column (as there are no new values no need for any corrections)
--checks

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE() -- also add this new column used for EDA & ADA
);
INSERt INTO silver.crm_sales_details
(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE 
WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt ,
CASE 
WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE 
WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
sls_quantity,
CASE 
WHEN sls_price IS NULL OR sls_price <= 0 
THEN sls_sales / NULLIF(sls_quantity, 0)
ELSE sls_price  -- Derive price if original value is invalid
END AS sls_price
FROM bronze.crm_sales_details;

--NOW in 4th table we need to do corrections and insert the values of 4th table from broze to silver

INSERT INTO silver.erp_cust_az12
(cid,bdate,gen)
(SELECT
CASE
WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) -- Remove 'NAS' prefix if present
ELSE cid
END AS cid,
CASE 
WHEN bdate >GETDATE() then NULL  -- Set future birthdates to NULL
ELSE bdate
END AS bdate,
CASE 
WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') then 'Female'
WHEN UPPER(TRIM(gen)) IN ('M','MALE') then 'Male'
ELSE 'n/a'
END AS gen  -- Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12)

SELECT *FROM bronze.erp_cust_az12
SELECT *FROM silver.crm_cust_info
//
SELECT bdate FROM bronze.erp_cust_az12
where MONTH (bdate) =02 and day (bdate) >28
SELECT gen FROM bronze.erp_cust_az12
GROUP BY gen//

-- NOW we correct values in 5th table i.e in bronze.erp_loc_a101  and insert into silver table i.e silver.erp_loc_a101
-- 1st column cid is checked with silver.crm_cust_info based on data integration picture
--the '-' from cid column is removed/replaced to match with the values in cst_key from silver.crm_cust_info
--2nd column cntry we group by cntry to check all the distinct values in cntry column 

INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT 
REPLACE(cid,'-','') as cid,
CASE
WHEN TRIM(cntry) = 'DE' THEN 'Germany'
WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'United States'
WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
ELSE TRIM(cntry)
END AS cntry   -- Normalize and Handle missing or blank country codes
FROM bronze.erp_loc_a101;

//SELECT REPLACE(cid,'-','') as cid
FROM bronze.erp_loc_a101
where  REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

SELECT cntry from
bronze.erp_loc_a101
group by cntry//

--NOW we correct values in 6th table i.e in bronze.erp_px_cat_g1v2  and insert into silver table i.e silver.erp_px_cat_g1v2
-- Theres no corrections in this table so directly insert into the silver table

INSERT INTO silver.erp_px_cat_g1v2
(id,cat,subcat,maintenance)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

SELECT*FROM silver.erp_px_cat_g1v2

//
SELECT*FROM bronze.erp_px_cat_g1v2
WHERE cat != trim(cat) or subcat != trim(subcat)

SELECT Distinct(maintenance) FROM bronze.erp_px_cat_g1v2//


