-- TEST crm_cust_info

SELECT cst_id, count(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id is null

SELECT * FROM silver.crm_cust_info WHERE cst_id = 29449

-- Check for unwanted Spaces
-- EXPECTATION : NO RESULT

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

---------------------------------------------------------------------
---------------------------------------------------------------------
-- TEST crm_prd_info

SELECT * FROM bronze.crm_prd_info

SELECT prd_id, count(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id is null

-- Check for unwanted Spaces
-- EXPECTATION : NO RESULT

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
select prd_cost
from bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date orders
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------------------

SELECT * FROM silver.crm_prd_info

SELECT prd_id, count(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id is null

-- Check for unwanted Spaces
-- EXPECTATION : NO RESULT

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
select prd_cost
from silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date orders
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------------------------------------------------------------------
------------------------------------------------------------------

-- TEST crm_sales_details

SELECT * FROM bronze.crm_sales_details
--WHERE sls_ord_num != TRIM(sls_ord_num)
WHERE sls_cust_id NOT IN (
SELECT cst_id FROM silver.crm_cust_info)

SELECT sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

SELECT sls_ship_dt FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0


-- Check For Invalid Dates

SELECT 
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR  LEN(sls_order_dt) != 8
OR sls_order_dt > 20250101

SELECT 
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR  LEN(sls_due_dt) != 8
OR sls_due_dt > 20250101


-- Check for Invalid Date Orders

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

-- Check Data Consistency

SELECT DISTINCT
sls_sales as sls_old_sales,
sls_quantity,
sls_price as old_price,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != ABS(sls_price) * sls_quantity
		 THEN ABS(sls_price) * sls_quantity
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_sales <= 0
OR sls_quantity IS NULL OR sls_quantity <= 0
OR sls_price is NULL OR sls_price <= 0

---------------------------
-- Check silver


-- Check for Invalid Date Orders

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

-- Check Data Consistency

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price

FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_sales <= 0
OR sls_quantity IS NULL OR sls_quantity <= 0
OR sls_price is NULL OR sls_price <= 0

------------------------------------------------------------------------
------------------------------------------------------------------------

-- TEST erp_cust_az12

SELECT * FROM bronze.erp_cust_az12

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
	ELSE cid
END AS cid
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
	ELSE cid
END NOT IN (
SELECT cst_key FROM silver.crm_cust_info)
 

 SELECT DISTINCT
 bdate
 FROM bronze.erp_cust_az12
 WHERE bdate < '1924-01-01' OR bdate > GETDATE()

 SELECT DISTINCT
 gen 
 FROM bronze.erp_cust_az12

SELECT DISTINCT
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

-- Check Silver

 SELECT DISTINCT
 bdate
 FROM silver.erp_cust_az12
 WHERE bdate > GETDATE()

 SELECT DISTINCT
 gen 
 FROM silver.erp_cust_az12

 SELECT *
 FROM silver.erp_cust_az12

 ---------------------------------------------------------------------
 ---------------------------------------------------------------------

 -- TEST erp_loc_a101

 SELECT * FROM bronze.erp_loc_a101

SELECT cst_key FROM silver.crm_cust_info


SELECT * FROM bronze.erp_loc_a101
WHERE cid NOT IN (
SELECT cst_key FROM silver.crm_cust_info)


SELECT * FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
SELECT cst_key FROM silver.crm_cust_info)

-- Data Standardization & Consistency

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
order by cntry

SELECT DISTINCT cntry,
CASE WHEN TRIM(cntry) = 'DE' tHEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

--------------------	
-- Check Silver

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
order by cntry

SELECT * FROM silver.erp_loc_a101

----------------------------------------------------------------------
----------------------------------------------------------------------

-- TEST erp_px_cat_g1v2


SELECT * FROM bronze.erp_px_cat_g1v2

SELECT cat_id FROM silver.crm_prd_info




-- Check Unwanted Spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)


-- Data Standardization & Consistency

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2
