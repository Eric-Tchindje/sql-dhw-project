/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
CREATE VIEW gold.dim_customers  AS
SELECT  
	ROW_NUMBER () OVER (ORDER BY cst_id ) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_first_name as first_name,
	ci.cst_lastname as last_name,
	cl.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a') 
	END as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN  silver.erp_cust_az12  ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl ON  ci.cst_key = cl.cid;



-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
CREATE VIEW gold.dim_products  AS
SELECT 
	ROW_NUMBER () OVER (ORDER BY pn.prd_start_dt, prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	prd_cost as cost,
	pc.maintenance,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt is NULL; -- Filter out all historical data


-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
CREATE VIEW gold.fact_sales AS 
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key, -- surrogate key for inernal join fact to dim table
	cst.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS  due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
from silver.crm_sls_info sd 
LEFT JOIN gold.dim_customers cst ON sd.sls_cust_id = cst.customer_id
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number


