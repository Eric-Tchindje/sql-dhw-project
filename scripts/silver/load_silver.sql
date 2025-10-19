CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY 
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

		-- track batch time load
		SET @batch_start_time = GETDATE();

		PRINT '=================================================';
		PRINT 'LOAD silver layer';
		PRINT '=================================================';

		PRINT '--------------------------------------------------';
		PRINT 'Loading crm table';
		PRINT '--------------------------------------------------';

		-- track start time
		SET @start_time = GETDATE();

		-- CRM cust_info
		PRINT '>> Truncate table : silver.crm_cust_info ';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Insert data into silver.crm_cust_info ';
		WITH 
			cte_cst_with_flag_last AS(
				-- count number of row for each custumer key : cst_id  
				-- remove unwanted space
				select 
					cst_id,
					cst_key,
					TRIM(cst_lastname) as cst_lastname,
					TRIM(cst_first_name) as cst_first_name,
					cst_gndr,
					cst_marital_status,
					cst_create_date,
					ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date  DESC) as flag_last
				from bronze.crm_cust_info
				where cst_id is NOT NULL
			),
			-- remove duplicate and trim  : Data filtered
			-- the last row correspond to the expected value
			cte_cst_uniq_key as (
				SELECT 
					cst_id,
					cst_key,
					TRIM(cst_lastname) as cst_lastname,
					TRIM(cst_first_name) as cst_first_name,
					cst_gndr,
					cst_marital_status,
					cst_create_date
				FROM cte_cst_with_flag_last
				WHERE flag_last =  1
			),

			-- Data Normalization or Data standardization :  gender and marital status columns
			cte_cst_standardize as (
				SELECT 
					cst_id,
					cst_key,
					cst_lastname,
					cst_first_name,
					CASE 
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						ELSE 'n/a'
					END cst_gndr,
					CASE 
						WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
						ELSE 'n/a'
					END cst_marital_status,
					cst_create_date
				FROM cte_cst_uniq_key
			)

			-- Insert clean data into the siver customer info table
			INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_first_name,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT 
				cst_id,
				cst_key,
				cst_first_name,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			FROM cte_cst_standardize;

		-----------------------------------------------------

		-- crm_prd_info
		PRINT '>> Truncate table : silver.crm_prd_info ';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Insert data into silver.crm_prd_info ';
		WITH  
			--Derived Column :  Creation of new column base of calculation of existing coluns 
			-- Transformation column : remove null value,  data enrichemnet , data standardization
			cte_extract_cat_to_product_key as (
				SELECT 
					prd_id,
					prd_key,
					REPLACE(SUBSTRING(prd_key,1,5), '-', '_') as cat_id, 
					SUBSTRING(prd_key, 7, LEN(prd_key)) as prod_key,
					prd_nm, 
					ISNULL(prd_cost,0) as prd_cost ,
					CASE
						WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
						WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
						WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Ohter Sales'
						WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
						ELSE 'n/a'
					END AS prd_line,
 					prd_start_dt,
					prd_end_dt
				FROM bronze.crm_prd_info
			),

			cte_prd_create_correct_end_date as (	
				SELECT 
					prd_id,
					prd_key,
					prod_key,
					cat_id,
					prd_nm,
					prd_cost,
					prd_line,
					CAST (prd_start_dt AS DATE) AS prd_start_dt,
					prd_end_dt,
					-- for the same key product take the next dday price's change and substract 1
					CAST(LEAD(prd_start_dt) OVER(PARTITION BY prod_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_true
				FROM cte_extract_cat_to_product_key
			)

			-- Insert into siler product info
			INSERT INTO silver.crm_prd_info (
				prd_id,
				prd_key,
				cat_id,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT 
				prd_id,
				prod_key,
				cat_id,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_true
			FROM cte_prd_create_correct_end_date;

		------------------------------------------------------

		-- crm_sls_info
		PRINT '>> Truncate table : silver.crm_sls_info ';
		TRUNCATE TABLE silver.crm_sls_info;
		PRINT '>> Insert data into silver.crm_sls_info ';
		WITH 
			cte_clean_crm_sls_info as (
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
				END AS sls_ship_dt,
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
				FROM bronze.crm_sls_info
			)

			INSERT INTO silver.crm_sls_info
			(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			FROM cte_clean_crm_sls_info;

		SET @end_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'Loading Silver layer CRM Table';
		PRINT '>>Total  Duration  :' + CAST(DATEDIFF (second, @start_time, @end_time) AS NVARCHAR)  +  ' seconds'; 
		PRINT '--------------------------------------------------';

		--------------------------------------------------------
	
		PRINT '--------------------------------------------------';
		PRINT 'Loading erp Tables';
		PRINT '--------------------------------------------------';

		-- track start time
		SET @start_time = GETDATE();

		--ERP Cleaning erp_cust_az12
		PRINT '>> Truncate table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Insert data into silver.erp_cust_az12';
		WITH 
			cte_clean_erp_cust_az12 as (
				SELECT 
					cid as old_cid,
					CASE
					WHEN  cid LIKE 'NAS%'  THEN substring(cid, 4, len(cid))
					ELSE cid
					END AS cid,
					bdate as  bdate_old,
					CASE 
						WHEN bdate > GETDATE() THEN NULL
						ELSE bdate
					END as bdate,
					gen as gen_old,
					CASE 
						WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE')  THEN 'Female'
						WHEN UPPER(TRIM(gen))  IN ('M', 'MALE')  THEN 'Male' 
						ELSE 'n/a'
					END as gen
				FROM bronze.erp_cust_az12
			)

			INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)
			select 
				cid,
				bdate,
				gen
			from cte_clean_erp_cust_az12;


		-------------------------------------------------------

		-- erp_loc_a101
		PRINT '>> Truncate table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Insert data into silver.erp_loc_a101';
		WITH 
			cte_clean_erp_loc_a101 as (
				SELECT
				cid AS cid_old,
				REPLACE(cid, '-', '') as cid ,
				cntry as cntry_old,
				CASE
					WHEN  TRIM(cntry) in ('US', 'USA') THEN 'United States'
					WHEN  TRIM(cntry) in ('DE') THEN 'Germany'
					WHEN  TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					ELSE TRIM(cntry)
				END AS cntry
				FROM  bronze.erp_loc_a101
			)
	 
			INSERT INTO silver.erp_loc_a101 (
				cid, 
				cntry
			)
			SELECT 
				cid,
				cntry
			from  cte_clean_erp_loc_a101

		---------------------------------------------------------

		-- erp_px_cat_g1v2
		PRINT '>> Truncate table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Insert data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
		(    id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			TRIM(cat),
			TRIM(subcat),
			TRIM(maintenance)
		from bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '--------------------------------------------------';
		PRINT 'Loading Silver layer ERP Table';
		PRINT '>>Total  Duration  :' + CAST(DATEDIFF (second, @start_time, @end_time) AS NVARCHAR)  +  ' seconds'; 
		PRINT '--------------------------------------------------';

		-- track end of silver load time
		SET @batch_end_time=GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver is Completed';
		PRINT '>>Total  Duration  :' + CAST(DATEDIFF (second, @batch_start_time, @batch_end_time) AS NVARCHAR)  +  ' seconds'; 
		PRINT '================================================';
	END TRY


	BEGIN CATCH
		PRINT '==================================================================';
		PRINT 'ERROR Occure During Loading silver layer';
		PRINT 'Error Message' + ERROR_MESSAGE() ;
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==================================================================';
	END CATCH
END
