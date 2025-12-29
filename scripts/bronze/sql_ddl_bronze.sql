/*
=============================================================================================
DDL Script: Create Bronze Tables
=============================================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
=============================================================================================
*/

-- Create DDL for Tables
-- Consult the technical experts of the source system to understand its metadata.
-- OR
-- Explore the data to identify column names and data types

--==========================
-- 1st CRM TABLE
--==========================
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
GO
--============================
-- 2nd CRM Table
--============================
IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
GO

--============================
-- 3rd CRM table
--===========================
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details (
	sls_prd_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

--===========================
-- 1st ERP Table
--===========================
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO

--===========================
-- 2nd ERP Table
--===========================
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	CNTRY NVARCHAR(50)
);
GO

--===========================
-- 3rd ERP Table
--===========================
IF OBJECT_ID ('bronze.erp_px_cat_g1v2') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO

-- TO RENAME A TABLE
-- EXEC sp_rename 'bronze.erp_px_cat_g1v2', 'erp_px_cat_g1v2';

-- Develop SQL Load Script
-- BULK INSERT
--===========================================
-- STORED PROCCEDURE
--===========================================
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================================';

		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------';

		--===========================
		-- 1st CRM Table Loading
		--===========================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:path_to_csv\cust_info.csv'
		WITH (
			FIRSTROW = 2,		--> First row includes the column names so we want that first row should be the second row
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------'

		--===========================
		-- 2nd CRM Table Loading
		--===========================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:path_to_csv\prd_info.csv'
		WITH (
			FIRSTROW = 2,		--> First row includes the column names so we want that first row should be the second row
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------'


		--===========================
		-- 3rd CRM Table Loading
		--===========================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:path_to_csv\sales_details.csv'
		WITH (
			FIRSTROW = 2,		--> First row includes the column names so we want that first row should be the second row
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------'

		PRINT '--------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------';
		--===========================
		-- 1st ERP Table
		--===========================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:path_to_csv\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,		--> First row includes the column names so we want that first row should be the second row
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------'


		--===========================
		-- 2st ERP Table
		--===========================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:path_to_csv\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,		--> First row includes the column names so we want that first row should be the second row
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------'


		--===========================
		-- 3rd ERP Table
		--===========================
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:path_to_csv\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,		--> First row includes the column names so we want that first row should be the second row
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '---------------'
		SET @batch_end_time = GETDATE();
		PRINT '===============================================';
		PRINT 'Loading Bronze Layer is Completed.';
		PRINT '>> TIME TAKEN TO LOAD WHOLE BATCH: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
		PRINT '===============================================';
	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Procedure in which the error occured: ' + CAST(ERROR_PROCEDURE() AS NVARCHAR);
		PRINT 'Error Line In Routine: ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT '===============================================';
	END CATCH												   
END;

EXEC bronze.load_bronze;
