/*
==========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
==========================================================
*/
create or alter procedure bronze.load_bronze as
	begin
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	select count(*) from bronze.crm_cust_info;

	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	select count(*) from bronze.crm_prd_info;

	truncate table bronze.crm_sales_details;
	bulk insert bronze.crm_sales_details
	from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	select count(*) from bronze.crm_sales_details;

	truncate table bronze.erb_cust_az12;
	bulk insert bronze.erb_cust_az12
	from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	select count(*) from bronze.erb_cust_az12;

	truncate table bronze.erb_laoc_a101;
	bulk insert bronze.erb_laoc_a101
	from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	select count(*) from bronze.erb_laoc_a101;

	truncate table bronze.erb_px_cat_g1v2;
	bulk insert bronze.erb_px_cat_g1v2
	from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
	firstrow = 2,
	fieldterminator =',',
	tablock
	);
	select count(*) from bronze.erb_px_cat_g1v2;


end




