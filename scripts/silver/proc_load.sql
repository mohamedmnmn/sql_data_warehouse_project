create or alter procedure silver.load_silver as
begin
	truncate table silver.crm_cust_info
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname)as cst_lastname,
	case when upper(cst_marital_status) ='S' then 'Single'
		when upper(cst_marital_status) = 'M' then 'Married'
		else 'n/a'
	end cst_marital_status,
	case when upper(cst_gndr) ='F' then 'Female'
		when upper(cst_gndr) = 'M' then 'Male'
		else 'n/a'
	end cst_gndr,
	cst_create_date
	from(
	select * ,
	row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null


	)t where flag_last =1

	------------------------------------------

	truncate table silver.crm_prd_info
	insert into silver.crm_prd_info(
	prd_id ,
	cat_id ,
	prd_key ,
	prd_nm ,
	prd_cost ,
	prd_line ,
	prd_start_dt ,
	prd_end_dt ,
	dwh_create_date 
	)
	select 
	prd_id,
	prd_key,
	replace(SUBSTRING(prd_key,1,5),'-','_')as cat_id,
	substring(prd_key,7,len(prd_key))as prd_key,
	isnull(prd_cost,0)as prd_cost,

	case  upper(trim(prd_line))
		 when 'S'then  'Other sales'
		 when 'M'then  'Mountain'
		 when 'R'then  'Road'
		 when 'T'then  'Touring'
		 else 'n/a'
		 end as prd_line,
	prd_start_dt,
	DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt,
	GETDATE() AS dwh_create_date 
  


	from bronze.crm_prd_info
	-----------------------------
	truncate table silver.crm_sales_details 
	insert into silver.crm_sales_details(
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price

	)
	select 
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,

	case when sls_order_dt =0 or len(sls_order_dt)!=8 then null
		else cast(cast(sls_order_dt as nvarchar) as date)
		end as sls_order_dt,
	case when sls_ship_dt =0 or len(sls_ship_dt)!=8 then null
		else cast(cast(sls_ship_dt as nvarchar) as date)
		end as sls_ship_dt,
	case when sls_due_dt =0 or len(sls_due_dt)!=8 then null
		else cast(cast(sls_due_dt as nvarchar) as date)
		end as sls_due_dt,
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price)
		then sls_quantity*abs(sls_price)
		else sls_sales
		end as sls_sales,
	sls_quantity ,
	case when sls_price is null or sls_price <=0 
		then sls_sales/nullif(sls_quantity,0)
		else sls_price
	end as sls_price

	from bronze.crm_sales_details

		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from 'C:\Users\modye\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator =',',
		tablock
		);


	-----------------------------------
	truncate table silver.erb_cust_az12
	insert into silver.erb_cust_az12 (cid,bdate,gen)
	select 
	case when cid like'NAS%' then SUBSTRING(cid,3,len(cid))
		else cid
	end as cid,
	case when bdate > getdate() then null
		else bdate 
	end as bdate,
	case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		 when upper(trim(gen)) in ('M','MALE') then 'Male'
		 else 'n/a'
	end as gen
	from bronze.erb_cust_az12


	--------------------------------------
	truncate table silver.erb_laoc_a101 
	insert into silver.erb_laoc_a101 (cid,cntry)
	select 
	replace(cid,'-','')as cid,
	case when cntry ='DE'then 'Germany'
		when  cntry in ('US','USA') then 'United States'
		when cntry is null or cntry =' ' then 'n/a'
		else cntry
	end as cntry
	from bronze.erb_laoc_a101

	--------------------------------------------
	truncate table  silver.erb_px_cat_g1v2
	insert into silver.erb_px_cat_g1v2(id,
	cat,
	subcat,
	maintenance)
	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erb_px_cat_g1v2
end
