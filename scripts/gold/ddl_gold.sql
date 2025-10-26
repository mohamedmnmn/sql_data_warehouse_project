create or alter view gold.dim_customer as 
select 
	row_number()over(order by ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != ca.gen then ci.cst_marital_status
	    else coalesce(ca.gen,'n/a')
		end as gender,
	ca.bdate as birthdate,
	
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erb_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erb_laoc_a101 la
on ci.cst_key = la.cid
----------------------------------------------------------
create view gold.dim_products as
select
row_number() over (order by pn.prd_start_dt ,pn.prd_id) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pc.cat as product_category,
pc.subcat as product_subcategory,
pc.maintenance as product_maintenance,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erb_px_cat_g1v2 pc
on pn.prd_key = pc.id
where prd_end_dt is  null -- no old data
----------------------------------------------------------
create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_name
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id
