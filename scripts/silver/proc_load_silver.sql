-- use EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

DECLARE 
@start_time DATETIME2(7),
@end_time DATETIME2(7),
@duration BIGINT;

BEGIN TRY

/*====================================================*/
PRINT '>> Loading silver.crm_cust_info';
SET @start_time = SYSDATETIME();

TRUNCATE TABLE silver.crm_cust_info;

insert into silver.crm_cust_info
(
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
Trim(cst_firstname) as first_name,
Trim(cst_lastname) as last_name,
CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Un-Married'
	ELSE 'N/A'
END cst_marital_status,
CASE
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
ELSE 'N/A'
end cst_gndr,
cst_create_date
from (
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date Desc) flag
from bronze.crm_cust_info
) as t where flag = 1 and cst_id is not null;

SET @end_time = SYSDATETIME();
SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);
PRINT 'Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


/*====================================================*/
PRINT '>> Loading silver.crm_prd_info';
SET @start_time = SYSDATETIME();

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info
(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select 
prd_id,
Replace(substring(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
isNull(prd_cost,0) as prd_cost,
CASE 
	WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
	WHEN UPPER(TRIM(prd_line))='S' THEN 'Other sales'
ELSE 'N/A'
END prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(LEAD(prd_start_dt-1) over(partition by prd_key order by prd_id asc) as date) as prd_end_dt  
from bronze.crm_prd_info;

SET @end_time = SYSDATETIME();
SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);
PRINT 'Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


/*====================================================*/
PRINT '>> Loading silver.crm_sales_details';
SET @start_time = SYSDATETIME();

TRUNCATE TABLE silver.crm_sales_details;

insert into silver.crm_sales_details(
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
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
WHEN sls_order_dt=0 or len(sls_order_dt) !=8 THEN Null
ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END sls_order_dt,
CASE 
WHEN sls_ship_dt=0 or len(sls_ship_dt) !=8 THEN Null
ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE 
WHEN sls_due_dt=0 or len(sls_due_dt) !=8 THEN Null
ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
Case
	WHEN sls_sales<=0 OR sls_sales is Null OR sls_sales!= sls_quantity*ABS(sls_price)
		THEN sls_quantity*ABS(sls_price)
	ELSE sls_sales
END as sls_sales,
sls_quantity,
Case 
	 WHEN sls_price<=0 or sls_price is null 
	 THEN sls_sales/NULLIF(sls_quantity, 0)
	 Else sls_price
END as sls_price
from bronze.crm_sales_details;

SET @end_time = SYSDATETIME();
SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);
PRINT 'Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


/*====================================================*/
PRINT '>> Loading silver.erp_cust_az12';
SET @start_time = SYSDATETIME();

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12( 
CID,
BDATE,
GEN
)
select
CASE 
WHEN CID like 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
	ELSE CID
END CID,
CASE WHEN BDATE > GETDATE() THEN NULL
ELSE BDATE
END BDATE,
CASE WHEN UPPER(TRIM(GEN)) like 'F%' OR UPPER(GEN) like 'f%' THEN 'FEMALE'
	WHEN UPPER(TRIM(GEN)) Like 'M%' or UPPER(GEN) Like 'm%' THEN 'MALE'
	WHEN GEN is null THEN 'N/A'
	WHEN GEN='' THEN 'N/A'
	ELSE GEN
END AS GEN
from bronze.erp_cust_az12;

SET @end_time = SYSDATETIME();
SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);
PRINT 'Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


/*====================================================*/
PRINT '>> Loading silver.erp_loc_a101';
SET @start_time = SYSDATETIME();

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101
(
CID,
CNTRY
)
select
REPLACE(CID,'-','') as CID,
CASE 
	WHEN CNTRY='DE' THEN 'GERMANY'
	WHEN CNTRY='US' THEN 'UNITED STATES'
	WHEN CNTRY='USA' THEN 'UNITED STATES'
	WHEN CNTRY='' THEN 'N/A'
	WHEN CNTRY is NULL THEN 'N/A'
	ELSE UPPER(CNTRY)
END CNTRY
from bronze.erp_loc_a101;

SET @end_time = SYSDATETIME();
SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);
PRINT 'Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


/*====================================================*/
PRINT '>> Loading silver.erp_px_cat_g1v2';
SET @start_time = SYSDATETIME();

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2
(
ID,
CAT,
SUBCAT,
MAINTENANCE
)
SELECT * from bronze.erp_px_cat_g1v2;

SET @end_time = SYSDATETIME();
SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);
PRINT 'Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';

END TRY

BEGIN CATCH
PRINT 'Error: ' + ERROR_MESSAGE();
END CATCH

END
