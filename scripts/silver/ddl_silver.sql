IF OBJECT_ID('silver.crm_cust_info','U') is not null
    Drop Table silver.crm_cust_info;
create TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr nvarchar(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 Default GETDATE()
);
IF OBJECT_ID('silver.crm_prd_info','U') is not null
    Drop Table silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME,
    dwh_create_date DATETIME2 Default GETDATE()
);
IF OBJECT_ID('silver.crm_sales_details','U') is not null
    Drop Table silver.crm_sales_details;
create table silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id int,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date DATETIME2 Default GETDATE()

);
IF OBJECT_ID('silver.erp_cust_az12','U') is not null
    Drop Table silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(20),
    dwh_create_date DATETIME2 Default GETDATE()
);
IF OBJECT_ID('silver.erp_loc_a101','U') is not null
    Drop Table silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    CID NVARCHAR(50),
    CNTRY NVARCHAR(100),
    dwh_create_date DATETIME2 Default GETDATE()
);
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') is not null
    Drop Table silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    ID NVARCHAR(50),
    CAT NVARCHAR(100),
    SUBCAT NVARCHAR(100),
    MAINTENANCE NVARCHAR(10),
    dwh_create_date DATETIME2 Default GETDATE()
);
