CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    DECLARE 
        @start_time DATETIME2(7),
        @end_time DATETIME2(7),
        @duration BIGINT;

    BEGIN TRY

        /*====================================================*/
        PRINT '>> Loading bronze.crm_cust_info';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Muhammad Awais\Documents\SQL Server Management Studio 22\med architecture datawarehouse\source_crm\cust_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);

        PRINT '   Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


        /*====================================================*/
        PRINT '>> Loading bronze.crm_prd_info';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Muhammad Awais\Documents\SQL Server Management Studio 22\med architecture datawarehouse\source_crm\prd_info.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);

        PRINT '   Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


        /*====================================================*/
        PRINT '>> Loading bronze.crm_sales_details';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Muhammad Awais\Documents\SQL Server Management Studio 22\med architecture datawarehouse\source_crm\sales_details.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);

        PRINT '   Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


        /*====================================================*/
        PRINT '>> Loading bronze.erp_cust_az12';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12 
        FROM 'C:\Users\Muhammad Awais\Documents\SQL Server Management Studio 22\med architecture datawarehouse\source_erp\CUST_AZ12.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);

        PRINT '   Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


        /*====================================================*/
        PRINT '>> Loading bronze.erp_loc_a101';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101 
        FROM 'C:\Users\Muhammad Awais\Documents\SQL Server Management Studio 22\med architecture datawarehouse\source_erp\loc_a101.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);

        PRINT '   Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


        /*====================================================*/
        PRINT '>> Loading bronze.erp_px_cat_g1v2';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2 
        FROM 'C:\Users\Muhammad Awais\Documents\SQL Server Management Studio 22\med architecture datawarehouse\source_erp\PX_CAT_G1V2.csv'
        WITH(
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        SET @duration = DATEDIFF(MICROSECOND,@start_time,@end_time);

        PRINT '   Completed in ' + CAST(@duration AS NVARCHAR(50)) + ' microseconds';


    END TRY
    BEGIN CATCH
        PRINT 'Error Message: ' + ERROR_MESSAGE();
    END CATCH

END;
