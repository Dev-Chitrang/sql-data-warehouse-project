/*
This script load the data from csv into bronze layer.
Since I am using ubuntu system so my I have my path where the CSV files are stored in FROM clause, change the path accordling for respective OS.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME
    BEGIN TRY
        PRINT '************************************'
        PRINT 'Loading Bronze Layer'
        PRINT '************************************'
        
        PRINT '------------------------------------'
        PRINT 'Loading CRM Tables'
        PRINT '------------------------------------'
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting into Table: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sec'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting into Table: bronze.crm_prd_info '
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sec'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting into Table: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sec'

        -------------------------------------------------------------------------------------------------

        PRINT '------------------------------------'
        PRINT 'Loading ERP Tables'
        PRINT '------------------------------------'

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'Inerting into Table: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sec'

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'Inserting into Table: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sec'

        SET @start_time = GETDATE();
        PRINT 'Truncating Table: bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'Inserting into Table: bronze.erp_px_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' sec'
    END TRY
    BEGIN CATCH
        PRINT '****************************************************'
        PRINT 'ERROR OCCURED DURING LOADING LAYER'
        PRINT 'Error message' + ERROR_MESSAGE()
        PRINT '****************************************************'
    END CATCH
END

EXEC bronze.load_bronze;