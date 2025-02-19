/*


Create Database and Schemas


This script checks if the database already exists or not if it exists then it will delete the exisiting database.
Additionally, the script sets up three schemas within the database: 'bronze', 'silver' and 'gold.
*/


USE master;
GO

-- Drop and recreate the "DataWarehouse" database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "DataWarehouse")
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehoue;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

