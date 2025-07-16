CREATE DATABASE InsigniaDW;
GO

USE InsigniaDW;
GO

CREATE TABLE dbo.ETL_Lineage (
    Lineage_Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    Source_System VARCHAR(100),
    Load_Start_Datetime DATETIME,
    Load_End_Datetime DATETIME,
    Rows_at_Source INT,
    Rows_at_Destination_Fact INT,
    Load_Status BIT
);

CREATE TABLE dbo.DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE,
    Day_Number INT,
    Month_Name VARCHAR(20),
    Short_Month CHAR(3),
    Calendar_Month_Number INT,
    Calendar_Year INT,
    Fiscal_Month_Number INT,
    Fiscal_Year INT,
    Week_Number INT
);

CREATE TABLE dbo.DimCustomer (
    CustomerSK INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerContactNumber BIGINT,
    IsActive BIT,
    StartDate DATETIME,
    EndDate DATETIME,
    Lineage_Id BIGINT
);

CREATE TABLE dbo.DimEmployee (
    EmployeeSK INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeId INT,
    EmployeeName VARCHAR(100),
    IsActive BIT,
    StartDate DATETIME,
    EndDate DATETIME,
    Lineage_Id BIGINT
);

CREATE TABLE dbo.DimProduct (
    ProductSK INT IDENTITY(1,1) PRIMARY KEY,
    Description VARCHAR(200),
    Unit_Price DECIMAL(10,2),
    Tax_Rate DECIMAL(5,2),
    Lineage_Id BIGINT
);

CREATE TABLE dbo.DimGeography (
    GeographySK INT IDENTITY(1,1) PRIMARY KEY,
    City VARCHAR(50),
    State_Province VARCHAR(50),
    Country VARCHAR(50),
    Continent VARCHAR(50),
    Region VARCHAR(50),
    Subregion VARCHAR(50),
    Current_Population INT,
    Previous_Population INT,
    Lineage_Id BIGINT
);

CREATE TABLE dbo.FactSales (
    FactSalesSK INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceId INT,
    ProductSK INT,
    CustomerSK INT,
    EmployeeSK INT,
    GeographySK INT,
    DateKey INT,
    Quantity INT,
    Profit DECIMAL(10,2),
    Total_Excluding_Tax DECIMAL(10,2),
    Tax_Amount DECIMAL(10,2),
    Total_Including_Tax DECIMAL(10,2),
    Lineage_Id BIGINT
);


DECLARE @StartDate DATE = '2000-01-01', @EndDate DATE = '2023-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO dbo.DimDate (
        DateKey, Date, Day_Number, Month_Name, Short_Month, Calendar_Month_Number,
        Calendar_Year, Fiscal_Month_Number, Fiscal_Year, Week_Number
    )
    SELECT
        CONVERT(INT, CONVERT(VARCHAR(8), @StartDate, 112)),
        @StartDate,
        DAY(@StartDate),
        DATENAME(MONTH, @StartDate),
        LEFT(DATENAME(MONTH, @StartDate), 3),
        MONTH(@StartDate),
        YEAR(@StartDate),
        CASE WHEN MONTH(@StartDate) >= 7 THEN MONTH(@StartDate) - 6 ELSE MONTH(@StartDate) + 6 END,
        CASE WHEN MONTH(@StartDate) >= 7 THEN YEAR(@StartDate) ELSE YEAR(@StartDate) - 1 END,
        DATEPART(WEEK, @StartDate);

    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END

SELECT TOP 10 * FROM DimDate

-- Create staging copy
SELECT * INTO dbo.Insignia_staging_copy FROM dbo.Insignia_staging WHERE 1 = 0;

-- Truncate before insert (always)
TRUNCATE TABLE dbo.Insignia_staging_copy;

-- Insert incremental data (assume you already imported dbo.Insignia_incremental)
INSERT INTO dbo.Insignia_staging_copy
SELECT * FROM dbo.Insignia_incremental;

SELECT TOP 10 * FROM Insignia_staging_copy

DECLARE @LineageId BIGINT = 1, @Today DATETIME = GETDATE();

USE InsigniaDW;

DECLARE @LineageId BIGINT = 1, @Today DATETIME = GETDATE();

-- Update existing rows that have changed
UPDATE dc
SET dc.IsActive = 0,
    dc.EndDate = @Today
FROM DimCustomer dc
JOIN Insignia_staging_copy isc
    ON dc.CustomerID = isc.Customer_Id
WHERE dc.IsActive = 1
  AND (dc.CustomerName <> isc.CustomerName OR dc.CustomerContactNumber <> isc.CustomerContactNumber);

-- Insert new versions
INSERT INTO DimCustomer (
    CustomerID, CustomerName, CustomerContactNumber, IsActive, StartDate, EndDate, Lineage_Id
)
SELECT 
    isc.Customer_Id, isc.CustomerName, isc.CustomerContactNumber, 1, @Today, NULL, @LineageId
FROM Insignia_staging_copy isc
LEFT JOIN DimCustomer dc
    ON dc.CustomerID = isc.Customer_Id AND dc.IsActive = 1
WHERE dc.CustomerID IS NULL OR
      (dc.CustomerName <> isc.CustomerName OR dc.CustomerContactNumber <> isc.CustomerContactNumber);




-- Update existing employee
DECLARE @LineageId BIGINT = 1, @Today DATETIME = GETDATE();
UPDATE de
SET de.IsActive = 0,
    de.EndDate = @Today
FROM DimEmployee de
JOIN Insignia_staging_copy isc
    ON de.EmployeeId = isc.employee_Id
WHERE de.IsActive = 1
  AND de.EmployeeName <> (isc.EmployeeFirstName + ' ' + isc.EmployeeLastName);

-- Insert new version
INSERT INTO DimEmployee (
    EmployeeId, EmployeeName, IsActive, StartDate, EndDate, Lineage_Id
)
SELECT 
    isc.employee_Id,
    isc.EmployeeFirstName + ' ' + isc.EmployeeLastName,
    1, @Today, NULL, @LineageId
FROM Insignia_staging_copy isc
LEFT JOIN DimEmployee de
    ON de.EmployeeId = isc.employee_Id AND de.IsActive = 1
WHERE de.EmployeeId IS NULL OR
      de.EmployeeName <> (isc.EmployeeFirstName + ' ' + isc.EmployeeLastName);



-- Update existing products
DECLARE @LineageId BIGINT = 1, @Today DATETIME = GETDATE();
UPDATE dp
SET dp.Unit_Price = isc.Unit_Price,
    dp.Tax_Rate = isc.Tax_Rate,
    dp.Lineage_Id = @LineageId
FROM DimProduct dp
JOIN Insignia_staging_copy isc
    ON dp.Description = isc.Description;

-- Insert new products
INSERT INTO DimProduct (Description, Unit_Price, Tax_Rate, Lineage_Id)
SELECT isc.Description, isc.Unit_Price, isc.Tax_Rate, @LineageId
FROM Insignia_staging_copy isc
LEFT JOIN DimProduct dp
    ON dp.Description = isc.Description
WHERE dp.Description IS NULL;




CREATE TABLE dbo.DimGeography (
    GeographySK INT IDENTITY(1,1) PRIMARY KEY,
    City VARCHAR(50),
    State_Province VARCHAR(50),
    Country VARCHAR(50),
    Continent VARCHAR(50),
    Region VARCHAR(50),
    Subregion VARCHAR(50),
    Current_Population INT,
    Previous_Population INT,
    Lineage_Id BIGINT
);


-- Update current population to previous and set new population
DECLARE @LineageId BIGINT = 1, @Today DATETIME = GETDATE();
UPDATE dg
SET dg.Previous_Population = dg.Current_Population,
    dg.Current_Population = isc.Latest_Recorded_Population,
    dg.Lineage_Id = @LineageId
FROM DimGeography dg
JOIN Insignia_staging_copy isc
    ON dg.City = isc.City AND dg.State_Province = isc.State_Province;

-- Insert new locations
INSERT INTO DimGeography (
    City, State_Province, Country, Continent, Region, Subregion,
    Current_Population, Previous_Population, Lineage_Id
)
SELECT 
    isc.City, isc.State_Province, isc.Country, isc.Continent,
    isc.Region, isc.Subregion,
    isc.Latest_Recorded_Population, NULL, @LineageId
FROM Insignia_staging_copy isc
LEFT JOIN DimGeography dg
    ON dg.City = isc.City AND dg.State_Province = isc.State_Province
WHERE dg.City IS NULL;

SELECT * FROM DimCustomer;

SELECT * FROM DimDate;

SELECT * FROM DimEmployee;


SELECT * FROM DimGeography;




-- Insert fact with surrogate keys and date key
DECLARE @LineageId BIGINT = 1, @Today DATETIME = GETDATE();
INSERT INTO FactSales (
    InvoiceId, ProductSK, CustomerSK, EmployeeSK, GeographySK, DateKey,
    Quantity, Profit, Total_Excluding_Tax, Tax_Amount, Total_Including_Tax,
    Lineage_Id
)
SELECT 
    isc.InvoiceId,
    dp.ProductSK,
    dc.CustomerSK,
    de.EmployeeSK,
    dg.GeographySK,
    CONVERT(INT, CONVERT(VARCHAR(8), GETDATE(), 112)), -- assuming today as sales date
    isc.Quantity,
    isc.Profit,
    isc.Total_Excluding_Tax,
    isc.Tax_Amount,
    isc.Total_Including_Tax,
    @LineageId
FROM Insignia_staging_copy isc
JOIN DimProduct dp
    ON dp.Description = isc.Description
JOIN DimCustomer dc
    ON dc.CustomerID = isc.Customer_Id AND dc.IsActive = 1
JOIN DimEmployee de
    ON de.EmployeeId = isc.employee_Id AND de.IsActive = 1
JOIN DimGeography dg
    ON dg.City = isc.City AND dg.State_Province = isc.State_Province;

SELECT TOP 10 * FROM FactSales;

INSERT INTO ETL_Lineage (
    Source_System, Load_Start_Datetime, Load_End_Datetime,
    Rows_at_Source, Rows_at_Destination_Fact, Load_Status
)
SELECT 
    'Insignia Source',
    @Today,
    GETDATE(),
    (SELECT COUNT(*) FROM Insignia_staging_copy),
    (SELECT COUNT(*) FROM FactSales WHERE Lineage_Id = @LineageId),
    1;

SELECT * FROM ETL_Lineage


              



