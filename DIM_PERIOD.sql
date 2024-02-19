



create procedure [dbo].[usp_Dim_PERIOD_FY]
as 
begin


--CREATE TABLE dim_FY_Period (
--    MONTH_NO INT,
--    MONTH_NAME VARCHAR(50),
--    YEAR_NO INT,
--    FINANCIAL_MONTH_NO INT,
--    FINANCIAL_YEAR INT,
--    INDEX_NUMBER INT
--);

-- Set your start and end dates
DECLARE @StartDate DATE = '2016-01-01';
DECLARE @EndDate DATE = '2030-03-31';

-- Use a recursive CTE to generate the periods
WITH DateRange AS (
    SELECT @StartDate AS CurrentDate
    UNION ALL
    SELECT DATEADD(MONTH, 1, CurrentDate)
    FROM DateRange
    WHERE CurrentDate < @EndDate
)

INSERT INTO [dbo].[DIM_PERIOD_FY] (MONTH_NO, MONTH_NAME, YEAR_NO, FINANCIAL_MONTH_NO, FINANCIAL_YEAR, INDEX_NUMBER)
SELECT
    MONTH(CurrentDate) AS MONTH_NO,
    DATENAME(MONTH, CurrentDate) AS MONTH_NAME,
    YEAR(CurrentDate) AS YEAR_NO,
    (MONTH(CurrentDate) + 8) % 12 + 1 AS FINANCIAL_MONTH_NO,
    (CASE WHEN (MONTH(CurrentDate))  <=3 THEN convert(varchar(4),                             
		YEAR(CurrentDate)-1)  + '-' + convert(varchar(4), YEAR(CurrentDate)%100)    
		ELSE convert(varchar(4),YEAR(CurrentDate))+ '-' + convert(varchar(4),
		(YEAR(CurrentDate)%100)+1)END)  FINANCIAL_YEAR ,
    ROW_NUMBER() OVER (ORDER BY YEAR(CurrentDate) , MONTH(CurrentDate) ) AS INDEX_NUMBER
FROM DateRange
OPTION (MAXRECURSION 5475)
