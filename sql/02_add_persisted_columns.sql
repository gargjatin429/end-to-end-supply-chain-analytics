/*
Purpose:
    Add a derived, persisted date column to the Fact_Sales table.

Why this is separate:
    - The base table stores atomic date components (year, month, day)
    - This column reconstructs a full DATE for analytical convenience
    - Keeping it separate avoids coupling schema creation with enrichment

Notes:
    - This column is computed and PERSISTED
    - SQL Server will store the value physically
    - Improves query simplicity and BI compatibility
*/

ALTER TABLE Fact_Sales
ADD order_date AS DATEFROMPARTS(order_year, order_month, order_day) PERSISTED;
GO
