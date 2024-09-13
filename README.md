# Task-9-monthly_data


This PySpark script performs the following operations:

Initialization of Spark Session: A Spark session is created with the application name 'Split_by_Month'.

Schema Definition: The schema for the input CSV file is explicitly defined using StructType. This schema defines the structure of the CSV, including data types for each field.

Data Loading: The CSV file (Sales_SalesOrderDetail.csv) is loaded into a DataFrame using the defined schema.

Adding Month Column: A new column Month is added to the DataFrame, which extracts the month from the OrderDate field using the month function.

Aggregation: The data is grouped by the Month column, and two aggregations are performed:

Order Count: The total number of sales orders for each month is counted using the count() function on the SalesOrderID column.
Total Sales: The sum of LineTotal is computed for each month using the sum() function.
Results Display: The aggregated DataFrame is displayed, showing the Month, OrderCount, and TotalSales for each month.
