# CSV file(s) to SQL Server table importer

Bulk imports CSV files into a SQL Server table. 

## Features
- Validates CSV file headers for uniformity
- Creates staging table with named user input and columns based on CSV structure
- Bulk inserts data with user input for row terminator
- Handles transaction rollbacks on failure
- Windows authentication default but can use SQL Server authentication
