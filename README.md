# SQlBulkInsertDatatable
Illustrates use of SqlBulkCopy async WriteToServerAsyc method
reads a CSV file into a DataTable which is passed to the method.
Zip of sample csv file and SQL script to create database and table in SQL server are included

Example run:
Read in 5949ms  and  inserted 438309 rows to database in 4252ms.
By splitting the large datatable in to 4 chunks and running in parallel, we cut that time in half.