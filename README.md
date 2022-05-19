# RtoSQLServerVersioning
Work in progress. R package used to import R dataframes into a MS SQL Server database with [System Versioning](https://docs.microsoft.com/en-us/sql/relational-databases/tables/creating-a-system-versioned-temporal-table?view=sql-server-ver15) enabled.  

Functions are also currently available to: 
- List all tables in a specified schema of a database. 
- Select all rows of specific columns of a database table.
- Drop a table from the database.  

## Installation
R package can be installed directly from Github or locally from zip.  

While this repo is private, to install directly from Github will need to create a [Github PAT](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) and then can use your PAT to install in R (assuming you've been given access to the repository):

```r
# install.packages("devtools")
devtools::install_github("datasciencescotland/rtosqlserverversioning@main", auth_token = "<my personal access token>")
```

If the above does not work, install by downloading:

1. Go to the [repository on Github](https://github.com/datasciencescotland/rtosqlserverversioning)
2. Click Clone or download then Download ZIP.
3. Save the file locally and unzip.
4. Install with install.packages():
```
install.packages("C:/my_repos/RtoSQLServerVersioning", repos = NULL, type="source")
```

**IMPORTANT:** The [ODBC library](https://CRAN.R-project.org/package=odbc) version should be at least 1.3.3 as crashing issues were found when using 1.3.2.

## Example Usage
A work in progress, here is an example using the main functions:
```r
#Set connection details for use in functions:
database <- "my_database_name"
server <- "server\\instance"
schema <- "my_schema_name"

#Write the example Iris dataframe to a SQL Server table with system versioning (history table and start / end timestamps)
write_dataframe_to_db(database=database, server=server, schema=schema, table_name="test_iris", dataframe=iris, versioned_table=TRUE)

#Read the SQL Server table into an R dataframe
read_df <- read_table_from_db(database=database, server=server, schema=schema, table_name="test_iris")

#Drop the table from the database
drop_table_from_db(database=database, server=server, schema=schema, table_name="test_iris")

```

Important to note that the `write_dataframe_to_db` function will overwrite the table in the database if it already exists. If the table had versioning on when first created then the existing records will be written to the `<table name>History` table and the end timestamp column will be updated. 

The versioning status of a table is set when it is first created in the schema and it will not be changed with subsequent overwriting of an existing table even if the `versioned_table` parameter of `write_database_to_db` function is changed for subsequent imports. If wish to make a table versioned when it was not previously or vice-versa, use the `drop_table_from_db` function and then create the table again with `write_dataframe_to_db` setting `versioned_table` to TRUE or FALSE as required. 
