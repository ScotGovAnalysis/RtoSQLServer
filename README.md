# RtoSQLServerVersioning
R package used to import R dataframes into a MS SQL Server database, optionally with [System Versioning](https://docs.microsoft.com/en-us/sql/relational-databases/tables/creating-a-system-versioned-temporal-table?view=sql-server-ver15) enabled.  

Functions are also currently available to: 
- List all tables in a specified schema of a database. 
- Select all rows of specific columns of a database table.
- Drop a table from the database.

## Method used
When loading an R dataframe into SQL Server using `create_replace_table`, following steps are followed:

1. The R dataframe is loaded into a staging table in the database in batches of n rows at a time.

2. a) If table of the specified name does NOT already exist in the database schema:  
      i) Create target table in the database.  
      ii)Insert all rows from staging table to target table.

3. b) If table of same name does already exist in the database schema:  

    If 'append_to_existing'=FALSE:  
      i) delete all rows from the target table  
      ii) Insert all rows from staging table into target table.  

    If 'append_to_existing'=TRUE:  
      i) Check that staging table columns and existing target table columns are the same. If not, cancel loading and give a warning.  
      ii) If check passes, insert all rows from staging table into target table.  

4. Delete the staging table.

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
#Make a test dataframe with n rows
test_n_rows <- 1234567
test_df <- data.frame(a=rep("a", test_n_rows), b=rep("b", test_n_rows))

#Set database connection details for use in functions:
server <- "server\\instance"
database <- "my_database_name"
schema <- "my_schema_name"

#Write the test dataframe to a SQL Server table in 100K batches (by default system versioning is FALSE)
write_dataframe_to_db(server=server, 
                      database=database, 
                      schema=schema, 
                      table_name="test_r_tbl", 
                      dataframe=iris, 
                      append_to_existing = FALSE,
                      batch_size=1e5, 
                      versioned_table=FALSE)

#Read the SQL Server table into an R dataframe
read_df <- read_table_from_db(server=server, 
                              database=database, 
                              schema=schema, 
                              table_name="test_r_tbl")

#Drop the table from the database
drop_table_from_db(server=server, 
                   database=database, 
                   schema=schema, 
                   table_name="test_r_tbl")

```

The `write_dataframe_to_db` function will overwrite the table in the database if it already exists and argument `append_to_existing=FALSE`. If the table had versioning on when first created then the existing records will be written to the `<table name>History` table and the end timestamp column will be updated. 


Note a later version of SQL Server (2016 or later) is required for the System Versioning functionality. If using MS SQL Server 2012 or earlier, will only be successful if use version_table = FALSE arguments to these functions.
