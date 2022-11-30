
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RtoSQLServer

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
<!-- badges: end -->

R package used to import R dataframes into a MS SQL Server database,
optionally with [System
Versioning](https://docs.microsoft.com/en-us/sql/relational-databases/tables/creating-a-system-versioned-temporal-table?view=sql-server-ver15)
enabled.

## Installation

R package can be installed directly from Github or locally from zip.

``` r
# install.packages("devtools")
devtools::install_github("datasciencescotland/rtosqlserver@main")
```

If the above does not work, install by downloading:

1.  Go to the [repository on
    Github](https://github.com/datasciencescotland/rtosqlserverver)
2.  Click Clone or download then Download ZIP.
3.  Save the file locally and unzip.
4.  Install with install.packages():

<!-- end list -->

    install.packages("C:/my_repos/RtoSQLServer", repos = NULL, type="source")

## Functionality

As well as loading R dataframes into SQL Server databases, functions are
also currently available to: - Select all rows of specific columns of a
database table. - Drop a table from the database. - Run any other input
sql in the database and return dataframe if a select statement.

It is recommend to ensure using the latest versions of
[Rcpp](https://cran.r-project.org/web/packages/Rcpp/index.html),
[odbc](https://cran.r-project.org/web/packages/odbc/index.html) and
[DBI](https://cran.r-project.org/web/packages/DBI/index.html). If using
Windows in a secure environment install these from source or a Windows
binary compiled at the version of R you are using.

## Loading method used

When loading an R dataframe into SQL Server using
`write_dataframe_to_db`, following steps are followed:

1.  The R dataframe is loaded into a staging table in the database in
    batches of n rows at a time.

2.  1)  If table of the specified name does NOT already exist in the
        database schema:
        1)  Create target table in the database.  
        2)  Insert all rows from staging table to target table.

3.  2)  If table of same name does already exist in the database schema:
    
    If ‘append\_to\_existing’=FALSE (this will result in an overwrite):
    
    1)  Drop the existing copy of the target table and create a new one
        from staging table definition.  
    2)  Insert all rows from staging table into target table.
    
    If ‘append\_to\_existing’=TRUE:
    
    1)  Check that staging table columns and existing target table
        columns are the same. If not, cancel loading and give a
        warning.  
    2)  If check passes, insert all rows from staging table into target
        table.

4.  Delete the staging table.

## Example Usage

Here is an example using the main functions: \`\`\`r \# Make a test
dataframe with n rows test\_n\_rows \<- 1234567 test\_df \<-
data.frame(a=rep(“a”, test\_n\_rows), b=rep(“b”, test\_n\_rows))

# Set database connection details for use in functions:

server \<- “server\\instance” database \<- “my\_database\_name” schema
\<- “my\_schema\_name”

# Write the test dataframe to a SQL Server table in 100K batches (by default system versioning is FALSE)

write\_dataframe\_to\_db(server=server, database=database,
schema=schema, table\_name=“test\_r\_tbl”, dataframe=test\_df,
append\_to\_existing = FALSE, batch\_size=1e5, versioned\_table=FALSE)

# Read the SQL Server table into an R dataframe

read\_df \<- read\_table\_from\_db(server=server, database=database,
schema=schema, table\_name=“test\_r\_tbl”)

# Run other SQL for example select specific column

sql \<- paste0(“select a from”, schema, “.test\_r\_tbl”)

read\_selected\_df \<- execute\_sql(server=server, database=database,
sql=sql, output=TRUE)

# Drop the table from the database

drop\_table\_from\_db(server=server, database=database, schema=schema,
table\_name=“test\_r\_tbl”)
