# Read database table with column or row filtering

[`read_table_from_db()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/read_table_from_db.md)
includes optional arguments so you can read only specific columns into
an R dataframe, or so you can filter which rows are read.

To select only specific columns, specify a vector of column_names for
the `columns` argument of
[`read_table_from_db()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/read_table_from_db.md).
For example, `c("Sepal_Length", "Species")`.

To select only specific rows, specify a filter string using R syntax
(not SQL syntax) for the `filter_stmt` argument. For example
`"(Species == 'setosa' | Species == 'virginica') & Sepal_Length > 5.0"`.
The function will convert this to SQL.

If you are unsure of column names in an existing database table, use the
[`db_table_metadata()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/db_table_metadata.md)
function to check.

Here’s a full example of both column and row selection using the
`columns` and `filter_stmt` arguments of `read_table_from_db` for a copy
of the iris data loaded into the database:

``` r

library(RtoSQLServer)

db_test_iris <- read_table_from_db(
  server = server,
  database = database,
  schema = schema,
  table_name = "test_iris",
  columns = c("Sepal_Length", "Species"),
  filter_stmt = "(Species == 'setosa' | Species == 'virginica') & Sepal_Length > 5.0",
  include_pk = FALSE
)
```

For the `filter_stmt` argument, this should be passed as a string. If
you find the syntax confusing when wrapping your filter within quotation
marks, then instead try using `deparse1(substitute())`. For example, the
above filter example could be passed as
`deparse1(substitute((Species == 'setosa' | Species == 'virginica') & Sepal_Length > 5.0))`.

If you are comfortable with SQL, you may prefer to use the
[`execute_sql()`](https://scotgovanalysis.github.io/RtoSQLServer/reference/execute_sql.md)
function where a SQL select statement can be input to return an R
dataframe from a database.
