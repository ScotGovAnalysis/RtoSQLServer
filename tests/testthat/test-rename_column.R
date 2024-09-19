test_that("rename_column_sql generates correct SQL statement", {
  mockery::stub(
    rename_column_sql,
    "quoted_schema_tbl",
    DBI::SQL("\"test_schema\".\"test_tbl\"")
  )

  sql <- rename_column_sql(
    "test_schema",
    "test_tbl",
    "old_column",
    "new_column"
  )

  expected_sql <- glue::glue_sql(
    "EXEC sp_rename '\"test_schema\".\"test_tbl\".\"old_column\"', \\
    \"new_column\", 'COLUMN';",
    .con = DBI::ANSI()
  )

  expect_equal(sql, expected_sql)
})

test_that("rename_column stops if table does not exist", {
  mockery::stub(rename_column, "check_table_exists", FALSE)

  expect_error(
    rename_column(
      "server",
      "database",
      "schema",
      "table_name",
      "old_column",
      "new_column"
    ),
    "Table: schema.table_name does not exist in the database."
  )
})

test_that("rename_column stops if old column does not exist", {
  mockery::stub(rename_column, "check_table_exists", TRUE)
  mockery::stub(
    rename_column,
    "db_table_metadata",
    data.frame(column_name = c("existing_column"))
  )

  expect_error(
    rename_column(
      "server",
      "database",
      "schema",
      "table_name",
      "non_existent_column",
      "new_column"
    ),
    "Column non_existent_column does not exist in schema.table_name."
  )
})

test_that("rename_column stops if new column already exists", {
  mockery::stub(rename_column, "check_table_exists", TRUE)
  mockery::stub(
    rename_column,
    "db_table_metadata",
    data.frame(column_name = c("old_column", "new_column"))
  )

  expect_error(
    rename_column(
      "server",
      "database",
      "schema",
      "table_name",
      "old_column",
      "new_column"
    ),
    "Column new_column already exists in schema.table_name."
  )
})

test_that("rename_column cleans the new column name", {
  mockery::stub(rename_column, "check_table_exists", TRUE)
  mockery::stub(
    rename_column,
    "db_table_metadata",
    data.frame(column_name = c("old_column", "another_column"))
  )
  mockery::stub(rename_column, "rename_column_sql", "SQL_QUERY")
  mockery::stub(rename_column, "execute_sql", TRUE)

  expect_warning(
    rename_column(
      "server",
      "database",
      "schema",
      "table_name",
      "old_column",
      "test.column"
    ),
    "Column name test.column is invalid using test_column instead."
  )

  long_column_name <- paste0(rep("a", 130), collapse = "")
  truncated_column_name <- substr(long_column_name, 1, 126)

  expect_warning(
    rename_column(
      "server",
      "database",
      "schema",
      "table_name",
      "old_column",
      long_column_name
    ),
    glue::glue("Column name {long_column_name} \\
               is invalid using {truncated_column_name} instead.")
  )
})

test_that("rename_column generates and executes correct SQL", {
  mockery::stub(rename_column, "check_table_exists", TRUE)
  mockery::stub(
    rename_column,
    "db_table_metadata",
    data.frame(column_name = c("old_column", "another_column"))
  )
  mockery::stub(rename_column, "clean_new_column_name", "new_column")
  mockery::stub(rename_column, "rename_column_sql", "SQL_QUERY")
  mockery::stub(rename_column, "execute_sql", TRUE)

  expect_message(
    rename_column(
      "server",
      "database",
      "schema",
      "table_name",
      "old_column",
      "new_column"
    ),
    "Column old_column renamed to new_column in schema.table_name.",
    fixed = TRUE
  )
})
