test_that("rename_table_sql generates correct SQL statement", {
  mockery::stub(
    rename_table_sql, "quoted_schema_tbl",
    DBI::SQL("\"test_schema\".\"test_old_tbl\"")
  )

  sql <- rename_table_sql(
    "test_schema",
    "test_old_tbl",
    "test_new_tbl"
  )

  expected_sql <- glue::glue_sql(
    "EXEC sp_rename '\"test_schema\".\"test_old_tbl\"', \\
    \"test_new_tbl\", 'OBJECT';",
    .con = DBI::ANSI()
  )

  expect_equal(sql, expected_sql)
})

test_that("rename_table stops if old table does not exist", {
  mockery::stub(rename_table, "check_table_exists", FALSE)

  expect_error(
    rename_table("server", "database", "schema", "old_table", "new_table"),
    "Table: schema.old_table does not exist in the database."
  )
})

test_that("rename_table stops if new table already exists", {
  mockery::stub(rename_table, "check_table_exists", TRUE)
  mockery::stub(rename_table, "check_table_exists", TRUE, 2)

  expect_error(
    rename_table("server", "database", "schema", "old_table", "existing_table"),
    "Table: schema.existing_table already exists in the database."
  )
})

test_that("rename_table cleans the new table name", {
  mockery::stub(rename_table, "check_table_exists", mockery::mock(TRUE, FALSE))
  mockery::stub(rename_table, "rename_table_sql", "SQL_QUERY")
  mockery::stub(rename_table, "execute_sql", TRUE)

  expect_warning(
    rename_table("server", "database", "schema", "old_table", "new-table-name"),
    "Cannot name a table new-table-name replacing with name new_table_name"
  )
})

test_that("rename_table returns correct completion message", {
  mockery::stub(rename_table, "check_table_exists", mockery::mock(TRUE, FALSE))
  mockery::stub(rename_table, "clean_table_name", "new_table_name")
  mockery::stub(rename_table, "rename_table_sql", "SQL_QUERY")
  mockery::stub(rename_table, "execute_sql", TRUE)

  expect_message(
    rename_table("server", "database", "schema", "old_table", "new_table"),
    "Table old_table renamed to new_table_name in schema schema.",
    fixed = TRUE
  )
})
