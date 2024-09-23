test_that("table sql created correctly", {
  sql <- glue::glue_sql("SELECT name AS 'table', \\
                         create_date AS 'creation_date' \\
                         FROM sys.tables \\
                         WHERE SCHEMA_NAME(schema_id) = 'test_schema' \\
                         order by 2;")


  expect_equal(table_sql("test_schema"), sql)
})


test_that("table and view sql created correctly", {
  sql <- glue::glue_sql("SELECT name AS 'table', \\
                         type_desc AS 'object_type', \\
                         create_date AS 'creation_date' \\
                         FROM sys.objects \\
                         WHERE type IN ('U', 'V')  -- 'U'ser tables, 'V'iews \\
                         AND SCHEMA_NAME(schema_id) = 'test_schema' \\
                         order by type, name;")


  expect_equal(table_view_sql("test_schema"), sql)
})
