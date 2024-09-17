test_that("drop col sql works", {
  test_sql <- glue::glue_sql(
    "ALTER TABLE \"test_schema\".\"test_tbl\" \\
     DROP COLUMN \"val_col\";"
  )


  expect_equal(create_drop_column_sql(
    "test_schema",
    "test_tbl",
    "val_col"
  ), test_sql)
})
