test_that("filtered delete sql works", {
  test_sql <- glue::glue(
    "DELETE FROM \"test_schema\".\"test_tbl\" \\
     WHERE \"val_col\" = 'a';"
  )

  initial_sql <- delete_sql("test_schema", "test_tbl")

  filter_stmt <- "val_col == 'a'"


  expect_equal(add_filter_sql(initial_sql, filter_stmt), test_sql)
})
