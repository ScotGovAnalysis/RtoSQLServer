test_that("column formatting works", {
  test_cols <- "[col_a], [col_b]"
  cols <- c("col_a", "col_b")
  expect_equal(table_select_list(cols), test_cols)
})

test_that("select sql works", {
  test_sql <- "SELECT [col_a], [col_b] FROM [test_schema].[test_tbl];"
  select_list <- "[col_a], [col_b]"
  expect_equal(create_read_sql(
    select_list,
    "test_schema",
    "test_tbl"
  ), test_sql)
})
