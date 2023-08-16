test_that("column formatting works", {
  test_cols <- "[col_a], [col_b]"
  cols <- c("col_a", "col_b")
  expect_equal(table_select_list(cols), test_cols)
})

test_that("select sql works", {
  test_sql <- paste(
    "SELECT [col_a], [col_b] FROM [test_schema].[test_tbl]",
    "WHERE [col_a] = 'test1' AND [col_b] = 'test2';"
  )

  select_list <- "[col_a], [col_b]"

  filter_ex <- dbplyr::translate_sql(col_a == "test1" & col_b == "test2",
    con = dbplyr::simulate_mssql()
  )

  filter_ex <- gsub("(`|\")([^=]*)\\1", "[\\2]", filter_ex)

  mockery::stub(create_read_sql, "format_filter", filter_ex)

  expect_equal(create_read_sql(
    server = "test",
    database = "test",
    schema = "test_schema",
    select_list = select_list,
    table_name = "test_tbl",
    filter_stmt = "col_a == 'test1' & col_b = 'test2'"
  ), test_sql)
})
