test_that("basic sql created correctly", {
  check_sql <- glue::glue_sql(
  "SELECT column_name, data_type, CHARACTER_MAXIMUM_LENGTH
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_CATALOG = 'test_db'
  AND TABLE_SCHEMA = 'test_schema'
  AND TABLE_NAME = 'test_tbl'")

  expect_equal(col_query("test_db", "test_schema", "test_tbl"),
               check_sql)
})

test_that("char col lengths", {
  col_info_init <- readRDS(test_path(
    "testdata",
    "test_columns_info_init.rds"
  ))

  col_info_updated <- readRDS(test_path(
    "testdata",
    "test_columns_info_updated.rds"
  ))

  expect_equal(update_col_query(col_info_init),
               col_info_updated)
})

test_that("table stats SQL ok", {

  col_info_updated <- readRDS(test_path(
    "testdata",
    "test_columns_info_updated.rds"
  ))

  check_sql <- readRDS(test_path(
    "testdata",
    "test_columns_info_sql.rds"
  ))


  expect_equal(get_table_stats(1,
                               col_info_updated,
                               "test_schema",
                               "test_iris"),
               check_sql)
})
