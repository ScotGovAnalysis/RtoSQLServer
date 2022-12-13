test_that("cleaning table name works", {
  unclean_name <- "this is-a! test?1"
  clean_name <- "this_is_a_test1"
  expect_equal(clean_table_name(unclean_name), clean_name)
})

test_that("cleaning column names works", {
  unclean_cols <- c("test_tblkey", "test.test", "Test.Test2")
  clean_cols <- c("test_tblkey_old", "test_test", "Test_Test2")
  unclean_df <- data.frame(a = 1, b = 2, c = 3)
  colnames(unclean_df) <- unclean_cols
  clean_df <- data.frame(a = 1, b = 2, c = 3)
  colnames(clean_df) <- clean_cols
  expect_identical(clean_column_names(unclean_df, "test_tbl"), clean_df)
})


test_that("create table sql correct", {
  correct_sql <- paste(
    "CREATE TABLE [test].[test_tbl]",
    "([test_tblID] INT NOT NULL IDENTITY PRIMARY KEY,",
    "[Sepal.Length] float,",
    "[Sepal.Width] float,",
    "[Petal.Length] float,",
    "[Petal.Width] float,",
    "[Species] nvarchar(50));"
  )
  metadata_df <- df_to_metadata(iris)
  expect_identical(
    sql_create_table("test", "test_tbl", metadata_df),
    correct_sql
  )
})
