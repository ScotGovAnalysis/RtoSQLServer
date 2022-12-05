test_that("SQL Server data type mapping works", {
  expect_equal(r_to_sql_datatype(iris$Species), "nvarchar(50)")
  expect_equal(r_to_sql_datatype(iris$Petal.Length), "float")
})

test_that("Compatible character col check works", {
  expect_equal(compatible_character_cols(
    "float",
    "nvarchar(50"
  ), "incompatible")
  expect_equal(compatible_character_cols(
    "nvarchar(max)",
    "nvarchar(50)"
  ), "compatible")
  expect_equal(compatible_character_cols(
    "nvarchar(50)",
    "nvarchar(max)"
  ), "resize")
  expect_equal(compatible_character_cols(
    "nvarchar(255)",
    "nvarchar(255)"
  ), "compatible")
  expect_equal(compatible_character_cols(
    "nvarchar(50)",
    "nvarchar(255)"
  ), "resize")
})

test_that("Can extract col size from nvarchar string", {
  expect_equal(get_nvarchar_size("nvarchar(max)"), "max")
  expect_equal(get_nvarchar_size("nvarchar(50)"), "50")
})

test_that("Char size to nvarchar type mapping works", {
  expect_equal(r_to_sql_character_sizes("10"), "nvarchar(50)")
  expect_equal(r_to_sql_character_sizes("255"), "nvarchar(255)")
  expect_equal(r_to_sql_character_sizes("4000"), "nvarchar(4000)")
  expect_equal(r_to_sql_character_sizes("4001"), "nvarchar(max)")
})

test_that("Message printing wrap fn works", {
  test_msg <- "multi-line
  message"
  expect_equal(format_message(test_msg), "multi-line message")
})

test_that("Formatting column select sql works", {
  expect_equal(table_select_list(c("species", "petal_length")), "[species], [petal_length]")
})