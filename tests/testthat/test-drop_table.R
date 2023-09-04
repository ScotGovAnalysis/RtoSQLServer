test_that("table not exist error works", {
  mockery::stub(drop_table_from_db, "check_table_exists", FALSE)
  expect_error(drop_table_from_db("test", "test", "test", "test"))
})


test_that("versioned drop sql created correctly", {
  check_df <- data.frame(
    temporal_type_desc = "SYSTEM_VERSIONED_TEMPORAL_TABLE"
  )

  drop_ver_sql <- DBI::SQL(paste0(
    "ALTER TABLE \"test_schema\".\"test_tbl\" ",
    "SET ( SYSTEM_VERSIONING = OFF );",
    "DROP TABLE \"test_schema\".\"test_tbl\";",
    "DROP TABLE \"test_schema\".\"test_tblHistory\";"
  ))

  mockery::stub(create_drop_sql, "execute_sql", check_df)

  expect_equal(create_drop_sql(
    "test", "test", "test_schema", "test_tbl", TRUE
  ), drop_ver_sql)
})

test_that("non-versioned drop sql created correctly", {
  check_df <- data.frame(
    temporal_type_desc = "NOT APPLICABLE"
  )

  drop_nonver_sql <- DBI::SQL("DROP TABLE \"test_schema\".\"test_tbl\";")

  mockery::stub(create_drop_sql, "execute_sql", check_df)

  expect_equal(create_drop_sql(
    "test", "test", "test_schema", "test_tbl", FALSE
  ), drop_nonver_sql)
})

test_that("user error non-versioned drop sql created correctly", {
  check_df <- data.frame(
    temporal_type_desc = "NOT APPLICABLE"
  )

  drop_nonver_sql <- DBI::SQL("DROP TABLE \"test_schema\".\"test_tbl\";")

  mockery::stub(create_drop_sql, "execute_sql", check_df)

  expect_equal(create_drop_sql(
    "test", "test", "test_schema", "test_tbl", TRUE
  ), drop_nonver_sql)
})
