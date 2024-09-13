test_that("table not exist error works", {
  mockery::stub(drop_table_from_db, "check_table_exists", FALSE)
  expect_error(drop_table_from_db("test", "test", "test", "test"))
})


test_that("versioned drop sql created correctly", {
  drop_ver_sql <- glue::glue_sql(
    "ALTER TABLE \"test_schema\".\"test_tbl\" \\
    SET ( SYSTEM_VERSIONING = OFF ); \\
    DROP TABLE \"test_schema\".\"test_tbl\"; \\
    DROP TABLE \"test_schema\".\"test_tblHistory\";"
  )

  mockery::stub(create_drop_sql, "is_versioned", TRUE)

  expect_equal(create_drop_sql(
    "test", "test", "test_schema", "test_tbl"
  )$drop_sql, drop_ver_sql)
})

test_that("non-versioned drop sql created correctly", {
  drop_nonver_sql <- DBI::SQL("DROP TABLE \"test_schema\".\"test_tbl\";")

  mockery::stub(create_drop_sql, "is_versioned", FALSE)

  expect_equal(create_drop_sql(
    "test", "test", "test_schema", "test_tbl"
  )$drop_sql, drop_nonver_sql)
})

test_that("user error non-versioned drop sql created correctly", {
  drop_nonver_sql <- DBI::SQL("DROP TABLE \"test_schema\".\"test_tbl\";")

  mockery::stub(create_drop_sql, "is_versioned", FALSE)

  expect_equal(create_drop_sql(
    "test", "test", "test_schema", "test_tbl"
  )$drop_sql, drop_nonver_sql)
})
