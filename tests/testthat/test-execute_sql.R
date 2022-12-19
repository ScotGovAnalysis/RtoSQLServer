test_that("Output TRUE calls correct DBI function", {
  mockery::stub(execute_sql, "create_sqlserver_connection", 1)
  mockery::stub(execute_sql, "DBI::dbDisconnect", 1)

  m <- mockery::mock(1)
  mockery::stub(execute_sql, "DBI::dbGetQuery", m)
  mockery::stub(execute_sql, "DBI::dbExecute", 1)

  execute_sql("test", "test", "test", TRUE)

  mockery::expect_called(m, 1)
})


test_that("Output FALSE calls correct DBI function", {
  mockery::stub(execute_sql, "create_sqlserver_connection", 1)
  mockery::stub(execute_sql, "DBI::dbDisconnect", 1)

  m <- mockery::mock(1)
  mockery::stub(execute_sql, "DBI::dbGetQuery", 1)
  mockery::stub(execute_sql, "DBI::dbExecute", m)

  execute_sql("test", "test", "test", FALSE)

  mockery::expect_called(m, 1)
})


test_that("Output FALSE returns message", {
  fake_sql <- "delete from test"
  fake_msg <- paste0("SQL: ", fake_sql, "\nexecuted successfully")

  mockery::stub(execute_sql, "create_sqlserver_connection", 1)
  mockery::stub(execute_sql, "DBI::dbDisconnect", 1)

  mockery::stub(execute_sql, "DBI::dbGetQuery", 1)
  mockery::stub(execute_sql, "DBI::dbExecute", 1)

  expect_equal(execute_sql("test", "test", fake_sql, FALSE), fake_msg)
})
