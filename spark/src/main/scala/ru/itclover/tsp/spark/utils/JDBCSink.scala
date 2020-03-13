package ru.itclover.tsp.spark.utils

case class JDBCSink(url: String, table: String, driver: String, user: String, pwd: String)
    extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    val query = s"INSERT INTO $table VALUES (${value.mkString(",")})"
    statement.executeUpdate(query)
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}
