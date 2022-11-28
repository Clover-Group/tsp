package ru.itclover.tsp.streaming.services

import java.sql.DriverManager

import scala.util.Try

object JdbcService {

  def fetchFieldsTypesInfo(driverName: String, jdbcUrl: String, query: String): Try[Seq[(String, Class[_])]] = {
    val classTry: Try[Class[_]] = Try(Class.forName(driverName))

    val connectionTry = Try(DriverManager.getConnection(jdbcUrl))
    for {
      _          <- classTry
      connection <- connectionTry
      resultSet  <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${query}) as mainQ LIMIT 1"))
      metaData   <- Try(resultSet.getMetaData)
    } yield {
      (1 to metaData.getColumnCount).map { (i: Int) =>
        val className = metaData.getColumnClassName(i)
        (String(metaData.getColumnName(i)), Class.forName(className))
      }
    }
  }

  def fetchAvailableKeys(driverName: String, jdbcUrl: String, query: String, keyColumn: String): Try[Set[String]] = {
    val classTry: Try[Class[_]] = Try(Class.forName(driverName))

    val connectionTry = Try(DriverManager.getConnection(jdbcUrl))
    for {
      _          <- classTry
      connection <- connectionTry
      resultSet  <- Try(connection.createStatement().executeQuery(s"SELECT DISTINCT(${keyColumn}) FROM (${query})"))
    } yield {
      new Iterator[String] {
        def hasNext = resultSet.next()
        def next() = resultSet.getString(1)
      }.toStream.map(String(_)).toSet
    }
  }
}
