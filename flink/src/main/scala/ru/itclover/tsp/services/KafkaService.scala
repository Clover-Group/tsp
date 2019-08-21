package ru.itclover.tsp.services

import scala.util.Try
import ru.itclover.tsp.io.input.KafkaInputConf

object KafkaService {

  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = ???

  // def fetchFieldsTypesInfo(driverName: String, jdbcUrl: String, query: String): Try[Seq[(Symbol, Class[_])]] = {
  //   val classTry: Try[Class[_]] = Try(Class.forName(driverName))

  //   // val connectionTry = Try(DriverManager.getConnection(jdbcUrl))
  //   for {
  //      _          <- classTry
  //     // connection <- connectionTry
  //     // resultSet  <- Try(connection.createStatement().executeQuery(s"SELECT * FROM (${query}) as mainQ LIMIT 1"))

  //     metaData   = Try("hello")
  //   } yield {
  //     // (1 to metaData.getColumnCount) map { i: Int =>
  //     (1 to 1) map { i: Int =>
  //       // val className = 'metaData.getColumnClassName(i)
  //       val className = "Int"
  //       Symbol("test", Class.forName(className)
  //     }
  //   }
  // }

}
