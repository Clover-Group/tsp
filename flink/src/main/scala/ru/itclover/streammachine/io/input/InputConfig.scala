package ru.itclover.streammachine.io.input

import java.util.UUID

trait InputConfig {

}

case class JDBCConfig(jdbcUrl: String,
                      query: String,
                      driverName: String,
                      userName: Option[String] = None,
                      password: Option[String] = None
                     ) extends InputConfig

case class FileConfig(filePath: String) extends InputConfig

case class KafkaConfig(brokers: String, topic: String, group: String = UUID.randomUUID().toString,
                       offsetReset: String = "largest")

