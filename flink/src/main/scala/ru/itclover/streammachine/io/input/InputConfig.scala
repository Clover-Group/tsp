package ru.itclover.streammachine.io.input

import java.util.UUID

trait InputConfig


case class JDBCInputConfig(jdbcUrl: String,
                           query: String,
                           driverName: String,
                           datetimeColname: Symbol,
                           partitionColnames: Seq[Symbol],
                           userName: Option[String] = None,
                           password: Option[String] = None
                          ) extends InputConfig



case class FileConfig(filePath: String) extends InputConfig

case class KafkaConfig(brokers: String, topic: String, group: String = UUID.randomUUID().toString,
                       offsetReset: String = "largest")
