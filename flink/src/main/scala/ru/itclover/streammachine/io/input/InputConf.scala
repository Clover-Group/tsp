package ru.itclover.streammachine.io.input

import java.util.UUID

trait InputConf extends Serializable


case class JDBCInputConf(jdbcUrl: String,
                         query: String,
                         driverName: String,
                         datetimeColname: Symbol,
                         partitionColnames: Seq[Symbol],
                         userName: Option[String] = None,
                         password: Option[String] = None
                        ) extends InputConf

case class JDBCNarrowInputConf(jdbcConf: JDBCInputConf,
                               keyColname: Symbol,
                               valColname: Symbol,
                               fieldsTimeoutsMs: Map[Symbol, Long]
                              ) extends InputConf


case class FileConf(filePath: String) extends InputConf

case class KafkaConf(brokers: String, topic: String, group: String = UUID.randomUUID().toString,
                     offsetReset: String = "largest")
