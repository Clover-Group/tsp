package ru.itclover.streammachine.io.input

import java.util.UUID

trait InputConfig {

}

object StorageFormat extends Enumeration {
  type StorageFormat = Value
  // Tables of format: `ts, params, sensor_id, value` (value, possibly stored in 3 columns: int, double, string)
  val Narrow = Value("Narrow")
  val WideAndDense = Value("Wide")
}


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

