package ru.itclover.tsp.spark.io

import org.apache.spark.sql.Row
import ru.itclover.tsp.streaming.io.{JDBCOutputConf => GenericJDBCOutputConf, KafkaOutputConf => GenericKafkaOutputConf}


object OutputConf {
  type JDBCOutputConf = GenericJDBCOutputConf[Row]
  type KafkaOutputConf = GenericKafkaOutputConf[Row]
}
