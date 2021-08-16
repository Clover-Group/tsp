package ru.itclover.tsp.spark.io

import ru.itclover.tsp.spark.utils.RowWithIdx
import ru.itclover.tsp.streaming.io.{JDBCInputConf => GenericJDBCInputConf, KafkaInputConf => GenericKafkaInputConf}

object InputConf {
  type JDBCInputConf = GenericJDBCInputConf[RowWithIdx]
  type KafkaInputConf = GenericKafkaInputConf[RowWithIdx]
}