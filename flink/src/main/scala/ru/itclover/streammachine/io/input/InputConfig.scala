package ru.itclover.streammachine.io.input

import java.util.UUID

trait InputConfig {

}

case class ClickhouseConfig(jdbcUrl: String, query: String) extends InputConfig

case class FileConfig(filePath: String) extends InputConfig

case class KafkaConfig(brokers: String, topic: String, group: String = UUID.randomUUID().toString, offsetReset: String = "largest")

