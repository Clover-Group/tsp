package ru.itclover.streammachine.io.input

trait InputConfig {

}

case class ClickhouseConfig(jdbcUrl: String, query: String) extends InputConfig

case class FileConfig(filePath: String) extends InputConfig
