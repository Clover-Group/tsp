package ru.itclover.tsp.services

import scala.util.Try
import ru.itclover.tsp.io.input.KafkaInputConf

object KafkaService {

  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = Try(Seq(('and, classOf[Int])))

}
