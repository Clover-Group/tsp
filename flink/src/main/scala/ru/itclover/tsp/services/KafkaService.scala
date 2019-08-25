package ru.itclover.tsp.services

import scala.util.Try
import ru.itclover.tsp.io.input.KafkaInputConf

object KafkaService {

  // Stub
  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = Try(Seq(
    ('and, classOf[Int]), 
    ('ts, classOf[String]),
    ('SpeedEngine, classOf[Double]),
    ('Speed, classOf[Double]),
    ('loco_num, classOf[String]),
    ('Section, classOf[String]),
    ('upload_id, classOf[String]),
    
    ))

}
