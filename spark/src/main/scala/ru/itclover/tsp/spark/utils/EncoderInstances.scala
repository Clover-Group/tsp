package ru.itclover.tsp.spark.utils

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.types.StructType
import ru.itclover.tsp.core.Incident

import scala.reflect.ClassTag

object EncoderInstances {
  implicit val incidentEncoder: Encoder[Incident] = new Encoder[Incident] {
    override def schema: StructType = ???

    override def clsTag: ClassTag[Incident] = ???
  }

  implicit val sinkRowEncoder: Encoder[Row] = ???
}
