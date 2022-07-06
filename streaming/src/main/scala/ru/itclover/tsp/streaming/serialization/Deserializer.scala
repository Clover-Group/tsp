package ru.itclover.tsp.streaming.serialization

import ru.itclover.tsp.StreamSource.Row
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{BaseJsonNode, ObjectNode}

import scala.util.Try

trait Deserializer {
  def deserialize(data: Array[Byte]): Either[Throwable, Row]
}

case class JsonDeserializer(fields: Seq[(Symbol, Class[_])]) extends Deserializer {
  override def deserialize(data: Array[Byte]): Either[Throwable, Row] = {
    val mapper = new ObjectMapper
    Try(mapper.readTree(data)).toEither.flatMap {
      case objectNode: ObjectNode => {
        val row = new Row(fields.length)
        fields.zipWithIndex.foreach {
          case (field, idx) =>
            val node = objectNode.get(field._1.name)
            val value: Any = field._2 match {
              case c if c.isAssignableFrom(classOf[Byte]) => node.shortValue
              case c if c.isAssignableFrom(classOf[Short]) => node.shortValue
              case c if c.isAssignableFrom(classOf[Int]) => node.intValue
              case c if c.isAssignableFrom(classOf[Long]) => node.longValue
              case c if c.isAssignableFrom(classOf[Float]) => node.floatValue
              case c if c.isAssignableFrom(classOf[Double]) => node.doubleValue
              case c if c.isAssignableFrom(classOf[String]) => node.textValue
            }
            row(idx) = value.asInstanceOf[AnyRef]
        }
        Right(row)
      }
      case data => Left(new IllegalArgumentException(s"object expected, $data found"))
    }
  }
}