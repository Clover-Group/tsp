package ru.itclover.tsp.services

import scala.util.Try
//import org.apache.kafka.clients.consumer.ConsumerRecord
import ru.itclover.tsp.streaming.io.KafkaInputConf

class StreamEndException(message: String) extends Exception(message)

object KafkaService {

  // Stub
  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = Try(conf.fieldsTypes.map {
    case (fieldName, fieldType) =>
      val fieldClass = fieldType match {
        case "int8"    => classOf[Byte]
        case "int16"   => classOf[Short]
        case "int32"   => classOf[Int]
        case "int64"   => classOf[Long]
        case "float32" => classOf[Float]
        case "float64" => classOf[Double]
        case "boolean" => classOf[Boolean]
        case "string"  => classOf[String]
        case _         => classOf[Any]
      }
      (Symbol(fieldName), fieldClass)
  }.toSeq)

  // Stub
  /*def consumer(conf: KafkaInputConf, fieldsIdxMap: Map[Symbol, Int]) = {
    val props = new Properties
    props.setProperty("bootstrap.servers", conf.brokers)
    props.setProperty("group.id", conf.group)
    // //props.setProperty("client.id", "client0")
    props.setProperty("auto.offset.reset", "earliest") // Always read topic from start if no offset is provided
    props.setProperty("enable.auto.commit", "true") // Enable auto committing when checkpointing is disabled
    props.setProperty("auto.commit.interval.ms", "1000") // Enable auto committing when checkpointing is disabled

    val deserializer = conf.serializer.getOrElse("json") match {
      case "json"  => new RowDeserializationSchema(fieldsIdxMap)
      case _       => throw new IllegalArgumentException(s"No deserializer for type ${conf.serializer}")
    }

    new FlinkKafkaConsumer(conf.topic, deserializer, props)
  }*/
}
