package ru.itclover.tsp.services

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ObjectNode, ValueNode}

import scala.util.Try
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerRecord
import ru.itclover.tsp.io.input.KafkaInputConf

object KafkaService {

  // Stub
  def fetchFieldsTypesInfo(conf: KafkaInputConf): Try[Seq[(Symbol, Class[_])]] = Try(
    Seq(
      ('and, classOf[Int]),
      ('ts, classOf[String]),
      ('SpeedEngine, classOf[Double]),
      ('Speed, classOf[Double]),
      ('loco_num, classOf[String]),
      ('Section, classOf[String]),
      ('upload_id, classOf[String])
    )
  )

  // Stub
  def consumer(conf: KafkaInputConf, fieldsIdxMap: Map[Symbol, Int]) = {
    val props = new Properties
    props.setProperty("bootstrap.servers", conf.brokers)
    props.setProperty("group.id", conf.group)
    // //props.setProperty("client.id", "client0")
    props.setProperty("auto.offset.reset", "earliest"); // Always read topic from start

    new FlinkKafkaConsumer(conf.topic, new RowDeserializationSchema(fieldsIdxMap), props)
  }
}

class RowDeserializationSchema(fieldsIdxMap: Map[Symbol, Int]) extends KafkaDeserializationSchema[Row] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): Row = {
    val msgString = new String(record.value())
    val node = new ObjectMapper().readTree(msgString)
    val row = new Row(fieldsIdxMap.size)
    fieldsIdxMap.foreach {
      case (name, index) =>
        val mapper = new ObjectMapper()
        val fieldValue = mapper.convertValue(node.get(name.toString), classOf[java.lang.Object])
        row.setField(index, fieldValue)
    }
    row
  }

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = TypeInformation.of(classOf[Row])
}