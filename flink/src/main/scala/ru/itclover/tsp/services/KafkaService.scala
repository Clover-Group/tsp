package ru.itclover.tsp.services

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ObjectNode, ValueNode}

import scala.util.Try
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import ru.itclover.tsp.io.input.KafkaInputConf

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
    val mapper = new ObjectMapper()
    fieldsIdxMap.foreach {
      case (name, index) =>
        val v = node.get(name.name)
        val fieldValue = mapper.convertValue(v, classOf[java.lang.Object])
        row.setField(index, fieldValue)
    }
    row
  }

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = TypeInformation.of(classOf[Row])
}

class TimeOutFunction( // delay after which an alert flag is thrown
  val timeOut: Long
) extends ProcessFunction[Row, Row] {
  // state to remember the last timer set
  private var lastTimer: ValueState[Long] = _

  override def open(conf: Configuration): Unit = { // setup timer state
    val lastTimerDesc = new ValueStateDescriptor[Long]("lastTimer", classOf[Long])
    lastTimer = getRuntimeContext.getState(lastTimerDesc)
  }

  override def processElement(value: Row, ctx: ProcessFunction[Row, Row]#Context, out: Collector[Row]): Unit = { // get current time and compute timeout time
    val currentTime = ctx.timerService.currentProcessingTime
    val timeoutTime = currentTime + timeOut
    // register timer for timeout time
    ctx.timerService.registerProcessingTimeTimer(timeoutTime)
    // remember timeout time
    lastTimer.update(timeoutTime)
    // throughput the event
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[Row, Row]#OnTimerContext, out: Collector[Row]): Unit = {
    // check if this was the last timer we registered
    if (timestamp == lastTimer.value) {
      // it was, so no data was received afterwards.
      // fire an alert.
      out.collect(new Row(0))
    }
  }
}
