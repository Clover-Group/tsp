package ru.itclover.tsp.io.input

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.{ArrowFileReader, SeekableReadChannel}
import org.apache.arrow.vector.types.Types
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumerBase}
import org.apache.flink.api.common.serialization.{DeserializationSchema, TypeInformationSerializationSchema}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class InputData(id: Int)

case class KafkaConf(
  brokers: String,
  topic: String,
  group: String = UUID.randomUUID().toString,
  offsetReset: String = "largest"
)

object KafkaDeSerializer extends DeserializationSchema[InputData] {

  override def isEndOfStream(t: InputData): Boolean = false

  override def deserialize(bytes: Array[Byte]): InputData = {

    val tempFile = File.createTempFile("test", "tmp")
    val outputStream = new FileOutputStream(tempFile)
    outputStream.write(bytes)

    val fileInputStream = new FileInputStream(tempFile)
    val seekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel)

    val arrowFileReader = new ArrowFileReader(seekableReadChannel, new RootAllocator(Integer.MAX_VALUE))
    val schemaRow = arrowFileReader.getVectorSchemaRoot

    val javaBlocks = arrowFileReader.getRecordBlocks
    val scalaBlocks = javaBlocks.asScala.toList

    var test = new ListBuffer[Int]()

    scalaBlocks
      .foreach(block => {

        arrowFileReader.loadRecordBatch(block)
        val rowCount = schemaRow.getRowCount
        println(s"row count for current block: $rowCount")

        val vectors = schemaRow.getFieldVectors.asScala.toList

        vectors
          .foreach(vector => {

            val fieldType = vector.getMinorType

            if (fieldType == Types.MinorType.INT) {

              val intVector = vector.asInstanceOf[IntVector]

              (0 to intVector.getValueCount)
                .foreach(
                  i =>
                    if (!intVector.isNull(i)) {
                      test += intVector.get(i)
                    }
                )

            }

          })

      })

    println(s"TEST data: $test")

    InputData(1)

  }

  override def getProducedType: TypeInformation[InputData] = TypeExtractor.getForClass(classOf[InputData])
}

object KafkaInputConf {

  def getSource[Event: TypeInformationSerializationSchema](kafkaConfig: KafkaConf): FlinkKafkaConsumerBase[InputData] = {

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", kafkaConfig.brokers)
    kafkaProps.setProperty("group.id", kafkaConfig.group)
    kafkaProps.setProperty("auto.commit.enable", "false")
    kafkaProps.setProperty("auto.offset.reset", kafkaConfig.offsetReset)

    new FlinkKafkaConsumer010[InputData](kafkaConfig.topic, KafkaDeSerializer, kafkaProps)
  }

}
