package ru.itclover.tsp.deserializers

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.{ArrowFileReader, SeekableReadChannel}
import org.apache.arrow.vector.types.Types
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import ru.itclover.tsp.io.input.InputData

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
* Deserializer for Arrow file from Kafka
  */
object KafkaArrowDeserializer extends DeserializationSchema[InputData]{

  override def isEndOfStream(t: InputData): Boolean = true

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
