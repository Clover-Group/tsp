package ru.itclover.tsp.utils

import java.io.{File, FileInputStream}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.ipc.{ArrowFileReader, SeekableReadChannel}
import org.apache.arrow.vector.types.Types

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object ArrowOps {

  def readFromFile(inputFile: File): ListBuffer[Int] = {

    val fileInputStream = new FileInputStream(inputFile)
    val seekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel)

    val arrowFileReader = new ArrowFileReader(seekableReadChannel, new RootAllocator(Integer.MAX_VALUE))
    val schemaRow = arrowFileReader.getVectorSchemaRoot

    println(s"SCHEMA: ${schemaRow.toString}")

    val javaBlocks = arrowFileReader.getRecordBlocks
    val scalaBlocks = javaBlocks.asScala.toList

    println(s"BLOCKS: ${scalaBlocks}")

    var test = new ListBuffer[Int]()

    val t1 = System.currentTimeMillis()

    scalaBlocks
      .foreach(block => {

        arrowFileReader.loadRecordBatch(block)
        val rowCount = schemaRow.getRowCount
        println(s"row count for current block: $rowCount")

        val vectors = schemaRow.getFieldVectors.asScala.toList

        vectors
          .foreach(vector => {

            val fieldType = vector.getMinorType

            //TODO: add fabric of types
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

    val t2 = System.currentTimeMillis()

    println(s"Время обработки: ${(t2 - t1) / 1000} секунд")

    test

  }

}
