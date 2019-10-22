package ru.itclover.tsp.utils

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, SeekableReadChannel}
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object ArrowOps {

  def readFromFile(inputFile: File): ListBuffer[Int] = {

    val fileInputStream = new FileInputStream(inputFile)
    val seekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel)

    val arrowFileReader = new ArrowFileReader(seekableReadChannel, new RootAllocator(Integer.MAX_VALUE))
    val schemaRow = arrowFileReader.getVectorSchemaRoot

    schemaRow.getSchema.getFields

    val javaBlocks = arrowFileReader.getRecordBlocks
    val scalaBlocks = javaBlocks.asScala.toList

    var test = new ListBuffer[Int]()

    val t1 = System.currentTimeMillis()

    scalaBlocks
      .foreach(block => {

        arrowFileReader.loadRecordBatch(block)
        val rowCount = schemaRow.getRowCount

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

    test

  }

  def writeDataToFile(path: String): Unit = {

    val allocator = new RootAllocator(Integer.MAX_VALUE)

    val schemaConfig = List(
      new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)
    )

    val schema = new Schema(schemaConfig.asJava)
    val root = VectorSchemaRoot.create(schema, allocator)
    val provider = new DictionaryProvider.MapDictionaryProvider()

    val file = new File(path)

    val outputStream = new FileOutputStream(file)
    val arrowFileWriter = new ArrowFileWriter(root, provider, outputStream.getChannel)

    arrowFileWriter.start()

    for(i <- 0 to 100){

      root.setRowCount(10)

      for(field <- root.getSchema.getFields.asScala){

        val vector = root.getVector(field.getName)

        if(vector.getMinorType == Types.MinorType.INT){

          val intVector = vector.asInstanceOf[IntVector]
          intVector.setInitialCapacity(10)
          intVector.allocateNew()

          for(j <- 0 to 10){
            intVector.setSafe(j, i)
          }

          vector.setValueCount(10)

        }

      }

      arrowFileWriter.writeBatch()

    }

    arrowFileWriter.end()
    arrowFileWriter.close()
    outputStream.flush()
    outputStream.close()

  }

}
