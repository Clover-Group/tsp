package ru.itclover.tsp.serializers.core

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime

import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.{EventSchema, NewRowSchema, RowSchema}
import ru.itclover.tsp.serializers.utils.SerializationUtils
import ru.itclover.tsp.utils.ParquetOps

import scala.collection.mutable
import scala.util.Random

/**
* Sserialization for Apache Parquet format
  */
class ParquetSerialization extends Serialization[Array[Byte], Row]{

  override def serialize(output: Row, eventSchema: EventSchema): Array[Byte] = {

    val currentTime = LocalDateTime.now().toString
    val randomInd = Random.nextInt(Integer.MAX_VALUE)

    val tempDir = Files.createTempDirectory("test")
    val tempPath = Paths.get(tempDir.normalize().toString, s"temp_${randomInd}_($currentTime)", ".temp")
    val tempFile = tempPath.toFile


    eventSchema match {
      case rowSchema: RowSchema =>

        val stringSchema =
          s"""message row_schema {
             | required int32 sourceIdField;
             | required double fromTsField;
             | required double toTsField;
             | required int32 ${rowSchema.appIdFieldVal._1.name};
             | required binary patternIdField;
             | required double processingTsField;
             | required binary contextField;
             |}""".stripMargin

        val data = mutable.ListBuffer(
          mutable.Map(
            rowSchema.sourceIdField.name -> Tuple2(
              output.getField(rowSchema.sourceIdInd).asInstanceOf[Int],
              "int"
            ),
            rowSchema.fromTsField.name -> Tuple2(
              output.getField(rowSchema.beginInd).asInstanceOf[Double],
              "double"
            ),
            rowSchema.toTsField.name -> Tuple2(
              output.getField(rowSchema.endInd).asInstanceOf[Double],
              "double"
            ),
            rowSchema.appIdFieldVal._1.name -> Tuple2(
              output.getField(rowSchema.appIdInd).asInstanceOf[Int],
              "int"
            ),
            rowSchema.patternIdField.name -> Tuple2(
              output.getField(rowSchema.patternIdInd).asInstanceOf[String],
              "java.lang.String"
            ),
            rowSchema.processingTsField.name -> Tuple2(
              output.getField(rowSchema.processingTimeInd).asInstanceOf[Double],
              "double"
            ),
            rowSchema.contextField.name -> Tuple2(
              output.getField(rowSchema.contextInd).asInstanceOf[String],
              "java.lang.String"
            )
          )
        )
        ParquetOps.writeData((tempFile, stringSchema, data))
      case newRowSchema: NewRowSchema =>
        val stringSchema =
          s"""message row_schema {
             | required int32 unitIdField;
             | required double fromTsField;
             | required double toTsField;
             | required int32 ${newRowSchema.appIdFieldVal._1.name};
             | required binary patternIdField;
             | required int32 subunitIdField;
             |}""".stripMargin

        val data = mutable.ListBuffer(
          mutable.Map(
            newRowSchema.unitIdField.name -> Tuple2(
              output.getField(newRowSchema.unitIdInd).asInstanceOf[Int],
              "int"
            ),
            newRowSchema.fromTsField.name -> Tuple2(
              output.getField(newRowSchema.beginInd).asInstanceOf[Double],
              "double"
            ),
            newRowSchema.toTsField.name -> Tuple2(
              output.getField(newRowSchema.endInd).asInstanceOf[Double],
              "double"
            ),
            newRowSchema.appIdFieldVal._1.name -> Tuple2(
              output.getField(newRowSchema.appIdInd).asInstanceOf[Int],
              "int"
            ),
            newRowSchema.patternIdField.name -> Tuple2(
              output.getField(newRowSchema.patternIdInd).asInstanceOf[String],
              "java.lang.String"
            ),
            newRowSchema.subunitIdField.name -> Tuple2(
              output.getField(newRowSchema.subunitIdInd).asInstanceOf[Int],
              "java.lang.String"
            )
          )
        )
        ParquetOps.writeData((tempFile, stringSchema, data))
    }


    val result = Files.readAllBytes(tempPath)
    tempFile.delete()

    result


  }

  override def deserialize(input: Array[Byte], fieldsIdxMap: Map[Symbol, Int]): Row = {

    val schemaAndReader = ParquetOps.retrieveSchemaAndReader(input)
    val rowData = ParquetOps.retrieveData(schemaAndReader)

    SerializationUtils.combineRows(rowData)

  }
}
