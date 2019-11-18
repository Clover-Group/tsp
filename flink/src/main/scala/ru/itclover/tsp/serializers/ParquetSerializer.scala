package ru.itclover.tsp.serializers

import java.nio.file.Files

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.RowSchema
import ru.itclover.tsp.services.FileService
import ru.itclover.tsp.utils.ParquetOps

import scala.collection.mutable

/**
  * Class for serializing Flink row to Apache Parquet format
  * @param rowSchema schema of row
  */
class ParquetSerializer(rowSchema: RowSchema) extends SerializationSchema[Row]  {

  override def serialize(element: Row): Array[Byte] = {

    val tempPath = FileService.createTemporaryFile()
    val tempFile = tempPath.toFile

    val stringSchema =
      s"""message row_schema {
        | required int32 sourceIdField;
        | required double fromTsField;
        | required double toTsField;
        | required int32 ${rowSchema.appIdFieldVal._1.name};
        | required string patternIdField;
        | required double processingTsField;
        | required string contextField;
        |}""".stripMargin

    val data = mutable.ListBuffer(
      mutable.Map(
        rowSchema.sourceIdField.name -> (
          element.getField(rowSchema.sourceIdInd).asInstanceOf[Int], "int"
        ),
        rowSchema.fromTsField.name -> (
          element.getField(rowSchema.beginInd).asInstanceOf[Double], "double"
        ),
        rowSchema.toTsField.name -> (
          element.getField(rowSchema.endInd).asInstanceOf[Double], "double"
        ),
        rowSchema.appIdFieldVal._1.name -> (
          element.getField(rowSchema.appIdInd).asInstanceOf[Int], "int"
        ),
        rowSchema.patternIdField.name -> (
          element.getField(rowSchema.patternIdInd).asInstanceOf[String], "java.lang.String"
        ),
        rowSchema.processingTsField.name -> (
          element.getField(rowSchema.processingTimeInd).asInstanceOf[Double], "double"
        ),
        rowSchema.contextField.name -> (
          element.getField(rowSchema.contextInd).asInstanceOf[String], "java.lang.String"
        )
      )
    )

    ParquetOps.writeData((tempFile, stringSchema, data))

    val result = Files.readAllBytes(tempPath)
    tempFile.delete()

    result

  }
}
