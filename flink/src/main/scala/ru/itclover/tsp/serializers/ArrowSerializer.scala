package ru.itclover.tsp.serializers

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.RowSchema
import ru.itclover.tsp.services.FileService
import ru.itclover.tsp.utils.ArrowOps

import scala.collection.mutable
import scala.collection.JavaConverters._

import java.nio.file.Files

/**
* Class for serializing Flink row to Apache Arrow format
  * @param rowSchema schema of row
  */
class ArrowSerializer(rowSchema: RowSchema) extends SerializationSchema[Row] {

  override def serialize(element: Row): Array[Byte] = {

    val tempPath = FileService.createTemporaryFile()
    val tempFile = tempPath.toFile

    val schemaFields = List(
      new Field(
        rowSchema.sourceIdField.name,
        false,
        new ArrowType.Int(32, true),
        null
      ),
      new Field(
        rowSchema.fromTsField.name,
        false,
        new ArrowType.Decimal(3, 3),
        null
      ),
      new Field(
        rowSchema.toTsField.name,
        false,
        new ArrowType.Decimal(3, 3),
        null
      ),
      new Field(
        rowSchema.appIdFieldVal._1.name,
        false,
        new ArrowType.Int(32, true),
        null
      ),
      new Field(
        rowSchema.patternIdField.name,
        false,
        new ArrowType.Binary(),
        null
      ),
      new Field(
        rowSchema.processingTsField.name,
        false,
        new ArrowType.Decimal(3, 3),
        null
      ),
      new Field(
        rowSchema.contextField.name,
        false,
        new ArrowType.Binary(),
        null
      )
    )

    val schema = new Schema(schemaFields.asJava)
    val allocator = new RootAllocator(1000000)

    val data = mutable.ListBuffer(
      mutable.Map(
        rowSchema.sourceIdField.name -> element.getField(rowSchema.sourceIdInd).asInstanceOf[Int],
        rowSchema.fromTsField.name -> element.getField(rowSchema.beginInd).asInstanceOf[Double],
        rowSchema.toTsField.name -> element.getField(rowSchema.endInd).asInstanceOf[Double],
        rowSchema.appIdFieldVal._1.name -> element.getField(rowSchema.appIdInd).asInstanceOf[Int],
        rowSchema.patternIdField.name -> element.getField(rowSchema.patternIdInd).asInstanceOf[String],
        rowSchema.processingTsField.name -> element.getField(rowSchema.processingTimeInd).asInstanceOf[Double],
        rowSchema.contextField.name -> element.getField(rowSchema.contextInd).asInstanceOf[String]
      )
    )

    ArrowOps.writeData((tempFile, schema, data, allocator))

    val result = Files.readAllBytes(tempPath)
    tempFile.delete()

    result

  }
}
