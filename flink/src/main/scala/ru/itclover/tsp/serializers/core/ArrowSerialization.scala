package ru.itclover.tsp.serializers.core

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.{DateUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.flink.types.Row
import ru.itclover.tsp.io.output.{EventSchema, NewRowSchema}
import ru.itclover.tsp.serializers.utils.SerializationUtils
import ru.itclover.tsp.services.FileService
import ru.itclover.tsp.utils.ArrowOps

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Serialization for Apache Arrow format
  */
// Arrow serialization heavily deals with Any's and null's. Hence, no option other than suppress these warts here.
@SuppressWarnings(Array(
  "org.wartremover.warts.Null",
  "org.wartremover.warts.Any"
))
class ArrowSerialization extends Serialization[Array[Byte], Row] {

  override def serialize(output: Row, eventSchema: EventSchema): Array[Byte] = {

    val tempPath = FileService.createTemporaryFile()
    val tempFile = tempPath.toFile

    val schemaFields = eventSchema match {
      case newRowSchema: NewRowSchema =>
        List(
          new Field(
            newRowSchema.unitIdField.name,
            false,
            new ArrowType.Int(32, true),
            null
          ),
          new Field(
            newRowSchema.fromTsField.name,
            false,
            new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"),
            null
          ),
          new Field(
            newRowSchema.toTsField.name,
            false,
            new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"),
            null
          ),
          new Field(
            newRowSchema.appIdFieldVal._1.name,
            false,
            new ArrowType.Int(32, true),
            null
          ),
          new Field(
            newRowSchema.patternIdField.name,
            false,
            new ArrowType.Utf8,
            null
          ),
          new Field(
            newRowSchema.subunitIdField.name,
            false,
            new ArrowType.Utf8,
            null
          )
        )
    }

    val schema = new Schema(schemaFields.asJava)
    val allocator = new RootAllocator(1000000)

    val data = eventSchema match {
      case newRowSchema: NewRowSchema =>
        mutable.ListBuffer(
          mutable.Map(
            newRowSchema.unitIdField.name -> output.getField(newRowSchema.unitIdInd).asInstanceOf[Int],
            newRowSchema.fromTsField.name -> output.getField(newRowSchema.beginInd).asInstanceOf[Timestamp],
            newRowSchema.toTsField.name -> output.getField(newRowSchema.endInd).asInstanceOf[Timestamp],
            newRowSchema.appIdFieldVal._1.name -> output.getField(newRowSchema.appIdInd).asInstanceOf[Int],
            newRowSchema.patternIdField.name -> output.getField(newRowSchema.patternIdInd).asInstanceOf[String],
            newRowSchema.subunitIdField.name -> output.getField(newRowSchema.subunitIdInd).asInstanceOf[String]
          )
        )
    }

    ArrowOps.writeData((tempFile, schema, data, allocator))

    val result = Files.readAllBytes(tempPath)
    tempFile.delete()

    result

  }

  override def deserialize(input: Array[Byte], fieldsIdxMap: Map[Symbol, Int]): Row = {

    val tempFile: File = FileService.convertBytes(input)
    val schemaAndReader = ArrowOps.retrieveSchemaAndReader(tempFile, Integer.MAX_VALUE)
    val rowData = ArrowOps.retrieveData(schemaAndReader)
    tempFile.delete()

    SerializationUtils.combineRows(rowData)

  }
}
