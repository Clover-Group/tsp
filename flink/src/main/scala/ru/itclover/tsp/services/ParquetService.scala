package ru.itclover.tsp.services

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.{MessageType, OriginalType, PrimitiveType, Type}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParquetService {

  def typesMap = Map(
    PrimitiveType.PrimitiveTypeName.INT64   -> classOf[Long],
    PrimitiveType.PrimitiveTypeName.INT32   -> classOf[Int],
    PrimitiveType.PrimitiveTypeName.INT96   -> classOf[Array[Byte]],
    PrimitiveType.PrimitiveTypeName.BOOLEAN -> classOf[Boolean],
    PrimitiveType.PrimitiveTypeName.FLOAT   -> classOf[Float],
    PrimitiveType.PrimitiveTypeName.DOUBLE  -> classOf[Double],
    OriginalType.UTF8                       -> classOf[String]
  )

  /**
    * Method for retrieving schema and reader from input file
    *
    * @param inputData file with input data
    * @return tuple with schema and reader
    */
  def retrieveSchemaAndReader(inputData: File): (MessageType, ParquetFileReader) = {

    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(inputData.toURI), new Configuration()))
    val schema = reader.getFooter.getFileMetaData.getSchema

    (schema, reader)

  }

  /**
    * Method for converting input to list of values
    *
    * @param input parquet schema, parquet reader
    * @return list of values
    */
  def convertData(input: (MessageType, ParquetFileReader)): ListBuffer[ListBuffer[(String, Any)]] = {

    val schema = input._1
    val reader = input._2

    val schemaFields: List[Type] = schema.getFields.asScala.toList

    val fieldMap: mutable.Map[String, (PrimitiveType, OriginalType)] = mutable.Map.empty

    for (field <- schemaFields) {
      fieldMap += (field.getName -> Tuple2(field.asPrimitiveType, field.getOriginalType))
    }

    var pages = reader.readNextRowGroup()
    val groups: mutable.ListBuffer[SimpleGroup] = mutable.ListBuffer.empty

    var counter = 0L

    while (pages != null) {

      val rows = pages.getRowCount
      counter += rows

      val columnIO = new ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))

      (0 until rows.toInt).foreach(_ => groups += recordReader.read().asInstanceOf[SimpleGroup])

      pages = reader.readNextRowGroup()

    }

    reader.close()

    var result: mutable.ListBuffer[mutable.ListBuffer[(String, Any)]] = mutable.ListBuffer.empty

    for (group <- groups) {

      val rowResult: mutable.ListBuffer[(String, Any)] = mutable.ListBuffer.empty

      val fieldCount = group.getType.getFieldCount

      for (i <- 0 until fieldCount) {

        val valueCount = group.getFieldRepetitionCount(i)

        val fieldType = group.getType.getType(i)
        val fieldName = fieldType.getName
        val fieldMapping = fieldMap(fieldName)

        var value: Any = null

        for (j <- 0 until valueCount) {

          if (typesMap.contains(fieldMapping._2)) {
            value = group.getString(i, j)
          } else {

            val valueInfo = typesMap(fieldMapping._1.getPrimitiveTypeName)

            value = valueInfo.getName match {

              case "long" => group.getLong(i, j)
              case "int"  => group.getInteger(i, j)

              //array of bytes
              case "[B" => group.getBinary(i, j).getBytes

              case "boolean" => group.getBoolean(i, j)
              case "float"   => group.getFloat(i, j)
              case "double"  => group.getDouble(i, j)

              case _ => throw new IllegalArgumentException(s"No mapper for type ${valueInfo.getName}")

            }

          }

          rowResult += (fieldName -> value)

        }

        result += rowResult

      }

    }

    result

  }
}
