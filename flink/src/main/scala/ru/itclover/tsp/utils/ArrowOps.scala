package ru.itclover.tsp.utils

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.{
  ArrowFileReader,
  ArrowFileWriter,
  ArrowReader,
  ArrowStreamReader,
  SeekableReadChannel
}
import org.apache.arrow.vector.{
  BaseValueVector,
  BigIntVector,
  BitVector,
  FieldVector,
  Float4Vector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  VarCharVector,
  VectorDefinitionSetter,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.util.Text
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

// Types are not known at compile time, so we use asInstanceOf and Any
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
object ArrowOps {

  /**
    * Types mapping from Apache Arrow to Scala types
    * @return
    */
  def typesMap = Map(
    Types.MinorType.BIGINT   -> classOf[Long],
    Types.MinorType.SMALLINT -> classOf[Short],
    Types.MinorType.BIT      -> classOf[Boolean],
    Types.MinorType.INT      -> classOf[Int],
    Types.MinorType.VARCHAR  -> classOf[String],
    Types.MinorType.FLOAT4   -> classOf[Float],
    Types.MinorType.FLOAT8   -> classOf[Double]
  )

  /**
    * Method for retrieving typed value from vector
    * Uses asInstanceOf for converting, so disabling the wart warning
    * @param valueVector vector with raw value
    * @return typed value
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def retrieveFieldValue(valueVector: FieldVector): BaseValueVector with FieldVector with VectorDefinitionSetter = {

    val valueInfo = typesMap(valueVector.getMinorType)

    valueInfo.getName match {
      case "int"              => valueVector.asInstanceOf[IntVector]
      case "boolean"          => valueVector.asInstanceOf[BitVector]
      case "java.lang.String" => valueVector.asInstanceOf[VarCharVector]
      case "float"            => valueVector.asInstanceOf[Float4Vector]
      case "double"           => valueVector.asInstanceOf[Float8Vector]
      case "long"             => valueVector.asInstanceOf[BigIntVector]
      case "short"            => valueVector.asInstanceOf[SmallIntVector]
      case _                  => throw new IllegalArgumentException(s"No mapper for type ${valueInfo.getName}")
    }

  }

  /**
    * Method for retrieving schema and reader from input file
    * @param input file with input data
    * @param allocatorValue value for root allocator
    * @return tuple with schema and reader
    */
  def retrieveSchemaAndReader(input: File, allocatorValue: Int): (Schema, ArrowReader, RootAllocator) = {

    val allocator = new RootAllocator(allocatorValue.toLong)
    val fileStream = new FileInputStream(input)

    val readChannel = new SeekableReadChannel(fileStream.getChannel)

    val reader = new ArrowFileReader(readChannel, allocator)

    (reader.getVectorSchemaRoot.getSchema, reader, allocator)

  }

  /**
    * Method for retrieving schema and reader from bytes
    * @param input bytes with input data
    * @param allocatorValue value for root allocator
    * @return tuple with schema and reader
    */
  def retrieveSchemaAndReader(input: Array[Byte], allocatorValue: Int): (Schema, ArrowReader, RootAllocator) = {

    val allocator = new RootAllocator(allocatorValue.toLong)
    val inputStream = new ByteArrayInputStream(input)

    val reader = new ArrowStreamReader(inputStream, allocator)
    val schema = reader.getVectorSchemaRoot.getSchema

    (schema, reader, allocator)

  }

  /**
    * Method for schema fields
    * @param schema schema from Apache Arrow
    * @return list of fields(string)
    */
  def getSchemaFields(schema: Schema): List[String] = {

    schema.getFields.asScala
      .map(_.getName)
      .toList

  }

  /**
    * Retrieve data in Apache Flink rows
    * @param input arrow schema and reader
    * @return flink rows
    */
  def retrieveData(input: (Schema, ArrowReader, RootAllocator)): mutable.ListBuffer[Row] = {

    val (schema, reader, allocator) = input
    val schemaFields = getSchemaFields(schema)

    val schemaRoot = reader.getVectorSchemaRoot

    val result: mutable.ListBuffer[Row] = mutable.ListBuffer.empty[Row]
    val objectsList: mutable.ListBuffer[Any] = mutable.ListBuffer.empty[Any]

    while (reader.loadNextBatch()) {
      breakable {
        val rowCount = schemaRoot.getRowCount

        for (i <- 0 until rowCount) {

          for (field <- schemaFields) {

            val valueVector = schemaRoot.getVector(field)
            objectsList += retrieveFieldValue(valueVector).getObject(i)

          }

          val row = new Row(objectsList.size)
          for (i <- objectsList.indices) {
            row.setField(i, objectsList(i))
          }

          val _ = result += row
          objectsList.clear()

        }

        if (!reader.loadNextBatch()) break
      }
    }

    reader.close()
    allocator.close()

    result

  }

  /**
    * Method for writing data to Apache Arrow file
    * @param input file for data, schema for data, data, allocator
    */
  def writeData(input: (File, Schema, mutable.ListBuffer[mutable.Map[String, Any]], RootAllocator)): Unit = {

    val (inputFile, schema, data, allocator) = input

    val outStream = new FileOutputStream(inputFile)
    val rootSchema = VectorSchemaRoot.create(schema, allocator)
    val provider = new DictionaryProvider.MapDictionaryProvider()

    val dataWriter = new ArrowFileWriter(
      rootSchema,
      provider,
      outStream.getChannel
    )

    var counter = 0

    dataWriter.start()

    for (item <- data) {

      val fields = rootSchema.getSchema.getFields.asScala

      for (field <- fields) {

        if (item.contains(field.getName)) {

          val data = item(field.getName)
          val vector = rootSchema.getVector(field.getName)

          val valueInfo = typesMap(vector.getMinorType)

          valueInfo.getName match {

            case "int" =>
              val tempVector = vector.asInstanceOf[IntVector]
              tempVector.setSafe(counter, data.asInstanceOf[Int])

            case "boolean" =>
              var value: Int = 0
              if (!data.asInstanceOf[Boolean]) {
                value = 1
              }

              val tempVector = vector.asInstanceOf[BitVector]
              tempVector.setSafe(counter, value)

            case "java.lang.String" =>
              val tempVector = vector.asInstanceOf[VarCharVector]
              tempVector.setSafe(
                counter,
                new Text(data.asInstanceOf[String])
              )

            case "float" =>
              val tempVector = vector.asInstanceOf[Float4Vector]
              tempVector.setSafe(counter, data.asInstanceOf[Float])

            case "double" =>
              val tempVector = vector.asInstanceOf[Float8Vector]
              tempVector.setSafe(counter, data.asInstanceOf[Double])

            case "long" =>
              val tempVector = vector.asInstanceOf[BigIntVector]
              tempVector.setSafe(counter, data.asInstanceOf[Long])

            case "short" => {
              val tempVector = vector.asInstanceOf[SmallIntVector]
              tempVector.setSafe(counter, data.asInstanceOf[Short])
            }

            case _ => throw new IllegalArgumentException(s"No mapper for type ${valueInfo.getName}")
          }

          counter += 1

          dataWriter.writeBatch()

        }

      }

    }

    dataWriter.end()
    dataWriter.close()

    outStream.flush()
    outStream.close()

  }

}
