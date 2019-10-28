package ru.itclover.tsp.utils

import java.io.{File, FileInputStream}

import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowReader, SeekableReadChannel}
import org.apache.arrow.vector.{BaseValueVector, BigIntVector, BitVector, FieldVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, VarCharVector, VectorDefinitionSetter}
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._

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
    * @param valueVector vector with raw value
    * @return typed value
    */
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

    val allocator = new RootAllocator(allocatorValue)
    val fileStream = new FileInputStream(input)

    val readChannel = new SeekableReadChannel(fileStream.getChannel)

    val reader = new ArrowFileReader(readChannel, allocator)

    (reader.getVectorSchemaRoot.getSchema, reader, allocator)

  }

  def getSchemaFields(schema: Schema): List[String] = {

    schema.getFields
          .asScala
          .map(_.getName)
          .toList

  }

}
