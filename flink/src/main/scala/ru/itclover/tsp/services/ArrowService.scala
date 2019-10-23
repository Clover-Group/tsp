package ru.itclover.tsp.services

import java.io.{ByteArrayInputStream, File, FileInputStream}

import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowReader, ArrowStreamReader, SeekableReadChannel}
import org.apache.arrow.vector.{BaseValueVector, BitVector, Float4Vector, Float8Vector, IntVector, VarCharVector}
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Schema

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object ArrowService {

  val ALLOCATE_LIMIT = 1000000

  /**
  * Method for working with types from Apache Arrow schema.
  */
  def typesMap: Map[Types.MinorType, Class[_ >: Float with String with Boolean with Int with Double]] = Map(
    Types.MinorType.BIGINT -> classOf[Int],
    Types.MinorType.SMALLINT -> classOf[Int],
    Types.MinorType.BIT -> classOf[Boolean],
    Types.MinorType.INT -> classOf[Int],
    Types.MinorType.VARCHAR -> classOf[String],
    Types.MinorType.FLOAT4 -> classOf[Float],
    Types.MinorType.FLOAT8 -> classOf[Double]
  )

  /**
  * Method for retrieving schema and reader from input bytes
    * @param inputData byte array with input data
    * @return tuple with schema and reader
    */
  def retrieveSchemaAndReader(inputData: Array[Byte]): (Schema, ArrowReader, BaseAllocator) = {

    val allocator = new RootAllocator(ALLOCATE_LIMIT)
    val bytesStream = new ByteArrayInputStream(inputData)

    val reader = new ArrowStreamReader(bytesStream, allocator)

    (reader.getVectorSchemaRoot.getSchema, reader, allocator)

  }

  /**
  * Method for retrieving schema and reader from input file
    * @param inputData file with input data
    * @return tuple with schema and reader
    */
  def retrieveSchemaAndReader(inputData: File): (Schema, ArrowReader, BaseAllocator) = {

    val allocator = new RootAllocator(ALLOCATE_LIMIT)
    val fileStream = new FileInputStream(inputData)
    val readChannel = new SeekableReadChannel(fileStream.getChannel)

    val reader = new ArrowFileReader(readChannel, allocator)

    (reader.getVectorSchemaRoot.getSchema, reader, allocator)

  }

  /**
  * Method for converting input to list of values
    * @param input arrow schema, arrow reader, arrow allocator
    * @return list of values
    */
  def convertData(input: (Schema, ArrowReader, BaseAllocator)): ListBuffer[ListBuffer[(String, Any)]] = {

    val schema = input._1
    val reader = input._2
    val allocator = input._3

    val schemaFields: List[String] = schema.getFields
                                           .asScala
                                           .map(_.getName)
                                           .toList

    val schemaRoot = reader.getVectorSchemaRoot
    var rowCount = 0

    var readCondition = reader.loadNextBatch()

    var result: mutable.ListBuffer[mutable.ListBuffer[(String, Any)]] = mutable.ListBuffer.empty

    while(readCondition){

      rowCount = schemaRoot.getRowCount

      for(i <- 0 until rowCount){

        var rowResult: mutable.ListBuffer[(String, Any)] = mutable.ListBuffer.empty

        for(field <- schemaFields){

          val valueVector = schemaRoot.getVector(field)

          if(!typesMap.contains(valueVector.getMinorType)){
            throw new IllegalArgumentException(s"There is no mapping for Arrow Type ${valueVector.getMinorType}")
          }

          val valueInfo = typesMap(valueVector.getMinorType)

          val transferredVector: BaseValueVector = valueInfo.getName match {
            case "int" => valueVector.asInstanceOf[IntVector]
            case "boolean" => valueVector.asInstanceOf[BitVector]
            case "java.lang.String" => valueVector.asInstanceOf[VarCharVector]
            case "float" => valueVector.asInstanceOf[Float4Vector]
            case "double" => valueVector.asInstanceOf[Float8Vector]
            case _ => throw new IllegalArgumentException(s"No mapper for type ${valueInfo.getName}")
          }

          val value = transferredVector.getObject(i)

          rowResult += Tuple2(field, value)

        }

        result += rowResult

      }

      readCondition = reader.loadNextBatch()

    }

    reader.close()
    allocator.close()

    result

  }

}
