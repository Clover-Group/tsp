package ru.itclover.tsp.services

import java.nio.charset.Charset

import scredis.Redis
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.types.Row

import scala.util.Try
import scala.collection.mutable
import ru.itclover.tsp.io.input.{RedisInputConf, SerializerInfo}
import ru.itclover.tsp.io.output.{RedisOutputConf, RowSchema}

/**
* Deserialization trait for Redis
  * @tparam INPUT input type
  * @tparam OUTPUT output type
  */
trait Serialization[INPUT, OUTPUT]{

  def serialize(output: OUTPUT, rowSchema: RowSchema): INPUT
  def deserialize(input: INPUT, fieldsIdxMap: Map[Symbol, Int]): OUTPUT

}

/**
* JSON Serialization for Redis
  */
class JSONSerialization extends Serialization[Array[Byte], Row]{

  /**
  * Method for deserialize from json string
    * @param input bytes array from json string
    * @return flink row
    */
  override def deserialize(input: Array[Byte], fieldsIdxMap: Map[Symbol, Int]): Row = {

    val inputData = new String(input)
    val jsonTree = new ObjectMapper().readTree(inputData)
    val row = new Row(fieldsIdxMap.size)
    val mapper = new ObjectMapper()

    fieldsIdxMap.foreach {
      case (elem, index) =>
        val rawValue = jsonTree.get(elem.name)
        val fieldValue = mapper.convertValue(rawValue, classOf[java.lang.Object])
        row.setField(index, fieldValue)
    }

    row

  }

  /**
  * Method for serialize to json string
    * @param output flink row
    * @param rowSchema schema from flink row
    * @return bytes array from json string
    */
  override def serialize(output: Row, rowSchema: RowSchema): Array[Byte] = {

    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()

    root.put(rowSchema.sourceIdField.name, output.getField(rowSchema.sourceIdInd).asInstanceOf[Int])
    root.put(rowSchema.fromTsField.name, output.getField(rowSchema.beginInd).asInstanceOf[Double])
    root.put(rowSchema.toTsField.name, output.getField(rowSchema.endInd).asInstanceOf[Double])
    root.put(rowSchema.appIdFieldVal._1.name, output.getField(rowSchema.appIdInd).asInstanceOf[Int])
    root.put(rowSchema.patternIdField.name, output.getField(rowSchema.patternIdInd).asInstanceOf[String])
    root.put(rowSchema.processingTsField.name, output.getField(rowSchema.processingTimeInd).asInstanceOf[Double])
    root.put(rowSchema.contextField.name, output.getField(rowSchema.contextInd).asInstanceOf[String])

    val jsonString = mapper.writeValueAsString(root)

    jsonString.getBytes(Charset.forName("UTF-8"))

  }
}

object RedisService {

  /**
  * Method for retrieving types from Redis input
    * @param conf Redis config
    * @return field types info
    */
  def fetchFieldsTypesInfo(conf: RedisInputConf): Try[Seq[(Symbol, Class[_])]] = Try(
    conf.fieldsTypes.map {

      case (fieldName, fieldType) =>
        val fieldClass = fieldType match {

          case "int8"    => classOf[Byte]
          case "int16"   => classOf[Short]
          case "int32"   => classOf[Int]
          case "int64"   => classOf[Long]
          case "int96"   => classOf[BigInt]
          case "float32" => classOf[Float]
          case "float64" => classOf[Double]
          case "boolean" => classOf[Boolean]
          case "string"  => classOf[String]
          case _         => classOf[Any]

        }
        (Symbol(fieldName), fieldClass)

    }.toSeq
  )

  /**
  * Mapping of serialization types to implementation instances
    * @param info DeserializationInfo instance
    * @return implementation instance
    */
  def getSerialization(info: SerializerInfo) = info.serializerType match {

    case "json" => new JSONSerialization()
    case _ => null

  }

  /**
  * Instantiating of redis client
    * @param conf redis input config
    * @param info serialization info
    * @return client, serializer
    */
  def clientInstance(conf: RedisInputConf, info: SerializerInfo) = {

     val client = new Redis(
       host = conf.host,
       port = conf.port,
       database = conf.database.getOrElse(0),
       passwordOpt = conf.password
     )

    (client, getSerialization(info))

  }

  /**
    * Instantiating of redis client
    * @param conf redis output config
    * @param info serialization info
    * @return client, serializer
    */
  def clientInstance(conf: RedisOutputConf, info: SerializerInfo) = {

    val client = new Redis(
      host = conf.host,
      port = conf.port,
      database = conf.database.getOrElse(0),
      passwordOpt = conf.password
    )

    (client, getSerialization(info))

  }

}


