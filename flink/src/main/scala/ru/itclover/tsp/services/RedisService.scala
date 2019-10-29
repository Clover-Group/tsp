package ru.itclover.tsp.services

import scredis.Redis
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.types.Row

import scala.util.Try
import ru.itclover.tsp.io.input.{DeserializationInfo, RedisInputConf}

/**
* Deserialization trait for Redis
  * @tparam INPUT input type
  * @tparam OUTPUT output type
  */
trait Deserializer[INPUT, OUTPUT]{

  def deserialize(input: INPUT): OUTPUT

}

/**
* JSON Deserializer for Redis
  * @param fieldsIdxMap mapping of fields to count
  */
class JSONDeserializer(fieldsIdxMap: Map[Symbol, Int]) extends Deserializer[Array[Byte], Row]{
  override def deserialize(input: Array[Byte]): Row = {

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
  * Mapping of serializer types to implementation instances
    * @param info DeserializationInfo instance
    * @param fieldsIdxMap mapping of fields to count
    * @return implementation instance
    */
  def getDeserializer(info: DeserializationInfo, fieldsIdxMap: Map[Symbol, Int]) = info.serializerType match {

    case "json" => new JSONDeserializer(fieldsIdxMap)
    case _ => null

  }

  /**
  * Instantiating of redis client
    * @param conf redis config
    * @param info serialization info
    * @param fieldsIdxMap mapping of fields to count
    * @return client, serializer, mapping
    */
  def clientInstance(conf: RedisInputConf, info: DeserializationInfo,  fieldsIdxMap: Map[Symbol, Int]) = {

     val client = new Redis(
       host = conf.host,
       port = conf.port,
       database = conf.database.getOrElse(None),
       passwordOpt = conf.password
     )

    (client, getDeserializer(info, fieldsIdxMap), fieldsIdxMap)

  }

}


