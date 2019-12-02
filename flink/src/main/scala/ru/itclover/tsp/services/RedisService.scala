package ru.itclover.tsp.services

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config

import scala.util.Try
import ru.itclover.tsp.io.input.RedisInputConf
import ru.itclover.tsp.io.output.{RedisOutputConf, RowSchema}
import ru.itclover.tsp.serializers.core.{ArrowSerialization, JSONSerialization, ParquetSerialization}

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
    * @param serializer information about serialization type
    * @return implementation instance
    */
  def getSerialization(serializer: String) = serializer match {

    case "json" => new JSONSerialization()
    case "parquet" => new ParquetSerialization()
    case "arrow" => new ArrowSerialization()
    case _      => throw new IllegalArgumentException(s"No serialization for type $serializer")

  }

  /**
    * Helper method to prepare Redis client
    * @param redisURL redis connection url
    * @return redis client
    */
  def extractClient(redisURL: String): RedissonClient = {

    val redisConfig = new Config
    redisConfig.useSingleServer().setAddress(redisURL)

    Redisson.create(redisConfig)

  }

  /**
    * Instantiating of redis client
    * @param conf redis input config
    * @param serializer information about serialization type
    * @return client, serializer
    */
  def clientInstance(conf: RedisInputConf, serializer: String) = {

    val client = extractClient(conf.url)

    (client, getSerialization(serializer))

  }

  /**
    * Instantiating of redis client
    * @param conf redis output config
    * @param serializer information about serialization type
    * @return client, serializer
    */
  def clientInstance(conf: RedisOutputConf, serializer: String) = {

    val client = extractClient(conf.url)

    (client, getSerialization(serializer))

  }

}
