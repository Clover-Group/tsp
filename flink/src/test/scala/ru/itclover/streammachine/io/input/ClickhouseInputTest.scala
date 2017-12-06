package ru.itclover.streammachine.io.input

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.{FunSuite, Matchers}

class ClickhouseInputTest extends FunSuite with Matchers {
  val wid_dt_tin1__query = "select Wagon_id, datetime, Tin_1 from series765_data limit 10000, 100"
  val jdbcConf = JDBCConfig(
        jdbcUrl = "jdbc:clickhouse://82.202.237.34:8123/renamedTest",
        query = wid_dt_tin1__query,
        driverName = "ru.yandex.clickhouse.ClickHouseDriver"
      )

  test("ClickhouseInput.getTypeInformation correct") {
    val wid_dt_tin1__conf = jdbcConf.copy(query = wid_dt_tin1__query)
    val typeInfoEither = ClickhouseInput.queryFieldsTypeInformation(wid_dt_tin1__conf)
    assert(typeInfoEither.isRight)
    val typeInfo = typeInfoEither.right.get
    assert(typeInfo.size == 3)

    val refTypes = Map[String, TypeInformation[_]]("Wagon_id" -> TypeInformation.of(classOf[Int]),
      "datetime" -> TypeInformation.of(classOf[String]), "Tin_1" -> TypeInformation.of(classOf[Float]))
    refTypes.keys should contain allElementsOf typeInfo.map(_._1)
    refTypes.values should contain allElementsOf typeInfo.map(_._2)
  }

  test("ClickhouseInput.getTypeInformation invalid") {
    val invalidUrlConf = jdbcConf.copy(jdbcUrl = "jdbc:CH11://0.0.0.0:8123/renamedTest")
    val invalidUrlTypeInfoEither = ClickhouseInput.queryFieldsTypeInformation(invalidUrlConf)
    invalidUrlTypeInfoEither should be ('left)
  }
}
