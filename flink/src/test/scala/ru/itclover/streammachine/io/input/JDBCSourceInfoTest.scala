package ru.itclover.streammachine.io.input

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.{FunSuite, Matchers}
import ru.itclover.streammachine.io.input.source.JDBCSourceInfo

class JDBCSourceInfoTest extends FunSuite with Matchers {
  val wid_dt_tin1__query = "select Wagon_id, datetime, Tin_1 from series765_data limit 10000, 100"
  val jdbcConf = JDBCInputConfig(
        jdbcUrl = "jdbc:clickhouse://82.202.237.34:8123/renamedTest",
        query = wid_dt_tin1__query,
        driverName = "ru.yandex.clickhouse.ClickHouseDriver",
        datetimeColname = 'datetime,
        partitionColnames = Seq('Wagon_id)
      )

  test("JDBCSourceInfo query correct") {
    val wid_dt_tin1__conf = jdbcConf.copy(query = wid_dt_tin1__query)
    val srcInfo = JDBCSourceInfo(wid_dt_tin1__conf)
    assert(srcInfo.isRight)
    val typeInfo = srcInfo.right.get
    assert(typeInfo.fieldsInfo.length == 3)

    val refTypes = Map[String, TypeInformation[_]]("Wagon_id" -> TypeInformation.of(classOf[Int]),
      "datetime" -> TypeInformation.of(classOf[String]), "Tin_1" -> TypeInformation.of(classOf[Float]))
    refTypes.keys should contain allElementsOf typeInfo.fieldsInfo.map(_._1)
    refTypes.values should contain allElementsOf typeInfo.fieldsInfo.map(_._2)
  }

  test("JDBCSourceInfo query incorrect") {
    val invalidUrlConf = jdbcConf.copy(jdbcUrl = "jdbc:CH11://0.0.0.0:8123/renamedTest")
    val invalidQuerySrcInfo = JDBCSourceInfo(invalidUrlConf)
    invalidQuerySrcInfo should be ('left)
  }
}
