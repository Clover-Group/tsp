package ru.itclover.streammachine.io.input

import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.util.{Success, Try}


trait InputConf extends Serializable {
  def sourceId: Int

  def datetimeFieldName: Symbol

  def partitionFieldNames: Seq[Symbol]

  def eventsMaxGapMs: Long

  def fieldsTypesInfo: InputConf.ThrowableOrTypesInfo
}

object InputConf {
  type ThrowableOrTypesInfo = Either[Throwable, IndexedSeq[(Symbol, TypeInformation[_])]]
}


// import com.paulgoldbaum.influxdbclient._

/*case class InfluxDBInputConf(sourceId: Int,
                             dbName: String, host: String, port: Int,
                             query: String,
                             datetimeFieldName: Symbol,
                             eventsMaxGapMs: Long,
                             partitionFieldNames: Seq[Symbol],
                             userName: Option[String] = None,
                             password: Option[String] = None) extends InputConf {
  override def fieldsTypesInfo = connectDb match {
    case Success((connection, db)) =>
      val r = db.query(s"SELECT * FROM ($query)")
      r.value.get.get.series.foreach { s =>
        s.records.head.allValues.head match {
          case s: String =>
        }
      }
  }

  private def connectDb = for {
    connection <- Try(InfluxDB.connect(host, port, userName.orNull, password.orNull))
    db <- Try(connection.selectDatabase(dbName))
  } yield (connection, db)
}*/


case class FileConf(sourceId: Int, filePath: String, eventsMaxGapMs: Long,
                    datetimeFieldName: Symbol, partitionFieldNames: Seq[Symbol]) extends InputConf {
  def fieldsTypesInfo: InputConf.ThrowableOrTypesInfo = ???
}
