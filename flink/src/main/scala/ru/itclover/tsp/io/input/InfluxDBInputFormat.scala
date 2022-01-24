package ru.itclover.tsp.io.input

import java.io.IOException
import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import okhttp3.OkHttpClient
import org.apache.flink.api.common.io.{GenericInputFormat, NonParallelInput}
import org.apache.flink.core.io.GenericInputSplit
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.{Query, QueryResult}

object InfluxDBInputFormat {
  def create() = new Builder()

  class Builder {
    val obj = new InfluxDBInputFormat()

    def url(url: String) = {

      obj.url = url
      this
    }

    def username(username: String) = {

      obj.username = username
      this
    }

    def password(password: String) = {

      obj.password = password
      this
    }

    def database(database: String) = {

      obj.database = database
      this
    }

    def timeoutSec(sec: Long) = {

      obj.timeoutSec = sec
      this
    }

    def query(query: String) = {

      obj.query = query
      this
    }

    def and() =
      new Actions()

    class Actions {

      def buildIt() =
        obj

      def consumeIt(consumer: Consumer[InfluxDBInputFormat]) =
        consumer.accept(buildIt())
    }
  }
}

@SerialVersionUID(42L)
// TODO: replace Java-style builder with Scala-conformant code
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class InfluxDBInputFormat extends GenericInputFormat[QueryResult.Result] with NonParallelInput {
  var url: String = _
  var username: String = _
  var password: String = _
  var database: String = _
  var query: String = _
  var timeoutSec: Long = _

  var queryResult: util.ListIterator[QueryResult.Result] = _

  // --------------------------------------------------------------------------

  override def open(split: GenericInputSplit) = {
    val extraConf = new OkHttpClient.Builder()
      .readTimeout(timeoutSec, TimeUnit.SECONDS)
      .writeTimeout(timeoutSec, TimeUnit.SECONDS)
      .connectTimeout(timeoutSec, TimeUnit.SECONDS)
    val connection = InfluxDBFactory.connect(url, username, password, extraConf)
    val queryResult = connection.query(new Query(this.query, database))
    if (queryResult.hasError) {
      throw new IOException("Could not execute query: " + queryResult.getError)
    }
    this.queryResult = queryResult.getResults.listIterator()
  }

  override def reachedEnd() = !queryResult.hasNext

  def nextRecord(reuse: QueryResult.Result): QueryResult.Result = {
    val result = queryResult.next()
    reuse.setError(result.getError)
    reuse.setSeries(result.getSeries)
    reuse
  }
}
