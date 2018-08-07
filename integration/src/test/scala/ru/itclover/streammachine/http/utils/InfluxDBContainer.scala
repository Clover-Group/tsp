package ru.itclover.streammachine.http.utils

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.SingleContainer
import org.junit.runner.Description
import org.testcontainers.containers.wait.WaitStrategy
import org.testcontainers.containers.{BindMode, GenericContainer => OTCGenericContainer}
import ru.itclover.streammachine.services.InfluxDBService
import org.influxdb.{BatchOptions, InfluxDB, InfluxDBFactory}
import org.influxdb.dto.{Query, QueryResult}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}


class InfluxDBContainer(imageName: String,
                        val portsBindings: List[(Int, Int)] = List.empty,
                        val url: String,
                        val dbName: String,
                        val userName: String,
                        val password: String = "",
                        env: Map[String, String] = Map(),
                        command: Seq[String] = Seq(),
                        classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
                        waitStrategy: Option[WaitStrategy] = None
                      ) extends SingleContainer[OTCGenericContainer[_]] {

  type OTCContainer = OTCGenericContainer[T] forSome {type T <: OTCGenericContainer[T]}
  override implicit val container: OTCContainer = new OTCGenericContainer(imageName)

  if (portsBindings.nonEmpty) {
    val bindings = portsBindings.map { case (out, in) => s"${out.toString}:${in.toString}" }
    container.setPortBindings(bindings.asJava)
  }
  env.foreach(Function.tupled(container.withEnv))
  if (command.nonEmpty) {
    container.withCommand(command: _*)
  }
  classpathResourceMapping.foreach(Function.tupled(container.withClasspathResourceMapping))
  waitStrategy.foreach(container.waitingFor)

  var db: InfluxDB = _

  override def starting()(implicit description: Description): Unit = {
    super.starting()
    db = InfluxDBService.connectDb(url, dbName, Some(userName), Some(password)) match {
      case Success(database) => database
      case Failure(exception) => throw exception
    }
  }

  override def finished()(implicit description: Description): Unit = {
    super.finished()
    db.close()
  }

  def executeQuery(sql: String): QueryResult = db.query(new Query(sql, dbName))

  def executeUpdate(sql: String): Unit = db.write(sql)
}
