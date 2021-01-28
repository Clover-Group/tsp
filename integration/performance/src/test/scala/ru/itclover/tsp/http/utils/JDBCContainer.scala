package ru.itclover.tsp.http.utils

import java.sql.{Connection, DriverManager, ResultSet}

import com.dimafeng.testcontainers.SingleContainer
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.containers.{BindMode, GenericContainer => OTCGenericContainer}

import scala.collection.JavaConverters._
import scala.language.existentials

// The `connection` member (it cannot be initialised directly).
// Also, default arguments are useful when creating containers.
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.DefaultArguments"
  )
)
class JDBCContainer(imageName: String,
                    val portsBindings: List[(Int, Int)] = List.empty,
                    val driverName: String,
                    val jdbcUrl: String,
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
    val _ = container.withCommand(command: _*)
  }
  classpathResourceMapping.foreach(Function.tupled(container.withClasspathResourceMapping))
  waitStrategy.foreach(container.waitingFor)

  var connection: Connection = _

  override def start(): Unit = {
    super.start()
    connection = {
      val _ = Class.forName(driverName)
      DriverManager.getConnection(jdbcUrl)
    }
  }

  override def stop(): Unit = {
    super.stop()
    connection.close()
  }

  def executeQuery(sql: String): ResultSet = connection.createStatement().executeQuery(sql)

  def executeUpdate(sql: String): Int = connection.createStatement().executeUpdate(sql)
}
