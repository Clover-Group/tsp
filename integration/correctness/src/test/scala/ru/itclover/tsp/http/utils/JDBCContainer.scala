package ru.itclover.tsp.http.utils

import java.sql.{Connection, DriverManager, ResultSet}

import com.dimafeng.testcontainers.SingleContainer
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.containers.{BindMode, GenericContainer => OTCGenericContainer}

import scala.collection.JavaConverters._

// Default arguments for constructing a container are useful
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
class JDBCContainer(
  imageName: String,
  val portsBindings: List[(Int, Int)] = List.empty,
  val driverName: String,
  val jdbcUrl: String,
  env: Map[String, String] = Map(),
  command: Seq[String] = Seq(),
  classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
  waitStrategy: Option[WaitStrategy] = None
) extends SingleContainer[OTCGenericContainer[_]] {

  implicit override val container = new OTCGenericContainer(imageName)

  if (portsBindings.nonEmpty) {
    val bindings = portsBindings.map { case (out, in) => s"${out.toString}:${in.toString}" }
    val exposedPorts = portsBindings.map(_._2)
    container.setPortBindings(bindings.asJava)
    container.addExposedPorts(exposedPorts: _*)
  }

  env.foreach(container.withEnv(_, _))

  if (command.nonEmpty) {
    val _ = container.withCommand(command: _*)
  }

  classpathResourceMapping.foreach(container.withClasspathResourceMapping(_, _, _))
  waitStrategy.foreach(container.waitingFor)

  // We cannot initialise `connection` here yet
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  var connection: Connection = _

  override def start(): Unit = {
    super.start()
    Thread.sleep(8000)
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
