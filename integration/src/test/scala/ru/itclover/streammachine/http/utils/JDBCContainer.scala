package ru.itclover.streammachine.http.utils

import com.dimafeng.testcontainers.SingleContainer
import org.testcontainers.containers.wait.WaitStrategy
import org.testcontainers.containers.{BindMode, GenericContainer => OTCGenericContainer}
import collection.JavaConverters._


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
    container.withCommand(command: _*)
  }
  classpathResourceMapping.foreach(Function.tupled(container.withClasspathResourceMapping))
  waitStrategy.foreach(container.waitingFor)
}
