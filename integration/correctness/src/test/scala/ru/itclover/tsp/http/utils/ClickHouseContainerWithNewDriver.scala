package ru.itclover.tsp.http.utils

import ru.itclover.tsp.http.utils.JavaClickHouseContainer
import org.testcontainers.utility.DockerImageName
import com.dimafeng.testcontainers.SingleContainer
import com.dimafeng.testcontainers.JdbcDatabaseContainer
import com.dimafeng.testcontainers.ContainerDef

case class ClickHouseContainerWithNewDriver(
  dockerImageName: DockerImageName = DockerImageName.parse(ClickHouseContainerWithNewDriver.defaultDockerImageName)
) extends SingleContainer[JavaClickHouseContainer]
    with JdbcDatabaseContainer {

  override val container: JavaClickHouseContainer = new JavaClickHouseContainer(dockerImageName)

  def testQueryString: String = container.getTestQueryString
}

object ClickHouseContainerWithNewDriver {

  val defaultDockerImageName = s"${JavaClickHouseContainer.IMAGE}:${JavaClickHouseContainer.DEFAULT_TAG}"

  case class Def(
    dockerImageName: DockerImageName = DockerImageName.parse(ClickHouseContainerWithNewDriver.defaultDockerImageName)
  ) extends ContainerDef {

    override type Container = ClickHouseContainerWithNewDriver

    override def createContainer(): ClickHouseContainerWithNewDriver = {
      new ClickHouseContainerWithNewDriver(
        dockerImageName = dockerImageName
      )
    }

  }

}
