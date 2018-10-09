package ru.itclover.tsp.http.routes
import akka.http.scaladsl.server.Route
import ru.itclover.tsp.http.protocols.RoutesProtocols

object ValidationRoutes {

}

trait ValidationRoutes extends RoutesProtocols {
  val route: Route = path("rules" / "validate" ./) {

  }
}
