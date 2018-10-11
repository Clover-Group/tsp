package ru.itclover.tsp.http.protocols
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.dsl.PatternsValidatorConf
import spray.json.DefaultJsonProtocol

trait PatternsValidatorProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val rules = jsonFormat1(PatternsValidatorConf.apply)
}
