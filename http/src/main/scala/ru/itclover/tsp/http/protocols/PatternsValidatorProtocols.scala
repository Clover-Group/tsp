package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.dsl.PatternsValidatorConf
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat

case class ValidationResult(pattern: RawPattern, success: Boolean, context: String)

trait PatternsValidatorProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val rawPattern: RootJsonFormat[RawPattern] = jsonFormat4(RawPattern.apply)
  implicit val patterns: RootJsonFormat[PatternsValidatorConf] = jsonFormat2(PatternsValidatorConf.apply)
  implicit val patternResult: RootJsonFormat[ValidationResult] = jsonFormat3(ValidationResult.apply)
}
