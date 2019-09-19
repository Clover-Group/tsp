package ru.itclover.tsp.dsl

import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.core.io.{Decoder, TimeExtractor}

import scala.reflect.ClassTag

object PatternsValidator {

  def validate[Event](
    patterns: Seq[RawPattern],
    fieldsTypes: Map[String, String]
  )(
    implicit timeExtractor: TimeExtractor[Event],
    //toNumberExtractor: Extractor[Event, Int, Any],
    doubleDecoder: Decoder[Any, Double]
  ): Seq[(RawPattern, Either[Throwable, AST])] = {
    // Since it's only the validation, we don't need any tolerance fraction here.
    patterns.map(
      p => (p, new ASTBuilder(p.sourceCode, 0.0, toClassTags(fieldsTypes)).start.run().toEither)
    )
  }

  def toClassTags(fields: Map[String, String]): Map[Symbol, ClassTag[_]] = fields.map {
    case (name, dataType) =>
      Symbol(name) -> (dataType match {
        case "float64" => ClassTag.Double
        case "float32" => ClassTag.Float
        case "int64"   => ClassTag.Long
        case "int32"   => ClassTag.Int
        case "int16"   => ClassTag.Short
        case "int8"    => ClassTag.Byte
        case "boolean" => ClassTag.Boolean
        case "string"  => ClassTag(classOf[String])
        case _         => ClassTag.Any
      })
  }.toMap
}
