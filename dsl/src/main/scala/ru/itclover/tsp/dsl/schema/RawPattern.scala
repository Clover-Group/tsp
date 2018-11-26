package ru.itclover.tsp.dsl.schema

case class RawPattern(id: String, sourceCode: String, payload: Map[String, String] = Map.empty,
                      forwardedFields: Seq[String] = Seq.empty) extends Serializable
