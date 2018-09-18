package ru.itclover.tsp.io.input


case class RawPattern(id: String, sourceCode: String, payload: Map[String, String] = Map.empty,
                      forwardedFields: Seq[Symbol] = Seq.empty) extends Serializable
