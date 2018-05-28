package ru.itclover.streammachine.io.input


case class RawPattern(id: String, sourceCode: String, payload: Map[String, String] = Map.empty) extends Serializable
