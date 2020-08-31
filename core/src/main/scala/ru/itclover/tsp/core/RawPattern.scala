package ru.itclover.tsp.core

case class RawPattern(
  id: String,
  sourceCode: String,
  payload: Option[Map[String, String]] = None,
  forwardedFields: Option[Seq[Symbol]] = None
) extends Serializable
