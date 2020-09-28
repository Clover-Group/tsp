package ru.itclover.tsp.core

case class RawPattern(
  id: Int,
  sourceCode: String,
  payload: Option[Map[String, String]] = None,
  subunit: Option[Int] = None,
  forwardedFields: Option[Seq[Symbol]] = None
) extends Serializable
