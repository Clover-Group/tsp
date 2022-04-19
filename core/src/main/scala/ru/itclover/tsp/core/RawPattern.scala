package ru.itclover.tsp.core

case class RawPattern(
  id: Int,
  sourceCode: String,
  metadata: Option[Map[String, String]] = None,
  subunit: Option[Int] = None,
) extends Serializable
