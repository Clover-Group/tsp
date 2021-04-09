package ru.itclover.tsp.core

case class RawPattern(
  id: Int,
  sourceCode: String,
  subunit: Option[Int] = None,
) extends Serializable
