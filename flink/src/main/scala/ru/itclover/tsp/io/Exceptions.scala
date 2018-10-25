package ru.itclover.tsp.io

object Exceptions {
  case class InvalidRequest(msg: String) extends RuntimeException(msg)
}
