package ru.itclover.tsp.streaming.utils

import doobie.Read

object RowImplicits {
  implicit val rowRead: Read[Row] = Read[Array[Any]]
}
