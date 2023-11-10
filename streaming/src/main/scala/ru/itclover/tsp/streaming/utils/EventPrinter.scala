package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.StreamSource.Row

trait EventPrinter[Event] {
  def prettyPrint(event: Event): String
}

object EventPrinterInstances {

  implicit val rowEventPrinter: EventPrinter[Row] = new EventPrinter[Row] {
    override def prettyPrint(event: Row): String = s"Row[${event.mkString(", ")}]"
  }

  implicit val rowWithIdxEventPrinter: EventPrinter[RowWithIdx] = new EventPrinter[RowWithIdx] {

    override def prettyPrint(event: RowWithIdx): String =
      s"RowWithIdx[index = ${event.idx}, data = (${event.row.mkString(", ")})]"

  }

}
