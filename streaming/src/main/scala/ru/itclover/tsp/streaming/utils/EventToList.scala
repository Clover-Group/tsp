package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp.RowWithIdx

trait EventToList[Event] {
  def toList(x: Event): List[Any]
}

object EventToList {

  implicit val rowEventToList: EventToList[Row] = new EventToList[Row] {

    override def toList(x: Row): List[Any] = x.toList

  }

  implicit val rowWithIdxToList: EventToList[RowWithIdx] = new EventToList[RowWithIdx] {

    override def toList(x: RowWithIdx): List[Any] = x.idx :: x.row.toList

  }

}
