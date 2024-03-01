package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.core.io.TimeExtractor

trait EventToList[Event] {
  def toList(x: Event)(implicit timeExtractor: TimeExtractor[Event]): List[Any]
}

object EventToList {

  implicit val rowEventToList: EventToList[Row] = new EventToList[Row] {

    override def toList(x: Row)(implicit timeExtractor: TimeExtractor[Row]): List[Any] = timeExtractor(x) :: x.toList

  }

  implicit val rowWithIdxToList: EventToList[RowWithIdx] = new EventToList[RowWithIdx] {

    override def toList(x: RowWithIdx)(implicit timeExtractor: TimeExtractor[RowWithIdx]): List[Any] =
      x.idx :: timeExtractor(x) :: x.row.toList

  }

}
