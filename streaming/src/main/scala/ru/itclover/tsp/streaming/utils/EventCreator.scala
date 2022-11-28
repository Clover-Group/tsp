package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.RowWithIdx
import ru.itclover.tsp.StreamSource.Row

trait EventCreator[Event, Key] extends Serializable {
  def create(kv: Seq[(Key, AnyRef)]): Event
}

object EventCreatorInstances {
  implicit val rowSymbolEventCreator: EventCreator[Row, String] = new EventCreator[Row, String] {
    override def create(kv: Seq[(String, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        row(kvWithIndex._2) = kvWithIndex._1._2
      }
      row
    }
  }

  implicit val rowIntEventCreator: EventCreator[Row, Int] = new EventCreator[Row, Int] {
    override def create(kv: Seq[(Int, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        row(kvWithIndex._2) = kvWithIndex._1._2
      }
      row
    }
  }

  //todo change it to not have effects here
  implicit val rowWithIdxSymbolEventCreator: EventCreator[RowWithIdx, String] = new EventCreator[RowWithIdx, String] {
    override def create(kv: Seq[(String, AnyRef)]): RowWithIdx = RowWithIdx(0, rowSymbolEventCreator.create(kv))
  }
}
