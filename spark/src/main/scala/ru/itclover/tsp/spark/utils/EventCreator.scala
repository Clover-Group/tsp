package ru.itclover.tsp.spark.utils

import org.apache.spark.sql.Row
import ru.itclover.tsp.core.Pattern.Idx

trait EventCreator[Event, Key] extends Serializable {
  def create(kv: Seq[(Key, AnyRef)]): Event
}

object EventCreatorInstances {
  implicit val rowSymbolEventCreator: EventCreator[Row, Symbol] = new EventCreator[Row, Symbol] {
    override def create(kv: Seq[(Symbol, AnyRef)]): Row = {
      Row.fromSeq(kv.map(_._2))
    }
  }

  implicit val rowIntEventCreator: EventCreator[Row, Int] = new EventCreator[Row, Int] {
    override def create(kv: Seq[(Int, AnyRef)]): Row = {
      Row.fromSeq(kv.map(_._2))
    }
  }

  //todo change it to not have effects here
  implicit val rowWithIdxSymbolEventCreator: EventCreator[RowWithIdx, Symbol] = new EventCreator[RowWithIdx, Symbol] {
    override def create(kv: Seq[(Symbol, AnyRef)]): RowWithIdx = RowWithIdx(0, rowSymbolEventCreator.create(kv))
  }
}
