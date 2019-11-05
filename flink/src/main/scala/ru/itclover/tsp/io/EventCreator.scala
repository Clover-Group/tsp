package ru.itclover.tsp.io
import org.apache.flink.types.Row
import ru.itclover.tsp.RowWithIdx

trait EventCreator[Event, Key] extends Serializable {
  def create(kv: Seq[(Key, AnyRef)]): Event
}

object EventCreatorInstances {
  implicit val rowSymbolEventCreator: EventCreator[Row, Symbol] = new EventCreator[Row, Symbol] {
    override def create(kv: Seq[(Symbol, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        row.setField(kvWithIndex._2, kvWithIndex._1._2)
      }
      row
    }
  }

  implicit val rowIntEventCreator: EventCreator[Row, Int] = new EventCreator[Row, Int] {
    override def create(kv: Seq[(Int, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        row.setField(kvWithIndex._2, kvWithIndex._1._2)
      }
      row
    }
  }

  //todo change it to not have effects here
  implicit val rowWithIdxSymbolEventCreator: EventCreator[RowWithIdx, Symbol] = new EventCreator[RowWithIdx, Symbol] {
    override def create(kv: Seq[(Symbol, AnyRef)]): RowWithIdx = RowWithIdx(0, rowSymbolEventCreator.create(kv))
  }
}
