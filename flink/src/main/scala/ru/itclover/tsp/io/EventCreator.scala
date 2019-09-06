package ru.itclover.tsp.io
import org.apache.flink.types.Row

trait EventCreator[Event, Key] extends Serializable {
  def create(kv: Seq[(Key, AnyRef)]): Event
  def emptyEvent(fieldsIdxMap: Map[Key, Int]): Event
}

object EventCreatorInstances {
  implicit val rowEventCreator: EventCreator[Row, Symbol] = new EventCreator[Row, Symbol] {
    override def create(kv: Seq[(Symbol, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        row.setField(kvWithIndex._2, kvWithIndex._1._2)
      }
      row
    }
    override def emptyEvent(fieldsIdxMap: Map[Symbol, Int]): Row = {
      val row = new Row(fieldsIdxMap.keySet.toSeq.length)
      fieldsIdxMap.foreach { case (_, i) =>
        row.setField(i, 0)
      }
      row
    }
  }
}
