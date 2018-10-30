package ru.itclover.tsp.io
import org.apache.flink.types.Row

trait EventCreator[Event] extends Serializable {
  def create(kv: Seq[(Symbol, AnyRef)]): Event
  def emptyEvent(fieldsIndexesMap: Map[Symbol, Int]): Event
}

object EventCreatorInstances {
  implicit val rowEventCreator: EventCreator[Row] = new EventCreator[Row] {
    override def create(kv: Seq[(Symbol, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        if (kvWithIndex._1 != null) { row.setField(kvWithIndex._2, kvWithIndex._1._2) }
      }
      row
    }
    override def emptyEvent(fieldsIndexesMap: Map[Symbol, Int]): Row = {
      val row = new Row(fieldsIndexesMap.keySet.toSeq.length)
      fieldsIndexesMap.foreach { fi =>
        row.setField(fi._2, 0)
      }
      row
    }
    override def emptyEvent(fieldsIndexesMap: Map[Symbol, Int]): Row = {
      val row = new Row(fieldsIndexesMap.keySet.toSeq.length)
      fieldsIndexesMap.foreach { fi =>
        row.setField(fi._2, 0)
      }
      row
    }
  }
}
