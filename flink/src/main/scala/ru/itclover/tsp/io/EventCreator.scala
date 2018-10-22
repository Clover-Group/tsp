package ru.itclover.tsp.io
import org.apache.flink.types.Row

trait EventCreator[Event] {
  def create(kv: Seq[(Symbol, AnyRef)]): Event
}

object EventCreatorInstances {
  implicit val rowEventCreator: EventCreator[Row] = new EventCreator[Row] {
    override def create(kv: Seq[(Symbol, AnyRef)]): Row = {
      val row = new Row(kv.length)
      kv.zipWithIndex.foreach { kvWithIndex =>
        row.setField(kvWithIndex._2, kvWithIndex._1._2)
      }
      row
    }
  }
}
