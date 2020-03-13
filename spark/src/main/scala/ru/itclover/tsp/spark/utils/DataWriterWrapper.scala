package ru.itclover.tsp.spark.utils

import org.apache.spark.sql.{DataFrameWriter, SaveMode}
import org.apache.spark.sql.streaming.DataStreamWriter

sealed trait DataWriterWrapper[Event] {
  def write(): Unit
}


case class DataFrameWriterWrapper[Event](writer: DataFrameWriter[Event]) extends DataWriterWrapper[Event] {
  override def write(): Unit = writer.mode(SaveMode.Append).save()
}

case class DataStreamWriterWrapper[Event](writer: DataStreamWriter[Event]) extends DataWriterWrapper[Event] {
  override def write(): Unit = {
    val q = writer.start()
    // TODO: Stopping conditions?
    q.awaitTermination(10000)
    q.stop()
  }
}

object DataWriterWrapperImplicits {
  implicit def dfwWrapperFromWriter[Event](writer: DataFrameWriter[Event]): DataFrameWriterWrapper[Event] =
    DataFrameWriterWrapper(writer)

  implicit def dswWrapperFromWriter[Event](writer: DataStreamWriter[Event]): DataStreamWriterWrapper[Event] =
    DataStreamWriterWrapper(writer)
}