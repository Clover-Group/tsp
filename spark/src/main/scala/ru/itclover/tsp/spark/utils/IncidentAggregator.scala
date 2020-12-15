package ru.itclover.tsp.spark.utils

import cats.kernel.Semigroup
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import ru.itclover.tsp.core.{Incident, Segment, Time}
import ru.itclover.tsp.core.IncidentInstances._

class IncidentAggregator extends UserDefinedAggregateFunction {
  def incidentSchema = ScalaReflection.schemaFor[Incident].dataType.asInstanceOf[StructType]

  override def inputSchema: StructType = incidentSchema

  override def bufferSchema: StructType = incidentSchema

  override def dataType: DataType = incidentSchema

  override def deterministic: Boolean = true

  var initialEvent: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    initialEvent = true
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val res: Incident =
      if (initialEvent) evaluate(input).asInstanceOf[Incident]
      else
        implicitly[Semigroup[Incident]].combine(
          evaluate(buffer).asInstanceOf[Incident],
          evaluate(input).asInstanceOf[Incident]
        )
    initialEvent = false
    buffer.update(0, res.id)
    buffer.update(1, res.patternId)
    buffer.update(2, res.maxWindowMs)
    buffer.update(3, res.segment)
    buffer.update(4, res.forwardedFields)
    buffer.update(5, res.patternSubunit)
    buffer.update(6, res.patternPayload)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val res: Incident =
      if (initialEvent) evaluate(buffer2).asInstanceOf[Incident]
      else
        implicitly[Semigroup[Incident]].combine(
          evaluate(buffer1).asInstanceOf[Incident],
          evaluate(buffer2).asInstanceOf[Incident]
        )
    initialEvent = false
    buffer1.update(0, res.id)
    buffer1.update(1, res.patternId)
    buffer1.update(2, res.maxWindowMs)
    buffer1.update(3, res.segment)
    buffer1.update(4, res.forwardedFields)
    buffer1.update(5, res.patternSubunit)
    buffer1.update(6, res.patternPayload)
  }

  override def evaluate(buffer: Row): Any = Incident(
    buffer.getString(0),
    buffer.getInt(1),
    buffer.getLong(2),
    Segment(Time(buffer.getAs[Row](3).getAs[Row](0).getLong(0)), Time(buffer.getAs[Row](3).getAs[Row](1).getLong(0))),
    buffer.getAs[Seq[(String, String)]](4),
    buffer.getInt(5),
    buffer.getAs[Seq[(String, String)]](6)
  )
}
