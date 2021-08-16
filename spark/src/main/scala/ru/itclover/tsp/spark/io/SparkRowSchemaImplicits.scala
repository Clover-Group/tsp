package ru.itclover.tsp.spark.io

import org.apache.spark.sql.types.{DataType, DataTypes}
import ru.itclover.tsp.streaming.io.RowSchema

object SparkRowSchemaImplicits {
  implicit class SparkRowSchema(rowSchema: RowSchema) {
    val fieldDatatypes: List[DataType] =
      List(
        DataTypes.IntegerType,
        DataTypes.TimestampType,
        DataTypes.TimestampType,
        DataTypes.IntegerType,
        DataTypes.IntegerType,
        DataTypes.IntegerType
      )
  }
}