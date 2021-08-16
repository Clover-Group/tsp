package ru.itclover.tsp.spark.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import ru.itclover.tsp.streaming.utils.EventCreator

object EventCreatorInstances {
  implicit val rowSymbolEventCreator: EventCreator[Row, Symbol, StructType] =
    (kv: Seq[(Symbol, AnyRef)], schema: StructType) => {
      new GenericRowWithSchema(kv.map(_._2).toArray, schema)
    }

  implicit val rowIntEventCreator: EventCreator[Row, Int, StructType] =
    (kv: Seq[(Int, AnyRef)], schema: StructType) => {
      new GenericRowWithSchema(kv.map(_._2).toArray, schema)
    }

  //todo change it to not have effects here
  implicit val rowWithIdxSymbolEventCreator: EventCreator[RowWithIdx, Symbol, StructType] =
    (kv: Seq[(Symbol, AnyRef)], schema: StructType) =>
      RowWithIdx(0, rowSymbolEventCreator.create(kv, schema))
}
