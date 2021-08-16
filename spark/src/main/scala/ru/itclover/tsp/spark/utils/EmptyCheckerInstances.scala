package ru.itclover.tsp.spark.utils

import org.apache.spark.sql.Row
import ru.itclover.tsp.core.Pattern.Idx
import ru.itclover.tsp.streaming.transformers.EmptyChecker

object EmptyCheckerInstances {
  val rowWithIdxEmptyChecker: EmptyChecker[RowWithIdx] = new EmptyChecker[RowWithIdx] {
    override def isEmpty(event: RowWithIdx): Boolean = event._2.length == 0
  }
}
