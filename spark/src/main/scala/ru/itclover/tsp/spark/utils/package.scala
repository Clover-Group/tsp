package ru.itclover.tsp.spark

import org.apache.spark.sql.Row
import ru.itclover.tsp.core.Pattern.Idx

package object utils {
  type RowWithIdx = (Idx, Row)

  def RowWithIdx(idx: Idx, row: Row): RowWithIdx = (idx, row)
}
