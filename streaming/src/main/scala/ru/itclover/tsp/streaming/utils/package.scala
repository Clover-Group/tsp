package ru.itclover.tsp.streaming

import ru.itclover.tsp.core.Pattern.Idx

package object utils {
  type Row = Array[Any]
  type RowWithIdx = (Idx, Row)
}
