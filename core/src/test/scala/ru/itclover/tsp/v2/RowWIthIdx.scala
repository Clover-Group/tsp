package ru.itclover.tsp.v2
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern.{Idx, IdxExtractor}

case class RowWithIdx(idx: Idx, ts: Time, value: Int)

object RowWIthIdxCompanion {

  implicit val timeExtractor: TimeExtractor[RowWithIdx] = TimeExtractor.of[RowWithIdx](_.ts)

  implicit val idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of[RowWithIdx](_.idx)

}
