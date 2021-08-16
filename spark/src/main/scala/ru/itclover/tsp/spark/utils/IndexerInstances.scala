package ru.itclover.tsp.spark.utils

import org.apache.spark.sql.Row
import ru.itclover.tsp.core.Pattern.Idx
import ru.itclover.tsp.streaming.utils.Indexer

object IndexerInstances {
  implicit val rowWithIdxIndexer: Indexer[RowWithIdx, Row] = new Indexer[RowWithIdx, Row] {
    override def isIndexed: Boolean = true

    override def getUnindexedEvent(event: RowWithIdx): Row = event._2
  }
}
