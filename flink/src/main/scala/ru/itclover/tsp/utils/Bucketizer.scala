package ru.itclover.tsp.utils
import ru.itclover.tsp.PatternsSearchJob.RichPattern
import scala.collection.immutable

object Bucketizer {

  case class Bucket[T](totalWeight: Long, items: Seq[T])

  def bucketizeByWeight[T: WeightExtractor](items: Seq[T], numBuckets: Int): Vector[Bucket[T]] = {
    require(numBuckets > 0, s"Cannot bucketize to $numBuckets buckets")
    val initBuckets = Vector.fill(numBuckets)(Bucket[T](0, List.empty))
    val bigToSmallItems = items.sortBy(implicitly[WeightExtractor[T]].apply(_)).reverse
    bigToSmallItems.foldLeft(initBuckets) {
      case (buckets, item) => {
        // TODO OPTIMIZE try to use min-heap here to retrieve min bucket
        val minBucketInd = buckets.zipWithIndex.minBy(_._1.totalWeight)._2
        val minBucket = buckets(minBucketInd)
        val windSize = implicitly[WeightExtractor[T]].apply(item)
        buckets.updated(minBucketInd, Bucket(minBucket.totalWeight + windSize, minBucket.items :+ item))
      }
    }
  }
  
  def bucketsToString[T](buckets: Seq[Bucket[T]]) =
    buckets.map(b => {
      val itemsStr = b.items.mkString("`", "`, `", "`")
      s"Bucket weight: ${b.totalWeight}, Bucket items: $itemsStr"
    }).mkString("\n\n")


  trait WeightExtractor[T] extends (T => Long)

  object WeightExtractorInstances {

    implicit def unitWeightExtractor[T] = new WeightExtractor[T] {
      override def apply(v1: T) = 1L
    }

    implicit def phasesWeightExtrator[Event] = new WeightExtractor[RichPattern[Event]] {
      override def apply(item: RichPattern[Event]) = item._1._2.maxWindowMs
    }
  }

}
