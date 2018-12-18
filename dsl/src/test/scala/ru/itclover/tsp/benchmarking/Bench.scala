package ru.itclover.tsp.benchmarking
import monix.eval.Task
import ru.itclover.tsp.core.{Pattern, Time, Window}
import ru.itclover.tsp.dsl.PhaseBuilder
import ru.itclover.tsp.io.{AnyDecodersInstances, Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.patterns.Numerics.NumericPhaseParser
import ru.itclover.tsp.v2.Extract.{Idx, IdxExtractor}
import ru.itclover.tsp.v2.{Patterns, StateMachine}

import scala.concurrent.Await

object Bench extends App {

  case class Row(ts: Time, posKM: Int, speedEngine: Double)

  val bufferedSource = io.Source.fromFile("/Users/bfattahov/Downloads/2te25km_1485486.csv")

  val start = System.currentTimeMillis()

  private val lines = bufferedSource.getLines()
  val columnNames = lines.take(1).next().split(",").map(_.trim.replace("\"", "")).zipWithIndex.toMap

  val tsCol = columnNames("\uFEFFts")
  val PosKMCol = columnNames("PosKM")
  val speedEngineCol = columnNames("SpeedEngine")

  val rows = (for (line <- lines) yield {
    val cols = line.split(",")
    // do whatever you want with the columns here
    Row(
      Time((cols(tsCol).trim.toDouble * 1000).longValue()),
      cols(PosKMCol).trim.toInt,
      cols(speedEngineCol).trim.toDouble
    )
  }).zipWithIndex.toVector
  bufferedSource.close

  println(s"Read time (${rows.size}) is " + (System.currentTimeMillis() - start) + "ms")

  implicit val timeExtractor: TimeExtractor[Row] = TimeExtractor.of[Row](_.ts)

  case object RowSymbolExtractor extends Extractor[Row, Symbol, Any] {

    def apply[T](r: Row, s: Symbol)(implicit d: Decoder[Any, T]): T =
      d(s match {
        case 'PosKM       => r.posKM
        case 'SpeedEngine => r.speedEngine
      })
  }

  val pattern =
    PhaseBuilder
      .build("PosKM = 0 andThen SpeedEngine > 0 and (PosKM > 4 for  110 min < 60 sec)", identity _)(
        timeExtractor,
        RowSymbolExtractor,
        AnyDecodersInstances.decodeToDouble
      )
      .right
      .get
      ._1 //.asInstanceOf[Pattern[Product with Serializable, _ , _]]

//  println(pattern.format(rows.head._1))

//  val results = ru.itclover.tsp.core.runRule(pattern, rows.map(_._1))

  case class RowWithIdx(ts: Time, posKM: Int, speedEngine: Double, idx: Idx)
  import cats.implicits._
  import ru.itclover.tsp.core.Time._
  import scala.concurrent.duration._
  import monix.execution.Scheduler.Implicits.global

  // A Future type that is also Cancelable
  import monix.execution.CancelableFuture

  // Task is in monix.eval
  import monix.eval.Task

  implicit val timeExtractor2: TimeExtractor[RowWithIdx] = TimeExtractor.of[RowWithIdx](_.ts)
  implicit val idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of[RowWithIdx](_.idx)
  val newTypesHolder = new Patterns[RowWithIdx, cats.Id, List] {}
  import newTypesHolder._

  val pattern2 =
  assert(field(_.posKM) === const(0)) andThen
  timer(
    assert(
      (field(_.speedEngine) > const(0.0)) and
      (truthMillis(
        assert(field(_.posKM) > const(4)),
        210.minutes
      ) < const(60.seconds.toMillis))
    ),
    110.minutes
  )

  println(pattern2)

  val rowsWithIndex = rows.map {
    case (Row(time, posKm, speedEngine), idx) => RowWithIdx(time, posKm, speedEngine, idx)
  }

  val startRun = System.currentTimeMillis()

  val q = StateMachine.run(pattern2, rowsWithIndex)// .runToFuture

//  q.foreach { q => {
    println(s"Result size is ${q.size}")
    println(s"Running time = ${System.currentTimeMillis() - startRun} ms")
//  }
//  }
//
//  Await.result(q, Duration.Inf)
}
