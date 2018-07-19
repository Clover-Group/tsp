package ru.itclover.streammachine.aggregators.accums

import ru.itclover.streammachine.core.{PhaseParser, Window}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.aggregators.accums.{ContinuousStates => Qs, OneTimeStates => Ots}


/**
  * Complete set of one time (apply) and continuous accumulation functions
  */
trait AccumFunctions {
  object avg {
    def apply[E, S](p: PhaseParser[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Ots.NumericAccumState(window))({ case s: Ots.NumericAccumState => s.avg}, "avg")

    def continuous[E, S](p: PhaseParser[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Qs.NumericAccumState(window))({ case s: Qs.NumericAccumState => s.avg }, "avg")
  }

  object sum {
    def apply[E, S](p: PhaseParser[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Ots.NumericAccumState(window))({ case s: Ots.NumericAccumState => s.sum }, "sum")

    def continuous[E, S](p: PhaseParser[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Qs.NumericAccumState(window))({ case s: Qs.NumericAccumState => s.sum }, "sum")
  }

  object count {
    def apply[E, S, T](p: PhaseParser[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Ots.CountAccumState[T](window))({ case s: Ots.CountAccumState[_] => s.count }, "count")

    def continuous[E, S, T](p: PhaseParser[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Qs.CountAccumState[T](window))({ case s: Qs.CountAccumState[_] => s.count }, "count")
  }

  object truthCount {
    def apply[E, S](p: PhaseParser[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Ots.TruthAccumState(window))({ case s: Ots.TruthAccumState => s.truthCount }, "truthCount")

    def continuous[E, S](p: PhaseParser[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Qs.TruthAccumState(window))({ case s: Qs.TruthAccumState => s.truthCount }, "truthCount")
  }

  object millisCount {
    def apply[E, S, T](p: PhaseParser[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Ots.CountAccumState[T](window))({ case s: Ots.CountAccumState[_] => s.overallTimeMs.getOrElse(0L) }, "millisCount")

    def continuous[E, S, T](p: PhaseParser[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Qs.CountAccumState[T](window))({ case s: Qs.CountAccumState[_] => s.overallTimeMs.getOrElse(0L) }, "millisCount")
  }

  object truthMillisCount {
    def apply[E, S](p: PhaseParser[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Ots.TruthAccumState(window))({ case s: Ots.TruthAccumState => s.truthMillisCount }, "truthMillisCount")

    def continuous[E, S](p: PhaseParser[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
      new AccumPhase(p, window, Qs.TruthAccumState(window))({ case s: Qs.TruthAccumState => s.truthMillisCount }, "truthMillisCount")
  }

}
