//package ru.itclover.tsp.v2.aggregators.accums
//
//import ru.itclover.tsp.aggregators.AggregatorPhases.PreviousValue
//import ru.itclover.tsp.core.{Pattern, Window}
//import ru.itclover.tsp.io.TimeExtractor
//import ru.itclover.tsp.aggregators.accums.{ContinuousStates => Qs, OneTimeStates => Ots}
//
///**
//  * Complete set of one time (apply) and continuous accumulation functions
//  */
//trait AccumFunctions {
//
//  object avg {
//
//    def apply[E, S](p: Pattern[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Ots.NumericAccumState(window))({
//        case s: Ots.NumericAccumState => s.avg
//      }, "avg") {
//        override def toContinuous: AccumPattern[E, S, Double, Double] =
//          new AccumPattern(p, window, Qs.NumericAccumState(window))(
//            { case s: Qs.NumericAccumState => s.avg },
//            "avg.continuous"
//          )
//      }
//
//    def continuous[E, S](p: Pattern[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.NumericAccumState(window))(
//        { case s: Qs.NumericAccumState => s.avg },
//        "avg.continuous"
//      )
//  }
//
//  object sum {
//
//    def apply[E, S](p: Pattern[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Ots.NumericAccumState(window))({
//        case s: Ots.NumericAccumState => s.sum
//      }, "sum") {
//        override def toContinuous: AccumPattern[E, S, Double, Double] =
//          new AccumPattern(p, window, Qs.NumericAccumState(window))(
//            { case s: Qs.NumericAccumState => s.sum },
//            "sum.continuous"
//          )
//      }
//
//    def continuous[E, S](p: Pattern[E, S, Double], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.NumericAccumState(window))(
//        { case s: Qs.NumericAccumState => s.sum },
//        "sum.continuous"
//      )
//  }
//
//  object count {
//
//    def apply[E, S, T](p: Pattern[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Ots.CountAccumState[T](window))({
//        case s: Ots.CountAccumState[_] => s.count
//      }, "count") {
//        override def toContinuous: AccumPattern[E, S, T, Long] =
//          new AccumPattern(p, window, Qs.CountAccumState[T](window))(
//            { case s: Qs.CountAccumState[_] => s.count },
//            "count.continuous"
//          )
//      }
//
//    def continuous[E, S, T](p: Pattern[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.CountAccumState[T](window))(
//        { case s: Qs.CountAccumState[_] => s.count },
//        "count.continuous"
//      )
//  }
//
//  object truthCount {
//
//    def apply[E, S](p: Pattern[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Ots.TruthAccumState(window))(
//        { case s: Ots.TruthAccumState => s.truthCount },
//        "truthCount"
//      ) {
//        override def toContinuous: AccumPattern[E, S, Boolean, Long] =
//          new AccumPattern(p, window, Qs.TruthAccumState(window))(
//            { case s: Qs.TruthAccumState => s.truthCount },
//            "truthCount.continuous"
//          )
//      }
//
//    def continuous[E, S](p: Pattern[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.TruthAccumState(window))(
//        { case s: Qs.TruthAccumState => s.truthCount },
//        "truthCount.continuous"
//      )
//  }
//
//  object millisCount {
//
//    def apply[E, S, T](p: Pattern[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Ots.CountAccumState[T](window))({
//        case s: Ots.CountAccumState[_] => s.overallTimeMs.getOrElse(0L)
//      }, "millisCount") {
//        override def toContinuous: AccumPattern[E, S, T, Long] =
//          new AccumPattern(p, window, Qs.CountAccumState[T](window))({
//            case s: Qs.CountAccumState[_] => s.overallTimeMs.getOrElse(0L)
//          }, "millisCount.continuous")
//      }
//
//    def continuous[E, S, T](p: Pattern[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.CountAccumState[T](window))({
//        case s: Qs.CountAccumState[_] => s.overallTimeMs.getOrElse(0L)
//      }, "millisCount.continuous")
//  }
//
//  object truthMillisCount {
//
//    def apply[E, S](p: Pattern[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) = {
//      new AccumPattern(p, window, Ots.TruthAccumState(window))(
//        { case s: Ots.TruthAccumState => s.truthMillisCount },
//        "truthMillisCount"
//      ) {
//        override def toContinuous: AccumPattern[E, S, Boolean, Long] =
//          new AccumPattern(p, window, Qs.TruthAccumState(window))(
//            { case s: Qs.TruthAccumState => s.truthMillisCount },
//            "truthMillisCount.continuous"
//          )
//      }
//    }
//
//    def continuous[E, S](p: Pattern[E, S, Boolean], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.TruthAccumState(window))(
//        { case s: Qs.TruthAccumState => s.truthMillisCount },
//        "truthMillisCount.continuous"
//      )
//
//  }
//
//  object lag {
//
//    def apply[E, S, T](p: Pattern[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Ots.LagState[T](window))({
//        case s: Ots.LagState[T] => s.value.getOrElse(null.asInstanceOf[T])
//      }, "lag") {
//        override def toContinuous: AccumPattern[E, S, T, T] =
//          new AccumPattern(p, window, Qs.LagState(window))(
//            { case s: Qs.LagState[T] => s.value.getOrElse(null.asInstanceOf[T]) },
//            "lag.continuous"
//          )
//      }
//
//    def continuous[E, S, T](p: Pattern[E, S, T], window: Window)(implicit timeExtractor: TimeExtractor[E]) =
//      new AccumPattern(p, window, Qs.LagState[T](window))(
//        { case s: Qs.LagState[T] => s.value.getOrElse(null.asInstanceOf[T]) },
//        "lag.continuous"
//      )
//  }
//
//}
