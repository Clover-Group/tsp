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
