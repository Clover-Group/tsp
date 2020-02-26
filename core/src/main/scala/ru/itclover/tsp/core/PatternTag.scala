package ru.itclover.tsp.core

import ru.itclover.tsp.core.aggregators._

sealed trait PatternTag extends Serializable

sealed trait WithoutInnerPatternTag extends PatternTag
case object ExtractingTag extends WithoutInnerPatternTag
case object SimplePatternTag extends WithoutInnerPatternTag
case object ConstPatternTag extends WithoutInnerPatternTag

sealed trait WithInnersPatternsTag extends PatternTag {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def getInnerPatterns[E](pattern: Pattern[E, _, _]): Seq[Pattern[E, _, _]] = {
    assume(pattern.patternTag eq this)
    this match {
      case ReducePatternTag            => pattern.asInstanceOf[ReducePattern[E, _, _, _]].patterns
      case tag: WithOneInnerPatternTag => Seq(tag.getInnerPattern(pattern))
      case tag: WithTwoInnersPatternTag =>
        val (x, y) = tag.getTwoInnerPatterns(pattern)
        Seq(x, y)
    }
  }
}

case object ReducePatternTag extends WithInnersPatternsTag

sealed trait WithOneInnerPatternTag extends WithInnersPatternsTag {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def getInnerPattern[E](pattern: Pattern[E, _, _]): Pattern[E, _, _] = {
    assume(pattern.patternTag eq this)
    this match {
      case GroupPatternTag           => pattern.asInstanceOf[GroupPattern[E, _, _]].inner
      case PreviousValueTag          => pattern.asInstanceOf[PreviousValue[E, _, _]].inner
      case TimerPatternTag           => pattern.asInstanceOf[TimerPattern[E, _, _]].inner
      case TimestampAdderPatternTag  => pattern.asInstanceOf[TimestampsAdderPattern[E, _, _]].inner
      case WaitPatternTag            => pattern.asInstanceOf[WaitPattern[E, _, _]].inner
      case WindowStatisticPatternTag => pattern.asInstanceOf[WindowStatistic[E, _, _]].inner
      case MapPatternTag             => pattern.asInstanceOf[MapPattern[E, _, _, _]].inner
      case MapWithContextPatternTag  => pattern.asInstanceOf[MapWithContextPattern[E, _, _, _]].inner
      case SegmentizerPatternTag     => pattern.asInstanceOf[SegmentizerPattern[E, _, _]].inner
    }
  }
}

case object GroupPatternTag extends WithOneInnerPatternTag
case object PreviousValueTag extends WithOneInnerPatternTag
case object TimerPatternTag extends WithOneInnerPatternTag
case object TimestampAdderPatternTag extends WithOneInnerPatternTag
case object WaitPatternTag extends WithOneInnerPatternTag
case object WindowStatisticPatternTag extends WithOneInnerPatternTag
case object MapPatternTag extends WithOneInnerPatternTag
case object MapWithContextPatternTag extends WithOneInnerPatternTag
case object SegmentizerPatternTag extends WithOneInnerPatternTag

sealed trait WithTwoInnersPatternTag extends WithInnersPatternsTag {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def getTwoInnerPatterns[E](pattern: Pattern[E, _, _]): (Pattern[E, _, _], Pattern[E, _, _]) = {
    assume(pattern.patternTag eq this)
    this match {
      case AndThenPatternTag =>
        val t = pattern.asInstanceOf[AndThenPattern[E, _, _, _, _]]
        (t.first, t.second)
      case CouplePatternTag =>
        val t = pattern.asInstanceOf[CouplePattern[E, _, _, _, _, _]]
        (t.left, t.right)
    }
  }

}

case object AndThenPatternTag extends WithTwoInnersPatternTag
case object CouplePatternTag extends WithTwoInnersPatternTag


object PatternTag {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def computeOffset(pattern: Pattern[_, _, _]): Window = {
    pattern.patternTag match {
      // handle special cases
      case AndThenPatternTag =>
        val (first, second) = AndThenPatternTag.getTwoInnerPatterns(pattern)
        Window(computeOffset(first).toMillis + computeOffset(second).toMillis)
      case TimerPatternTag => pattern.asInstanceOf[TimerPattern[_, _, _]].window
      case WaitPatternTag  => pattern.asInstanceOf[WaitPattern[_, _, _]].window

      // rest patterns are below
      case _: WithoutInnerPatternTag  => Window(0L)
      case tag: WithInnersPatternsTag =>
        //take the biggest offset for patterns with process patterns
        Window(tag.getInnerPatterns(pattern).map((computeOffset _).andThen(_.toMillis)).fold(0L)(Math.max))
    }

  }
}