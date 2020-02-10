package ru.itclover.tsp.core

sealed trait PatternTag

case object GroupPatternTag extends PatternTag
case object PreviousValueTag extends PatternTag
case object TimerPatternTag extends PatternTag
case object TimestampAdderPatternTag extends PatternTag
case object WaitPatternTag extends PatternTag
case object WindowStatisticPatternTag extends PatternTag

case object AndThenPatternTag extends PatternTag
case object CouplePatternTag extends PatternTag
case object ConstPatternTag extends PatternTag
case object ExtractingTag extends PatternTag
case object MapPatternTag extends PatternTag
case object MapWithContextPatternTag extends PatternTag
case object ReducePatternTag extends PatternTag
case object SegmentizerPatternTag extends PatternTag
case object SimplePatternTag extends PatternTag
