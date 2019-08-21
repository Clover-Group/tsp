package ru.itclover.tsp.core.framework

import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.fixtures.Event

/**
  * An abstract class for test context
  */
abstract class TestContext

/**
  * Abstract builder for test context
  * @tparam Type type parameter for events value and state type
  * @tparam State type parameter for state value in patterns. Must be one type for event and pattern!
  */
abstract class AbstractContextBuilder[Type, State] {

  var patterns: Seq[Pattern[Event[Type], State, Type]]
  var events: Seq[Event[Type]]

  def withPatterns(patterns: Seq[Pattern[Event[Type], State, Type]]): AbstractContextBuilder[Type, State]
  def withEvents(events: Seq[Event[Type]]): AbstractContextBuilder[Type, State]

  def build: TestContext

}

/**
  * Builder, which will be used in context construction
  * @param builder abstract builder for setting context values
  * @tparam Type type parameter for events value and state type
  * @tparam State type parameter for state value in patterns. Must be one type for event and pattern!
  */
class TestContextBuilder[Type, State](builder: AbstractContextBuilder[Type, State]) extends TestContext {

  var patterns: Seq[Pattern[Event[Type], State, Type]] = builder.patterns
  var events: Seq[Event[Type]] = builder.events

  override def toString: String = s"ContextBuilder(patterns = $patterns, events = $events)"

}

/**
  * Result context builder, which will be used in tests
  *
  * Usage example:
  *
  * {{{
  *   val testContextBuilder = new ResultContextBuilder[Int, Int]().withEvents(*events*)
  *                                                                .withPatterns(*patterns*)
  *   val testContext = testContextBuilder.build
  * }}}
  *
  * @tparam Type type parameter for events value and state type
  * @tparam State type parameter for state value in patterns. Must be one type for event and pattern!
  */
class ResultContextBuilder[Type, State] extends AbstractContextBuilder[Type, State] {
  override var patterns: Seq[Pattern[Event[Type], State, Type]] = _
  override var events: Seq[Event[Type]] = _

  override def withPatterns(patterns: Seq[Pattern[Event[Type], State, Type]]): AbstractContextBuilder[Type, State] = {
    this.patterns = patterns
    this
  }

  override def withEvents(events: Seq[Event[Type]]): AbstractContextBuilder[Type, State] = {
    this.events = events
    this
  }

  override def build: TestContext = new TestContextBuilder(builder = this)
}
