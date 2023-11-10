package ru.itclover.tsp.core.framework

import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.fixtures.Event

/** An abstract class for test context
  */
abstract class TestContext

/** Abstract builder for test context
  * @tparam Type
  *   type parameter for events value and state type
  * @tparam State
  *   type parameter for state value in patterns. Must be one type for event and pattern!
  */
abstract class AbstractContextBuilder[Type, State] {

  var patterns: Seq[Pattern[Event[Type], State, Type]] = _
  var events: Seq[Event[Type]] = _
  var finalState: State = _

  def withPatterns(patterns: Seq[Pattern[Event[Type], State, Type]]): AbstractContextBuilder[Type, State]
  def withEvents(events: Seq[Event[Type]]): AbstractContextBuilder[Type, State]
  def withFinalState(finalState: State): AbstractContextBuilder[Type, State]

  def build: TestContext

}

/** Builder, which will be used in context construction Deals with vars
  * @param builder
  *   abstract builder for setting context values
  * @tparam Type
  *   type parameter for events value and state type
  * @tparam State
  *   type parameter for state value in patterns. Must be one type for event and pattern!
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestContextBuilder[Type, State](builder: AbstractContextBuilder[Type, State]) extends TestContext {

  var patterns: Seq[Pattern[Event[Type], State, Type]] = builder.patterns
  var events: Seq[Event[Type]] = builder.events
  var finalState: State = builder.finalState

  override def toString: String =
    s"ContextBuilder(patterns = $patterns, events = $events, finalState = $finalState)"

}

/** Result context builder, which will be used in tests Deals with nulls and vars
  *
  * Usage example:
  *
  * {{{
  *   val testContextBuilder = new ResultContextBuilder[Int, Int]().withEvents(*events*)
  *                                                                .withPatterns(*patterns*)
  *                                                                .withFinalState(*state*)
  *   val testContext = testContextBuilder.build
  * }}}
  *
  * @tparam Type
  *   type parameter for events value and state type
  * @tparam State
  *   type parameter for state value in patterns. Must be one type for event and pattern!
  */
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class ResultContextBuilder[Type, State] extends AbstractContextBuilder[Type, State] {
  // override var patterns: Seq[Pattern[Event[Type], State, Type]] = _
  // override var events: Seq[Event[Type]] = _
  // override var finalState: State = _

  override def withPatterns(patterns: Seq[Pattern[Event[Type], State, Type]]): AbstractContextBuilder[Type, State] = {
    this.patterns = patterns
    this
  }

  override def withEvents(events: Seq[Event[Type]]): AbstractContextBuilder[Type, State] = {
    this.events = events
    this
  }

  override def withFinalState(finalState: State): AbstractContextBuilder[Type, State] = {
    this.finalState = finalState
    this
  }

  override def build: TestContext = new TestContextBuilder(builder = this)
}
