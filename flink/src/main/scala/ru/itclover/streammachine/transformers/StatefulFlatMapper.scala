package ru.itclover.streammachine.transformers

import org.apache.flink.api.common.functions.AbstractRichFunction


trait StatefulFlatMapper[In, State, Out] extends ((In, State) => (Seq[Out], State)) {
  def initialState: State
}

trait RichStatefulFlatMapper[In, State, Out] extends AbstractRichFunction with StatefulFlatMapper[In, State, Out]
