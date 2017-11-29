/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.itclover


import java.util
import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

import scala.reflect.ClassTag


/**
  * The function that maintains the per-IP-address state machines and verifies that the
  * events are consistent with the current state of the state machine. If the event is not
  * consistent with the current state, the function produces an alert.
  */
case class StateMachineMapper[Event, State: ClassTag, Out](phaseParser: PhaseParser[Event, State, Out]) extends RichFlatMapFunction[Event, Out] with Serializable {

  @transient
  private[this] var currentState: ValueState[State] = _

  override def open(config: Configuration): Unit = {
    val classTag = implicitly[ClassTag[State]]
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classTag.runtimeClass.asInstanceOf[Class[State]], phaseParser.initialState))
  }

  override def flatMap(t: Event, outCollector: Collector[Out]): Unit = {
    val (result, newState) = phaseParser.apply(t, currentState.value())

    currentState.update(newState)

    result match {
      case Stay => println(s"Stay for event $t")
      case Failure(msg) =>
        //todo Should we try to run this message again?
        println(s"Failure for event $t: $msg")
        currentState.update(phaseParser.initialState)
      case Success(out) =>
        println(s"Success for event $t")
        outCollector.collect(out)
    }
  }

}