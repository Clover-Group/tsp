package ru.itclover.streammachine.core

/**
  * Trait to create InitialState for PhaseParsers
  */
trait InitialState[A] {
  def initial: A
}

/**
  * Object with implicit instances of InitialState[A]
  */
object InitialState {

  implicit def optionInitialState[A]: InitialState[Option[A]] = new InitialState[Option[A]] {
    override def initial: Option[A] = None
  }

  implicit val unitInitialState: InitialState[Unit] = new InitialState[Unit] {
    override def initial: Unit = ()
  }

  implicit def tuple2InitialState[
  A: InitialState,
  B: InitialState
  ]: InitialState[(A, B)] =
    new InitialState[(A, B)] {

      override def initial: (A, B) = Tuple2(implicitly[InitialState[A]].initial, implicitly[InitialState[B]].initial)
    }

  implicit def tuple3InitialState[
  A: InitialState,
  B: InitialState,
  C: InitialState
  ]: InitialState[(A, B, C)] =
    new InitialState[(A, B, C)] {

      override def initial: (A, B, C) = Tuple3(
        implicitly[InitialState[A]].initial,
        implicitly[InitialState[B]].initial,
        implicitly[InitialState[C]].initial
      )

    }


  implicit def tuple4InitialState[
  A: InitialState,
  B: InitialState,
  C: InitialState,
  D: InitialState
  ]: InitialState[(A, B, C, D)] =
    new InitialState[(A, B, C, D)] {

      override def initial: (A, B, C, D) = Tuple4(
        implicitly[InitialState[A]].initial,
        implicitly[InitialState[B]].initial,
        implicitly[InitialState[C]].initial,
        implicitly[InitialState[D]].initial
      )

    }

  implicit def tuple5InitialState[
  A: InitialState,
  B: InitialState,
  C: InitialState,
  D: InitialState,
  E: InitialState
  ]: InitialState[(A, B, C, D, E)] =
    new InitialState[(A, B, C, D, E)] {

      override def initial: (A, B, C, D, E) = Tuple5(
        implicitly[InitialState[A]].initial,
        implicitly[InitialState[B]].initial,
        implicitly[InitialState[C]].initial,
        implicitly[InitialState[D]].initial,
        implicitly[InitialState[E]].initial
      )

    }


}
