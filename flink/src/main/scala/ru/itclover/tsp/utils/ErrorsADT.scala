package ru.itclover.tsp.utils

object ErrorsADT {

  sealed trait Err extends Product with Serializable {
    val error: String
    def errorCode: Int
  }

  /**
    * Represents errors on configuration stage. It is guaranteed that no events is written in sink if error occurs.
    */
  sealed trait ConfigErr extends Err {
    def errorCode: Int = 4000
  }


  case class InvalidRequest(error: String) extends ConfigErr

  case class SourceUnavailable(error: String) extends ConfigErr

  case class SinkUnavailable(error: String) extends ConfigErr

  case class InvalidPatternsCode(errors: Seq[String]) extends ConfigErr {
    override val error = errors.mkString("\n")
  }

  case class GenericConfigError(ex: Exception) extends ConfigErr {
    override val error = Option(ex.getMessage).getOrElse(ex.toString)
  }

  /**
    * Represents errors on run-time stage. Some data could be already written in the sink.
    */
  sealed trait RuntimeErr extends Err

  case class GenericRuntimeErr(ex: Throwable, errorCode: Int = 5000) extends RuntimeErr {
    override val error = Option(ex.getMessage).getOrElse(ex.toString)
  }

  // case class RecoverableError() extends RuntimeError
}
