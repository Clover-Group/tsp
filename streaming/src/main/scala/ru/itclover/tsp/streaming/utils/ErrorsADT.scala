package ru.itclover.tsp.streaming.utils

object ErrorsADT {

  sealed trait Err extends Product with Serializable {
    val error: String
    val errorCode: Int
  }

  /**
    * Represents errors on configuration stage. It is guaranteed that no events is written in sink if error occurs.
    */
  sealed trait ConfigErr extends Err

  case class InvalidRequest(error: String, errorCode: Int = 4010) extends ConfigErr

  case class InvalidPatternsCode(errors: Seq[String], errorCode: Int = 4020) extends ConfigErr {
    override val error = errors.mkString("\n")
  }

  case class SourceUnavailable(error: String, errorCode: Int = 4030) extends ConfigErr

  case class SinkUnavailable(error: String, errorCode: Int = 4040) extends ConfigErr

  case class GenericConfigError(ex: Exception, errorCode: Int = 4000) extends ConfigErr {
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
