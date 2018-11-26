package ru.itclover.tsp.utils

object ErrorsADT {

  sealed trait Err extends Product with Serializable {
    val message: String
  }

  /**
    * Represents errors on configuration stage. It is guaranteed that no events is written in sink if error occurs.
    */
  sealed trait ConfigErr extends Err {
    def errorCode: Int = 400
  }

  case class InvalidRequest(message: String) extends ConfigErr
  case class SourceUnavailable(message: String) extends ConfigErr
  case class InvalidSourceCode(errors: Seq[String]) extends ConfigErr {
    override  val message = "Errors: \n" + errors.mkString("\n")
  }
  case class GenericConfigError(ex: Exception) extends ConfigErr {
    override val message = Option(ex.getMessage).getOrElse(ex.toString)
  }

  /**
    * Represents errors on run-time stage. Some data could be already written in the sink.
    */
  sealed trait RuntimeErr extends Err
  
  case class GenericRuntimeError(ex: Exception) extends RuntimeErr {
    override val message = Option(ex.getMessage).getOrElse(ex.toString)
  }
  
  // case class RecoverableError() extends RuntimeError
}
