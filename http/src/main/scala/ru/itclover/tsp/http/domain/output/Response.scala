package ru.itclover.tsp.http.domain.output

import akka.http.scaladsl.model.StatusCodes.ServerError
import ru.itclover.tsp.http.utils.Exceptions
import ru.itclover.tsp.streaming.utils.ErrorsADT.{ConfigErr, RuntimeErr}

sealed trait Response[T] extends Product with Serializable

final case class SuccessfulResponse[T](response: T, messages: Seq[String] = Seq.empty) extends Response[T]

object SuccessfulResponse {
  case class ExecInfo(execTimeSec: Double, extraMetrics: Map[String, Option[Long]])

  type FinishedJobResponse = SuccessfulResponse[ExecInfo]
}

final case class FailureResponse(errorCode: Int, message: String, errors: Seq[String]) extends Response[Unit]

object FailureResponse {
  def apply(e: ServerError): FailureResponse = FailureResponse(5000, e.defaultMessage, Seq(e.reason))

  def apply(code: Int, ex: Throwable): FailureResponse = {
    val stackTrace = Exceptions.getStackTrace(ex)
    val message = if (ex != null && ex.getCause != null) ex.getCause.getMessage else ex.getMessage
    FailureResponse(code, "Internal server error", Seq(message, stackTrace))
  }

  // def apply(code: Int, msg: String, errs: Seq[String]): FailureResponse = {
  //   // apply (code, msg, errs)
  //   val ex = new Throwable("Ex")
  //   apply(500, ex)
  // }

  def apply(ex: Throwable): FailureResponse = apply(5000, ex)

  def apply(err: ConfigErr): FailureResponse = {
    val msg = makeConfigErrMsg(err.getClass.getName)
    FailureResponse(err.errorCode, msg, Seq(err.error))
  }

  def apply(errs: Seq[ConfigErr]): FailureResponse = {
    val msg = makeConfigErrMsg(errs.map(_.getClass.getName).mkString(", "))
    FailureResponse(4000, msg, errs.map(_.error))
  }

  def apply(err: RuntimeErr): FailureResponse = {
    val msg = "Runtime error: " + err.getClass.getName
    FailureResponse(err.errorCode, msg, Seq(err.error))
  }

  def makeConfigErrMsg(error: String) = s"Configuration error: $error. Note: no data has been written to the Sink."
}
