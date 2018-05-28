package ru.itclover.streammachine.http.domain.output

import akka.http.scaladsl.model.StatusCodes.ServerError
import ru.itclover.streammachine.utils.Exceptions


trait Response


final case class SuccessfulResponse[T](response: T) extends Response


final case class FailureResponse(errorCode: Int, message: String, errors: Seq[String]) extends Response

object FailureResponse {
  def apply(e: ServerError): FailureResponse = FailureResponse(500, e.defaultMessage, Seq(e.reason))

  def apply(code: Int, ex: Throwable): FailureResponse = {
    val stackTrace = Exceptions.getStackTrace(ex)
    val message = if (ex != null && ex.getCause != null) ex.getCause.getMessage else ex.getMessage
    FailureResponse(500, "Internal server error", Seq(stackTrace))
  }

  def apply(ex: Throwable): FailureResponse = apply(500, ex)
}
