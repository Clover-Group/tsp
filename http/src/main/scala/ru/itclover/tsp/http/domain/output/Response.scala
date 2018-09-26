package ru.itclover.tsp.http.domain.output

import akka.http.scaladsl.model.StatusCodes.ServerError
import ru.itclover.tsp.utils.Exceptions


trait Response[T] extends Product with Serializable


final case class SuccessfulResponse[T](response: T, messages: Seq[String]=Seq.empty) extends Response[T]

case class ExecInfo(execTimeSec: Long, extraMetrics: Map[String, Option[Long]])
final case class FinishedJobResponse(response: ExecInfo, messages: Seq[String]=Seq.empty) extends Response[ExecInfo]


final case class FailureResponse(errorCode: Int, message: String, errors: Seq[String]) extends Response[Unit]

object FailureResponse {
  def apply(e: ServerError): FailureResponse = FailureResponse(5000, e.defaultMessage, Seq(e.reason))

  def apply(code: Int, ex: Throwable): FailureResponse = {
    val stackTrace = Exceptions.getStackTrace(ex)
    val message = if (ex != null && ex.getCause != null) ex.getCause.getMessage else ex.getMessage
    FailureResponse(5000, "Internal server error", Seq(message, stackTrace))
  }

  def apply(ex: Throwable): FailureResponse = apply(5000, ex)
}
