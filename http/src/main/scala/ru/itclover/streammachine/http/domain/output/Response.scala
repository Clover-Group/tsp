package ru.itclover.streammachine.http.domain.output

import akka.http.scaladsl.model.StatusCodes.ServerError


trait Response


final case class SuccessfulResponse[T](response: T) extends Response


final case class FailureResponse(errorCode: Int, message: String, errors: Seq[String]) extends Response

object FailureResponse {
  def apply(e: ServerError): FailureResponse = FailureResponse(0, e.defaultMessage, Seq(e.reason))

  def apply(ex: Exception): FailureResponse = FailureResponse(0, "Internal server error", Seq(ex.getMessage))
}
