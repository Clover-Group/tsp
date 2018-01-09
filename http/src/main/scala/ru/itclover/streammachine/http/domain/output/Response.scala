package ru.itclover.streammachine.http.domain.output

trait Response

final case class SuccessfulResponse(data: Int, code: Int = 0) extends Response

final case class FailureResponse(message: String, code: Int = 1, reason: String = "") extends Response
