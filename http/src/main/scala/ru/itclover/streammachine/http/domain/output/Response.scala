package ru.itclover.streammachine.http.domain.output

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.unmarshalling.FromRequestUnmarshaller
import spray.json.JsonFormat


trait Response

final case class SuccessfulResponse[T](response: T) extends Response

final case class FailureResponse(errorCode: Int, message: String, errors: List[String]) extends Response
