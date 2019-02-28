//package ru.itclover.tsp.utils
//import scala.util.Try
//
//trait KeyCreator[Key] extends Serializable {
//  def create(keyName: String): Key
//}
//
//
//object KeyCreatorInstances {
//  implicit val intKeyCreator: KeyCreator[Int] = new KeyCreator[Int] {
//    override def create(keyName: String): Int = Try(keyName.toInt).getOrElse(0)
//  }
//
//  implicit val symbolKeyCreator: KeyCreator[Symbol] = new KeyCreator[Symbol] {
//    override def create(keyName: String): Symbol = Symbol(keyName)
//  }
//}