package ru.itclover.tsp.streaming.io

import cats.effect.{IO, MonadCancelThrow, Resource}
import cats.implicits._
import doobie.WeakAsync.doobieWeakAsyncForAsync
import doobie.{ConnectionIO, FC, Transactor, Update0}
import doobie.implicits._
import doobie.util.fragment.Fragment
import fs2.Pipe
import ru.itclover.tsp.StreamSource.Row

import java.sql.Connection
import scala.util.control.NonFatal

trait OutputConf[Event] {

  def getSink: Pipe[IO, Event, Unit]

  def parallelism: Option[Int]

  def rowSchema: EventSchema
}

/**
 * Sink for anything that support JDBC connection
 * @param rowSchema schema of writing rows
 * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
 * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
 * @param userName for JDBC auth
 * @param password for JDBC auth
 * @param batchInterval batch size for writing found incidents
 * @param parallelism num of parallel task to write data
 */
case class JDBCOutputConf(
                           tableName: String,
                           rowSchema: EventSchema,
                           jdbcUrl: String,
                           driverName: String,
                           password: Option[String] = None,
                           batchInterval: Option[Int] = None,
                           userName: Option[String] = None,
                           parallelism: Option[Int] = Some(1)
                         ) extends OutputConf[Row] {
  override def getSink: Pipe[IO, Row, Unit] =
    source => fuseMap(source, insertQuery)(transactor).drain

  lazy val transactor = Transactor.fromDriverManager[IO](
    driverName,
    jdbcUrl,
    userName.getOrElse(""),
    password.getOrElse("")
  )

  def insertQuery(data: Row): ConnectionIO[Int] = {
    val fields = rowSchema.fieldsNames.map(_.name).mkString(", ")
    (
      fr"""insert into """
      ++ Fragment.const(s"$tableName ($fields)")
      ++ fr"values ("
      ++ data.toList.map(x => fr"${x.toString}").intercalate(fr",")
      ++ fr")"
    ).update.run
  }

  def fuseMap[A, B](
                           source: fs2.Stream[IO, A],
                           sink:   A => ConnectionIO[B]
                         )(
                           sinkXA:   Transactor[IO]
                         ): fs2.Stream[IO, B] =
    fuseMapGeneric(source, identity[A], sink)(sinkXA)

  def fuseMapGeneric[F[_], A, B, C](
                                     source:       fs2.Stream[IO, A],
                                     sourceToSink: A => B,
                                     sink:         B => ConnectionIO[C]
                                   )(
                                     sinkTransactor:   Transactor[F]
                                   )(
                                     implicit ev: MonadCancelThrow[F]
                                   ): fs2.Stream[F, C] = {

    // Interpret a ConnectionIO into a Kleisli arrow for F via the sink interpreter.
    def interpS[T](f: ConnectionIO[T]): Connection => F[T] =
      f.foldMap(sinkTransactor.interpret).run

    // Open a connection in `F` via the sink transactor. Need patmat due to the existential.
    val conn: Resource[F, Connection] =
      sinkTransactor match { case xa => xa.connect(xa.kernel) }

    // Given a Connection we can construct the stream we want.
    def mkStream(c: Connection): fs2.Stream[F, C] = {

      // Now we can interpret a ConnectionIO into a Stream of F via the sink interpreter.
      def evalS(f: ConnectionIO[_]): fs2.Stream[F, Nothing] =
        fs2.Stream.eval(interpS(f)(c)).drain

      // And can thus lift all the sink operations into Stream of F
      val sinkEval: A => fs2.Stream[F, C]  = (a: A) => evalS(sink(sourceToSink(a)))
      //val before = evalS(sinkXA.strategy.before)
      //val after  = evalS(sinkXA.strategy.after )
      def oops(t: Throwable) = evalS(sinkTransactor.strategy.oops <* FC.raiseError(t))

      // And construct our final stream.
      //before ++ source.flatMap(sinkEval) ++ after
      source.flatMap(sinkEval).asInstanceOf[fs2.Stream[F, C]]
    }

    // And we're done!
    fs2.Stream.resource(conn).flatMap(mkStream)

  }
}

///**
//  * "Empty" sink (for example, used if one need only to report timings)
//  */
//case class EmptyOutputConf() extends OutputConf[Row] {
//  override def forwardedFieldsIds: Seq[Symbol] = Seq()
//  override def getOutputFormat: OutputFormat[Row] = ???
//  override def parallelism: Option[Int] = Some(1)
//}

/**
 * Sink for kafka connection
 * @param broker host and port for kafka broker
 * @param topic where is data located
 * @param serializer format of data in kafka
 * @param rowSchema schema of writing rows
 * @param parallelism num of parallel task to write data
 * @author trolley813
 */
case class KafkaOutputConf(
                            broker: String,
                            topic: String,
                            serializer: Option[String] = Some("json"),
                            rowSchema: EventSchema,
                            parallelism: Option[Int] = Some(1)
                          ) extends OutputConf[Row] {
  override def getSink: Pipe[IO, Row, Unit] = ???
}
