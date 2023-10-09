package ru.itclover.tsp.streaming.utils

import cats.effect.Concurrent
import cats.effect.kernel.Ref
import cats.effect.std.Queue
import cats.implicits.{catsSyntaxOptionId, catsSyntaxTuple2Semigroupal, toFlatMapOps, toFunctorOps, toTraverseOps}
import fs2.Pipe

object StreamPartitionOps {

  def groupBy[F[_], A, K](selector: A => F[K])(implicit F: Concurrent[F]): Pipe[F, A, (K, fs2.Stream[F, A])] = { in =>
    fs2.Stream.eval(Ref.of[F, Map[K, Queue[F, Option[A]]]](Map.empty)).flatMap { st =>
      val cleanup = {
        st.get.flatMap(_.toList.traverse(_._2.offer(None))).map(_ => ())
      }

      (in ++ fs2.Stream.exec(cleanup))
        .evalMap { el =>
          (selector(el), st.get).mapN { (key, queues) =>
            queues
              .get(key)
              .fold {
                for {
                  newQ <- Queue.bounded[F, Option[A]](10000) // Create a new queue
                  _    <- st.modify(x => (x + (key -> newQ), x)) // Update the ref of queues
                  _    <- newQ.offer(el.some)
                } yield (key -> fs2.Stream.fromQueueNoneTerminated(newQ)).some
              }(_.offer(el.some).as[Option[(K, fs2.Stream[F, A])]](None))
          }.flatten
        }
        .unNone
        .onFinalize(cleanup)
    }
  }
}
