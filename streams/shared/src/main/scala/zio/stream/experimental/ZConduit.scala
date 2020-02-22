package zio.stream.experimental

import zio._

sealed abstract class ZConduit[-R, +E, -I, +O, +Z] private[stream] (
  val run: ZManaged[R, Nothing, Chunk[I] => ZIO[R, Either[E, Z], Chunk[O]]]
) extends Serializable

sealed abstract class ZStream[-R, +E, +O](
  val process: ZManaged[R, Nothing, ZIO[R, Either[E, Unit], Chunk[O]]]
) extends ZConduit[R, E, Unit, O, Unit](process.map(pull => _ => pull)) { self =>
  import ZStream.Pull

  def filter(f: O => Boolean): ZStream[R, E, O] =
    ZStream(self.process.map(_.map(_.filter(f))))

  /**
   * Returns a stream made of the concatenation in strict order of all the streams
   * produced by passing each element of this stream to `f0`
   */
  def flatMap[R1 <: R, E1 >: E, O2](f0: O => ZStream[R1, E1, O2]): ZStream[R1, E1, O2] = {
    def go(
      outerStream: ZIO[R1, Either[E1, Unit], Chunk[O]],
      currOuterChunk: Ref[Chunk[O]],
      currOuterChunkIdx: Ref[Int],
      finalizer: Ref[Exit[_, _] => URIO[R1, _]],
      currInnerStream: Ref[ZIO[R1, Either[E1, Unit], Chunk[O2]]]
    ): ZIO[R1, Either[E1, Unit], Chunk[O2]] = {
      def pullOuter: ZIO[R1, Either[E1, Unit], Unit] = ZIO.uninterruptibleMask { restore =>
        for {
          outerChunk <- currOuterChunk.get
          outerIdx   <- currOuterChunkIdx.get
          _ <- if (outerIdx >= outerChunk.size)
                restore(outerStream).flatMap { o =>
                  if (o.isEmpty) pullOuter
                  else
                    (for {
                      _           <- currOuterChunk.set(o)
                      _           <- currOuterChunkIdx.set(1)
                      reservation <- f0(o(0)).process.reserve
                      innerStream <- restore(reservation.acquire)
                      _           <- finalizer.set(reservation.release)
                      _           <- currInnerStream.set(innerStream)
                    } yield ())
                } else
                (for {
                  _           <- currOuterChunkIdx.update(_ + 1)
                  reservation <- f0(outerChunk(outerIdx)).process.reserve
                  innerStream <- restore(reservation.acquire)
                  _           <- finalizer.set(reservation.release)
                  _           <- currInnerStream.set(innerStream)
                } yield ())

        } yield ()
      }

      currInnerStream.get.flatten.catchAllCause { c =>
        Cause.sequenceCauseEither(c) match {
          case Right(e) => ZIO.halt(e.map(Left(_)))
          case Left(_) =>
            finalizer.modify(fin => (fin(Exit.succeed(())), _ => UIO.unit)).flatten.uninterruptible *>
              pullOuter *>
              go(outerStream, currOuterChunk, currOuterChunkIdx, finalizer, currInnerStream)
        }
      }
    }

    ZStream {
      for {
        currInnerStream   <- Ref.make[ZIO[R1, Either[E1, Unit], Chunk[O2]]](ZIO.fail(Right(()))).toManaged_
        currOuterChunk    <- Ref.make[Chunk[O]](Chunk.empty).toManaged_
        currOuterChunkIdx <- Ref.make[Int](-1).toManaged_
        outerStream       <- self.process
        finalizer         <- ZManaged.finalizerRef[R1](_ => UIO.unit)
      } yield go(outerStream, currOuterChunk, currOuterChunkIdx, finalizer, currInnerStream)
    }
  }

  def map[O2](f: O => O2): ZStream[R, E, O2] =
    ZStream(self.process.map(_.map(_.map(f))))

  def mapConcat[O2](f: O => Iterable[O2]): ZStream[R, E, O2] =
    ZStream(self.process.map(_.map(_.flatMap(o => Chunk.fromIterable(f(o))))))

  /**
   * Takes all elements of the stream for as long as the specified predicate
   * evaluates to `true`.
   */
  final def takeWhile(pred: O => Boolean): ZStream[R, E, O] =
    ZStream {
      for {
        done <- Ref.make(false).toManaged_
        as   <- self.process
        pull = done.get.flatMap {
          if (_)
            Pull.end
          else
            as.flatMap { chunk =>
              val res = chunk.takeWhile(pred)
              res.length match {
                case 0                     => done.set(true) *> Pull.end
                case n if n < chunk.length => done.set(true) *> Pull.emit(res)
                case _                     => Pull.emit(res)
              }
            }
        }
      } yield pull
    }
}

object ZStream {
  object Pull {
    def emit[A](a: A): IO[Nothing, Chunk[A]]                  = UIO(Chunk.single(a))
    def emit[A](as: Chunk[A]): IO[Nothing, Chunk[A]]          = UIO(as)
    def fail[E](e: E): IO[Either[E, Nothing], Nothing]        = IO.fail(Left(e))
    def halt[E](c: Cause[E]): IO[Either[E, Nothing], Nothing] = IO.halt(c).mapError(Left(_))
    val end: IO[Either[Nothing, Unit], Nothing]               = IO.fail(Right(()))
  }

  def apply[R, E, O](
    process: ZManaged[R, Nothing, ZIO[R, Either[E, Unit], Chunk[O]]]
  ): ZStream[R, E, O] =
    new ZStream(process) {}

  /**
   * The stream that always dies with the `ex`.
   *
   * @param ex The exception that kills the stream
   * @return a stream that dies with an exception
   */
  def die(ex: => Throwable): UStream[Nothing] =
    halt(Cause.die(ex))

  /**
   * The stream that always dies with an exception described by `msg`.
   *
   * @param msg The message to feed the runtime exception
   * @return a stream that dies with a runtime exception
   */
  def dieMessage(msg: => String): UStream[Nothing] =
    halt(Cause.die(new RuntimeException(msg)))

  /**
   * The empty stream
   */
  val empty: UStream[Nothing] =
    ZStream(Managed.succeedNow(Pull.end))

  /**
   * Creates a stream from a [[zio.Chunk]] of values
   *
   * @tparam A the value type
   * @param c a chunk of values
   * @return a finite stream of values
   */
  def fromChunk[O](c: => Chunk[O]): ZStream[Any, Nothing, O] =
    ZStream(Managed.succeedNow(UIO.succeed(c)))

  /**
   * The stream that always halts with `cause`.
   *
   * @tparam The error type
   * @param the cause for halting the stream
   * @return a stream that is halted
   */
  def halt[E](cause: => Cause[E]): Stream[E, Nothing] =
    ZStream(Managed.succeedNow(Pull.halt(cause)))

  /**
   * The infinite stream of iterative function application: a, f(a), f(f(a)), f(f(f(a))), ...
   */
  def iterate[A](a: A)(f: A => A): UStream[A] =
    ZStream {
      Managed.fromEffect {
        Ref.make(a).map { currA =>
          currA.modify(a => Chunk.single(a) -> f(a))
        }
      }
    }

  /**
   * Creates a single-valued stream from a managed resource
   */
  def managed[R, E, A](managed: ZManaged[R, E, A]): ZStream[R, E, A] =
    ZStream {
      for {
        doneRef   <- Ref.make(false).toManaged_
        finalizer <- ZManaged.finalizerRef[R](_ => UIO.unit)
        pull = ZIO.uninterruptibleMask { restore =>
          doneRef.get.flatMap { done =>
            if (done) ZIO.fail(Right(()))
            else
              (for {
                _           <- doneRef.set(true)
                reservation <- managed.reserve
                _           <- finalizer.set(reservation.release)
                a           <- restore(reservation.acquire)
              } yield Chunk(a)).mapError(Left(_))
          }
        }
      } yield pull
    }

  /**
   * The stream that never produces any value or fails with any error.
   */
  val never: UStream[Nothing] =
    ZStream(ZManaged.succeedNow(UIO.never))

  /**
   * Constructs a stream from a range of integers (lower bound included, upper bound not included)
   *
   * @param min the lower bound
   * @param max the upper bound
   */
  def range(min: Int, max: Int): UStream[Int] =
    iterate(min)(_ + 1).takeWhile(_ < max)
}

sealed abstract class ZSink[-R, +E, -I, +Z](
  override val run: ZManaged[R, Nothing, Chunk[I] => ZIO[R, Either[E, Z], Chunk[Unit]]]
) extends ZConduit[R, E, I, Unit, Z](run)

sealed abstract class ZTransducer[-R, +E, -I, +O](
  override val run: ZManaged[R, Nothing, Chunk[I] => ZIO[R, Either[E, Nothing], Chunk[O]]]
) extends ZConduit[R, E, I, O, Nothing](run)
