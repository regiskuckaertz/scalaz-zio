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
   * Runs the stream and collects all of its elements in a list.
   *
   * Equivalent to `run(Sink.collectAll[A])`.
   *
   * @return an action that yields the list of elements in the stream
   */
  final def runCollect: ZIO[R, E, List[O]] = run(ZSink.collectAll[O])

  /**
   * Runs the stream and collects all of its elements in a list.
   *
   * Equivalent to `run(Sink.collectAll[A])`.
   *
   * @return an action that yields the list of elements in the stream
   */
  final def runCollectManaged: ZManaged[R, E, List[O]] = runManaged(ZSink.collectAll[O])

  /**
   * Runs the stream purely for its effects. Any elements emitted by
   * the stream are discarded.
   *
   * @return an action that drains the stream
   */
  final def runDrain: ZIO[R, E, Unit] = runDrainManaged.use(UIO.succeedNow)

  /**
   * Runs the stream purely for its effects. Any elements emitted by
   * the stream are discarded.
   *
   * @return a managed effect that drains the stream
   */
  final def runDrainManaged: ZManaged[R, E, Unit] = runManaged(ZSink.drain)

  /**
   * Runs the sink on the stream to produce either the sink's internal state or an error.
   *
   * @tparam B the internal state of the sink
   * @param sink the sink to run against
   * @return the internal state of the sink after exhausting the stream
   */
  def run[R1 <: R, E1 >: E, O1 >: O, B](sink: ZSink[R1, E1, O1, B]): ZIO[R1, E1, B] =
    runManaged(sink).use(UIO.succeedNow)

  /**
   * Runs the sink on the stream to produce either the sink's internal state or an error.
   *
   * @tparam B the internal state of the sink
   * @param sink the sink to run against
   * @return the internal state of the sink after exhausting the stream wrapped in a managed resource
   */
  def runManaged[R1 <: R, E1 >: E, O1 >: O, B](sink: ZSink[R1, E1, O1, B]): ZManaged[R1, E1, B] =
    for {
      pull <- self.process
      push <- sink.run
      run = {
        def go: ZIO[R1, E1, B] =
          pull.foldM(
            _.fold(
              ZIO.failNow,
              _ =>
                push(Chunk.empty).foldM(_.fold(IO.failNow, IO.succeedNow), _ => IO.dieMessage("This is not possible"))
            ),
            push(_).catchAll(_.fold(IO.failNow, _ => IO.dieMessage("This is not possible"))) *> go
          )
        go
      }
      b <- run.toManaged_
    } yield b

  /**
   * Takes the specified number of elements from this stream.
   * @param n the number of elements to retain
   * @return a stream with `n` or less elements
   */
  def take(n: Long): ZStream[R, E, O] =
    ZStream {
      for {
        as      <- self.process
        counter <- Ref.make(0L).toManaged_
        pull = counter.get.flatMap { c =>
          if (c >= n) Pull.end
          else as <* counter.set(c + 1)
        }
      } yield pull
    }

  /**
   * Takes all elements of the stream for as long as the specified predicate
   * evaluates to `true`.
   * @param pred Predicate function on the values produced by the stream
   * @return a stream of values agreeing with the predicate
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

  private[zio] def succeedNow[A](a: A): UStream[A] =
    ZStream(
      Managed.fromEffect(
        Ref.make(false).map(done => done.modify(if (_) Pull.end -> true else Pull.emit(a) -> true).flatten)
      )
    )
}

sealed abstract class ZSink[-R, +E, -I, +Z](
  override val run: ZManaged[R, Nothing, Chunk[I] => ZIO[R, Either[E, Z], Chunk[Unit]]]
) extends ZConduit[R, E, I, Unit, Z](run)

object ZSink {
  object Push {
    def emit[A](a: A): IO[Either[Nothing, A], Nothing] = IO.fail(Right(a))
    val next: UIO[Chunk[Unit]]                         = UIO(Chunk.unit)
  }

  def apply[R, E, I, Z](run: ZManaged[R, Nothing, Chunk[I] => ZIO[R, Either[E, Z], Chunk[Unit]]]): ZSink[R, E, I, Z] =
    new ZSink(run) {}

  def collectAll[A]: ZSink[Any, Nothing, A, List[A]] =
    ZSink {
      Managed.fromEffect {
        Ref.make[Either[List[A], Chunk[A]]](Right(Chunk.empty)).map { as => is =>
          as.modify {
            case l @ Left(as) => Push.emit(as) -> l
            case Right(as) =>
              is match {
                case Chunk.empty => val asl = as.toList; Push.emit(asl) -> Left(asl)
                case _           => Push.next -> Right(as ++ is)
              }
          }.flatten
        }
      }
    }

  val drain: ZSink[Any, Nothing, Any, Nothing] =
    ZSink(Managed.succeedNow(_ => UIO(Chunk.unit)))
}

sealed abstract class ZTransducer[-R, +E, -I, +O](
  override val run: ZManaged[R, Nothing, Chunk[I] => ZIO[R, Either[E, Nothing], Chunk[O]]]
) extends ZConduit[R, E, I, O, Nothing](run)
