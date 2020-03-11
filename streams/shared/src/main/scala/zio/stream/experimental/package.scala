package zio.stream

package object experimental {
  type UStream[A]   = zio.stream.experimental.ZStream[Any, Nothing, A]
  type Stream[E, A] = zio.stream.experimental.ZStream[Any, E, A]
}
