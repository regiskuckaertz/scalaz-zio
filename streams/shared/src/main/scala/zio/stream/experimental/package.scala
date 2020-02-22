package zio.stream

package object experimental {
  type UStream[A] = ZStream[Any, Nothing, A]
  type Stream[E, A] = ZStream[Any, E, A]
}