package zio.stream.experimental

import zio._
import zio.test._

object ZStreamSpec extends ZIOBaseSpec {
  def spec = suite("ZStreamSpec")(
    suite("Combinators")(
      suite("takeWhile")(
         testM("happy path")(checkM(streamOfBytes, Gen.function(Gen.boolean)) { (s, p) =>
           for {
             streamTakeWhile <- s.takeWhile(p).runCollect.run
             listTakeWhile   <- s.runCollect.map(_.takeWhile(p)).run
           } yield assert(listTakeWhile.succeeded)(isTrue) implies assert(streamTakeWhile)(equalTo(listTakeWhile))
         }),
         testM("short circuits")(
           assertM(
             (ZStream.succeedNow(1) ++ Stream.failNow("Ouch"))
               .takeWhile(_ => false)
               .runDrain
               .either
           )(isRight(isUnit))
         )
       )
    ),
    suite("Constructors")(
      testM("iterate")(
         assertM(ZStream.iterate(1)(_ + 1).take(10).runCollect)(equalTo((1 to 10).toList))
       ),
       testM("range") {
         assertM(ZStream.range(0, 10).runCollect)(equalTo(Range(0, 10).toList))
       }
    )
  )
}
