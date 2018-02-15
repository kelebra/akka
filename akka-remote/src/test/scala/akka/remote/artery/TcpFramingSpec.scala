/**
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.remote.artery

import scala.util.Random

import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.util.ByteString

class TcpFramingSpec extends AkkaSpec with ImplicitSender {
  import TcpFraming.frameHeader

  private val matSettings = ActorMaterializerSettings(system).withFuzzing(true)
  private implicit val mat = ActorMaterializer(matSettings)(system)

  private val afr = IgnoreEventSink

  private val framingFlow = Flow[ByteString].via(new TcpFraming(afr))

  private val payload5 = ByteString((1 to 5).map(_.toByte).toArray)

  private def frameBytes(numberOfFrames: Int): ByteString =
    (1 to numberOfFrames).foldLeft(ByteString.empty)((acc, _) ⇒ acc ++ frameHeader(payload5.size) ++ payload5)

  private val rndSeed = System.currentTimeMillis()
  private val rnd = new Random(rndSeed)

  private def rechunk(bytes: ByteString): Iterator[ByteString] = {
    var remaining = bytes
    new Iterator[ByteString] {
      override def hasNext: Boolean = remaining.nonEmpty

      override def next(): ByteString = {
        val chunkSize = rnd.nextInt(remaining.size) + 1 // no 0 length frames
        val chunk = remaining.take(chunkSize)
        remaining = remaining.drop(chunkSize)
        chunk
      }
    }
  }

  "TcpFraming stage" must {

    "grab streamId from first frame" in {
      val bytes = ByteString(2.toByte) ++ frameBytes(1)
      val frames = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
      frames.head.streamId should ===(2)
    }

    "include streamId in each frame" in {
      val bytes = ByteString(3.toByte) ++ frameBytes(3)
      val frames = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
      frames(0).streamId should ===(3)
      frames(1).streamId should ===(3)
      frames(2).streamId should ===(3)
    }

    "parse frames from random chunks" in {
      val numberOfFrames = 100
      val bytes = ByteString(3.toByte) ++ frameBytes(numberOfFrames)
      withClue(s"Random chunks seed: $rndSeed") {
        val frames = Source.fromIterator(() ⇒ rechunk(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
        //val frames = Source(List(bytes)).via(framingFlow).runWith(Sink.seq).futureValue
        frames.size should ===(numberOfFrames)
        frames.foreach { frame ⇒
          frame.byteBuffer.limit should ===(payload5.size)
          val payload = new Array[Byte](frame.byteBuffer.limit)
          frame.byteBuffer.get(payload)
          ByteString(payload) should ===(payload5)
          frame.streamId should ===(3)
        }
      }
    }

    "report truncated frames" in {
      val bytes = ByteString(3.toByte) ++ frameBytes(3).drop(1)
      val frames = Source(List(bytes)).via(framingFlow).runWith(Sink.seq)
        .failed.futureValue shouldBe a[FramingException]
    }

    "work with empty stream" in {
      val frames = Source.empty.via(framingFlow).runWith(Sink.seq).futureValue
      frames.size should ===(0)
    }

  }

}
