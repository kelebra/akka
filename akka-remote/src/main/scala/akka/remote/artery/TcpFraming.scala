/**
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.nio.ByteBuffer
import java.nio.ByteOrder

import akka.annotation.InternalApi
import akka.remote.artery.FlightRecorderEvents.TcpInbound_Received
import akka.stream.Attributes
import akka.stream.impl.io.ByteStringParser
import akka.stream.impl.io.ByteStringParser.ByteReader
import akka.stream.impl.io.ByteStringParser.ParseResult
import akka.stream.impl.io.ByteStringParser.ParseStep
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.stage.GraphStageLogic
import akka.util.ByteString

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TcpFraming {
  val Undefined = Int.MinValue

  /**
   * Encode the `frameLength` as 4 bytes
   */
  def encodeFrameHeader(frameLength: Int): ByteString =
    ByteString(
      (frameLength & 0xff).toByte,
      ((frameLength & 0xff00) >> 8).toByte,
      ((frameLength & 0xff0000) >> 16).toByte,
      ((frameLength & 0xff000000) >> 24).toByte
    )
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TcpFraming(flightRecorder: EventSink) extends ByteStringParser[EnvelopeBuffer] {
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new ParsingLogic {
    abstract class Step extends ParseStep[EnvelopeBuffer]
    startWith(ReadStreamId)

    case object ReadStreamId extends Step {
      override def parse(reader: ByteReader): ParseResult[EnvelopeBuffer] =
        ParseResult(None, ReadFrame(reader.readByte()))
    }
    case class ReadFrame(streamId: Int) extends Step {
      override def onTruncation(): Unit =
        failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))

      override def parse(reader: ByteReader): ParseResult[EnvelopeBuffer] = {
        val frameLength = reader.readIntLE()
        val buffer = createBuffer(reader.take(frameLength))
        ParseResult(Some(buffer), this)
      }

      private def createBuffer(bs: ByteString): EnvelopeBuffer = {
        val buffer = ByteBuffer.wrap(bs.toArray)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        flightRecorder.hiFreq(TcpInbound_Received, buffer.limit)
        val res = new EnvelopeBuffer(buffer)
        res.setStreamId(streamId)
        res
      }
    }
  }
}
