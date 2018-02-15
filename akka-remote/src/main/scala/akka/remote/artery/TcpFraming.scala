/**
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.annotation.tailrec

import akka.annotation.InternalApi
import akka.stream.Attributes
import akka.util.ByteString
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.StageLogging
import akka.remote.artery.FlightRecorderEvents.TcpInbound_Received
import akka.stream.scaladsl.Framing.FramingException

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

  /**
   * Decode the frame length, from first 4 bytes.
   */
  def decodeFrameHeader(bytes: ByteString): Int =
    (bytes(0) & 0xff) << 0 |
      (bytes(1) & 0xff) << 8 |
      (bytes(2) & 0xff) << 16 |
      (bytes(3) & 0xff) << 24
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TcpFraming(flightRecorder: EventSink) extends GraphStage[FlowShape[ByteString, EnvelopeBuffer]] {
  import TcpFraming._

  val in: Inlet[ByteString] = Inlet("Artery.TcpFraming.in")
  val out: Outlet[EnvelopeBuffer] = Outlet("Artery.TcpFraming.out")

  val shape: FlowShape[ByteString, EnvelopeBuffer] = FlowShape(in, out)

  def createLogic(inheritedAttributes: Attributes) = {
    new GraphStageLogic(shape) with InHandler with OutHandler with StageLogging { self ⇒
      var streamId: Int = Undefined
      var frameLength: Int = Undefined
      var inBuffer: ByteString = ByteString.empty

      override protected def logSource = classOf[TcpFraming]

      // initial handler, retrieving the streamId in the first frame
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val bytes = grab(in)
          streamId = bytes(0) & 0xff
          if (bytes.size == 1)
            tryPull()
          else {
            inBuffer = bytes.drop(1)
            handleBytes()
          }
          setHandler(in, self)
        }
      })

      // after first frame this handler is used
      override def onPush(): Unit = {
        inBuffer ++= grab(in)
        handleBytes()
      }

      override def onPull(): Unit = {
        handleBytes()
      }

      private def tryPull(): Unit = {
        if (isClosed(in)) {
          failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))
        } else pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        if (inBuffer.isEmpty) {
          completeStage()
        } else if (isAvailable(out)) {
          handleBytes()
        } // else swallow the termination and wait for pull
      }

      @tailrec
      private def handleBytes(): Unit = {
        if (frameLength == Undefined) {
          if (inBuffer.size >= 4) {
            frameLength = decodeFrameHeader(inBuffer)
            if (frameLength <= 0) // we don't need zero length frames, there will always be a header
              failStage(new FramingException(s"Frame header reported zero or negative size $frameLength"))
            else {
              inBuffer = inBuffer.drop(4)
              handleBytes()
            }
          } else tryPull() // wait for more frameLength bytes
        } else {
          if (inBuffer.size >= frameLength) {
            pushFrame()
          } else tryPull() // wait for more frame bytes
        }
      }

      private def pushFrame(): Unit = {
        val outFrame = createBuffer(inBuffer.take(frameLength))
        inBuffer = inBuffer.drop(frameLength)
        frameLength = Undefined
        push(out, outFrame)
        if (inBuffer.isEmpty && isClosed(in))
          completeStage()
      }

      private def createBuffer(bs: ByteString): EnvelopeBuffer = {
        val buffer = ByteBuffer.wrap(bs.toArray)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        flightRecorder.hiFreq(TcpInbound_Received, buffer.limit)
        val res = new EnvelopeBuffer(buffer)
        res.setStreamId(streamId)
        res
      }

      setHandler(out, this)
    }
  }

}
