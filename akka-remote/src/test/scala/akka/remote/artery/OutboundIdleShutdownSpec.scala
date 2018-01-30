/**
 * Copyright (C) 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.RootActorPath
import akka.remote.RARP
import akka.testkit.ImplicitSender
import akka.testkit.TestActors
import akka.testkit.TestProbe
import org.scalatest.concurrent.Eventually

class OutboundIdleShutdownSpec extends ArteryMultiNodeSpec("""
  akka.loglevel=INFO
  akka.remote.artery.advanced.stop-idle-outbound-after = 1 s
  akka.remote.artery.advanced.compression {
    actor-refs.advertisement-interval = 5 seconds
  }
  """) with ImplicitSender with Eventually {

  "Outbound streams" should {

    "eliminate an association when all streams within are idle" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) ⇒

        val association = localArtery.association(remoteAddress)
        withClue("When initiating a connection, both the control - and ordinary streams are opened (regardless of which one was used)") {
          association.isStreamActive(Association.ControlQueueIndex) shouldBe true
          association.isStreamActive(Association.OrdinaryQueueIndex) shouldBe true
        }

        eventually {
          association.isStreamActive(Association.ControlQueueIndex) shouldBe false
          association.isStreamActive(Association.OrdinaryQueueIndex) shouldBe false
        }

      // FIXME: Currently we have a memory leak in that "shallow" associations are kept around even though
      // the outbound streams are inactive
      //eventually { localArtery.remoteAddresses shouldBe 'empty }
    }

    "have individual (in)active cycles" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) ⇒

        val association = localArtery.association(remoteAddress)

        withClue("When the ordinary stream is used and the control stream is not, the former should be active and the latter inactive") {
          eventually {
            remoteEcho.tell("ping", localProbe.ref)
            association.isStreamActive(Association.ControlQueueIndex) shouldBe false
            association.isStreamActive(Association.OrdinaryQueueIndex) shouldBe true
          }
        }

        withClue("When the control stream is used and the ordinary stream is not, the former should be active and the latter inactive") {
          localProbe.watch(remoteEcho)
          eventually {
            association.isStreamActive(Association.ControlQueueIndex) shouldBe true
            association.isStreamActive(Association.OrdinaryQueueIndex) shouldBe false
          }
        }
    }

    "still be resumable after the association has been cleaned" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) ⇒
        val firstAssociation = localArtery.association(remoteAddress)

        eventually {
          firstAssociation.isActive() shouldBe false
          // FIXME
          //localArtery.remoteAddresses shouldBe 'empty
        }

        withClue("re-initiating the connection should be the same as starting it the first time") {

          eventually {
            remoteEcho.tell("ping", localProbe.ref)
            localProbe.expectMsg("ping")
            val secondAssociation = localArtery.association(remoteAddress)
            secondAssociation.isStreamActive(Association.ControlQueueIndex) shouldBe true
            secondAssociation.isStreamActive(Association.OrdinaryQueueIndex) shouldBe true
          }

        }
    }

    "not deactivate if there are unacknowledged system messages" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) ⇒
        val association = localArtery.association(remoteAddress)

        val associationState = association.associationState
        associationState.pendingSystemMessagesCount.incrementAndGet()
        association.isStreamActive(Association.ControlQueueIndex) shouldBe true

        Thread.sleep(3.seconds.toMillis)
        association.isStreamActive(Association.ControlQueueIndex) shouldBe true
    }

    "be dropped after the last outbound system message is acknowledged and the idle period has passed" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) ⇒

        val association = RARP(system).provider.transport.asInstanceOf[ArteryTransport].association(remoteAddress)

        association.associationState.pendingSystemMessagesCount.incrementAndGet()
        Thread.sleep(3.seconds.toMillis)
        association.isStreamActive(Association.ControlQueueIndex) shouldBe true

        association.associationState.pendingSystemMessagesCount.decrementAndGet()
        Thread.sleep(3.seconds.toMillis)
        eventually(association.isStreamActive(Association.ControlQueueIndex) shouldBe false)
    }

    "remove inbound compression after quarantine" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) ⇒

        val association = localArtery.association(remoteAddress)
        val remoteUid = association.associationState.uniqueRemoteAddress.futureValue.uid

        localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid)

        eventually {
          association.isStreamActive(Association.ControlQueueIndex) shouldBe false
          association.isStreamActive(Association.OrdinaryQueueIndex) shouldBe false
        }
        // compression still exists when idle
        localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid)

        localArtery.quarantine(remoteAddress, Some(remoteUid), "Test")
        // after quarantine it should be removed
        eventually {
          localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should not contain (remoteUid)
        }
    }

    "remove inbound compression after restart with same host:port" in withAssociation {
      (remoteSystem, remoteAddress, remoteEcho, localArtery, localProbe) ⇒

        val association = localArtery.association(remoteAddress)
        val remoteUid = association.associationState.uniqueRemoteAddress.futureValue.uid

        localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid)

        shutdown(remoteSystem)

        val remoteSystem2 = newRemoteSystem(Some(s"""
          akka.remote.artery.canonical.hostname = ${remoteAddress.host.get}
          akka.remote.artery.canonical.port = ${remoteAddress.port.get}
          """), name = Some(remoteAddress.system))
        try {

          remoteSystem2.actorOf(TestActors.echoActorProps, "echo2")

          def remoteEcho = system.actorSelection(RootActorPath(remoteAddress) / "user" / "echo2")
          val echoRef = eventually {
            remoteEcho.resolveOne(1.seconds).futureValue
          }

          echoRef.tell("ping2", localProbe.ref)
          localProbe.expectMsg("ping2")

          val association2 = localArtery.association(remoteAddress)
          val remoteUid2 = association2.associationState.uniqueRemoteAddress.futureValue.uid

          remoteUid2 should !==(remoteUid)

          eventually {
            localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid2)
          }
          eventually {
            localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should not contain (remoteUid)
          }
        } finally {
          shutdown(remoteSystem2)
        }
    }

    /**
     * Test setup fixture:
     * 1. A 'remote' ActorSystem is created to spawn an Echo actor,
     * 2. A TestProbe is spawned locally to initiate communication with the Echo actor
     * 3. Details (remoteAddress, remoteEcho, localArtery, localProbe) are supplied to the test
     */
    def withAssociation(test: (ActorSystem, Address, ActorRef, ArteryTransport, TestProbe) ⇒ Any): Unit = {
      val remoteSystem = newRemoteSystem()
      try {
        remoteSystem.actorOf(TestActors.echoActorProps, "echo")
        val remoteAddress = RARP(remoteSystem).provider.getDefaultAddress

        def remoteEcho = system.actorSelection(RootActorPath(remoteAddress) / "user" / "echo")

        val echoRef = remoteEcho.resolveOne(3.seconds).futureValue
        val localProbe = new TestProbe(localSystem)

        echoRef.tell("ping", localProbe.ref)
        localProbe.expectMsg("ping")

        val artery = RARP(system).provider.transport.asInstanceOf[ArteryTransport]

        test(remoteSystem, remoteAddress, echoRef, artery, localProbe)

      } finally {
        shutdown(remoteSystem)
      }
    }
  }
}
