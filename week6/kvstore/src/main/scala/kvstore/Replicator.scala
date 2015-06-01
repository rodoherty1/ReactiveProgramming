package kvstore

import akka.actor._

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case class CancelSnapshotMessage(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {

  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var pendingSnapshotAcks = Map.empty[Long, (ActorRef, Cancellable)]

  var _seqCounter = 0L

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case Replicate(k, v, id) => {
      log.debug(s"Received Replicate $k, $v from ${sender()}")

      val cancellable = context.system.scheduler.schedule(0 seconds, 250 millis, replica, Snapshot(k, v, id))
      pendingSnapshotAcks = pendingSnapshotAcks.updated(id, (sender(), cancellable))
      context.system.scheduler.scheduleOnce(1 second, self, CancelSnapshotMessage(id))
    }

    case SnapshotAck(k, id) =>
      if (pendingSnapshotAcks.contains(id)) {
        val (senderRef, cancellable) = pendingSnapshotAcks(id)
        cancellable.cancel()
        pendingSnapshotAcks = pendingSnapshotAcks - id
        senderRef ! Replicated(k, id)
      } else {
        log.debug("Received SnapshotAck for id={}.  Ack is too late.  Ignoring!", id)
      }

    case CancelSnapshotMessage(id) =>
      if (pendingSnapshotAcks.contains(id)) {
        val (_, cancellable) = pendingSnapshotAcks(id)
        cancellable.cancel()
        pendingSnapshotAcks = pendingSnapshotAcks - id
      } else {
        log.debug("All SnapshotAcks for id={} have been received.  Ignoring CancelSnapshotMessage!")
      }
  }
}
