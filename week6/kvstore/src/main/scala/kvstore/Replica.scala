package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persisted, Persist}
import scala.concurrent.duration._


object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  sealed trait Event
  case class ValueUpdated(key: String, value: Option[String]) extends Event
  case class ValueInserted(key: String, value: String) extends Event
  case class ValueRemoved(key: String) extends Event

  case class SnapshotAckDelivered(deliveryId: Long)

  case class PersistFailed(id: Long)
  case class CancelReplication(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {

  case class IdGenerator(id: Int)

  import Replica._
  import Replicator._
  import context.dispatcher
  import scala.language.postfixOps

  val persister = context.actorOf(persistenceProps)
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  var pendingPersists = Map.empty[Long, (ActorRef, Persist, Cancellable)]
  var pendingReplications = Map.empty[Long, Map[ActorRef, (ActorRef, Replicate, Cancellable)]]
  var events = List.empty[Persist]

  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedId = 0L

  arbiter ! Join

  def DeliverSnapshotAck(k: String, id: Long, l1: Long) = SnapshotAck(k, id)

  def receive = {
    case JoinedSecondary =>
      context.become(replica)
    case JoinedPrimary =>
      context.become(leader)
  }

  def persist(k: String, v: Option[String], id: Long) = {
    val persistEvent  = Persist(k, v, id)
    updateState(persistEvent)
    val cancellable = context.system.scheduler.schedule(0 millis, 100 millis, persister, persistEvent)
    pendingPersists = pendingPersists.updated(id, (sender(), persistEvent, cancellable))

    context.system.scheduler.scheduleOnce(1 second, self, PersistFailed(id))
  }

  var idGen = IdGenerator(0)

  def nextId(): Int = {
    idGen = IdGenerator(idGen.id + 1)
    idGen.id
  }

  def setupNewSecondaries(newSecondaries: Set[ActorRef]): Map[ActorRef, ActorRef] = {
    newSecondaries.foldLeft(Map.empty[ActorRef, ActorRef])((b, a) => {
      if (secondaries.contains(a)) {
        b.updated(a, secondaries(a))
      } else {
        val replicator = context.actorOf(Props(new Replicator(a)), name = "Replicator" + nextId)
        log.debug(s"Sending ${events.size} events to $replicator")
        events.reverse.foreach(e => replicator ! Replicate(e.key, e.valueOption, e.id))
        b.updated(a, replicator)
      }
    })
  }

  def retireOldSecondaries(newSecondaries: Map[ActorRef, ActorRef]): Unit = {
    secondaries.keys.foreach(secondary => {
      if (!newSecondaries.contains(secondary)) {
        val replica = secondaries(secondary)
        context.stop(replica)
      }
    })
  }

  def replicate(k: String, v: Option[String], id: Long): Unit = {
    /*
    Here I am sending out Replicate messages to each replicator.  I need to send this message every 100ms until I receive
    a Replicated message, at which point, I cancel the schedule.
    I need to track each one and if it fails, send an OperationFailed message to the original sender.
     */
    val replicateMsg = Replicate(k, v, id)
    val pendingReplicationsForOperation = secondaries.values.foldLeft(Map.empty[ActorRef, (ActorRef, Replicate, Cancellable)])((b, replicator) => {
      val cancellable = context.system.scheduler.schedule(0 millis, 100 millis, replicator, replicateMsg)
      Map(replicator -> (sender(), replicateMsg, cancellable))
    })

    pendingReplications = pendingReplications.updated(id, pendingReplicationsForOperation)

    context.system.scheduler.scheduleOnce(1 second, self, CancelReplication(id))
  }

  val leader: Receive = {
    case Insert(k, v, id) =>
      persist(k, Some(v), id)
      replicate(k, Some(v), id)
    case Remove(k, id) =>
      persist(k, None, id)
      replicate(k, None, id)

    case Get(k, id) => sender() ! GetResult(k, kv.get(k), id)

    // Create Replicator for each Replica.
    // Then forward all events to the Replica.
    // See "The Replication Protocol" in the assignment.
    case Replicas(replicas) =>
      val newSecondaries = setupNewSecondaries(replicas)
      retireOldSecondaries(newSecondaries)
      secondaries = newSecondaries

    case Persisted (k, id) => cancelPersists(id)(OperationAck(id))

    case PersistFailed(id) =>
      if (pendingPersists.contains(id)) {
        val (_, persistEvent, _) = pendingPersists(id)
        cancelPersists(id)(OperationFailed(id))
        updateState(Persist(persistEvent.key, None, id))
        pendingPersists = pendingPersists - id
      }

    case Replicated(k, id) =>

    case CancelReplication(id) =>
      val pendingReplicationsForOperation = pendingReplications(id)

      pendingReplicationsForOperation.keys.foreach(replicator => {
        val (senderRef, _, cancellable) = pendingReplicationsForOperation(replicator)
        cancellable.cancel()
        senderRef ! OperationFailed(id)
      })

      pendingReplications = pendingReplications - id
  }


  def updateState(e: Persist): Unit = {
    events = e :: events

    e.valueOption match {
      case Some(value) => kv = kv.updated(e.key, value)
      case None => kv = kv - e.key
    }
    expectedId += 1
  }

  def cancelPersists(id: Long)(ack: Any): Unit = {
    val (senderRef, persist, cancellable) = pendingPersists(id)
    pendingPersists = pendingPersists - id
    cancellable.cancel()
    senderRef ! ack
  }


  val replica: Receive = {
    case Get(k, id) =>
      sender() ! GetResult(k, kv.get(k), id)

    case Snapshot(k, v, id) =>
      if (id == expectedId) {
        persist(k, v, id)
        //sender() ! SnapshotAck(k, id)
      } else if (id < expectedId) {
        sender() ! SnapshotAck(k, id)
      } else {
        ()
      }

    case Persisted (k, id) => cancelPersists(id)(SnapshotAck(k, id))

    case x => log.info("Unhandled message {}", x)
  }

//  override def receiveRecover: Receive = {
//    case Persist(k, v, id) =>
//      pendingPersists = pendingPersists.updated(id, Persist(k, v, id))
//      sender() ! SnapshotAck(k, id)
//    case Persisted(_, deliveryId) => updateState(pendingPersists(deliveryId))
//    case _ => ()
//  }
}

