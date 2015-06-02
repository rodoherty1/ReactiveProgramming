package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}

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
  case class AcknowledgeEvent(id: Long, msg: Any)

  case object RetryPersistence

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {

  case class IdGenerator(id: Int)

  import Replica._
  import Replicator._
  import context.dispatcher

  import scala.language.postfixOps


//  def refreshPersister: ActorRef = {
//    context.unwatch(persister)
//    newPersister
//  }

  // def newPersister: ActorRef = context.watch(context.actorOf(persistenceProps))

  var persister = context.watch(context.actorOf(persistenceProps))

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = true) {
    case _:Exception =>
      self ! RetryPersistence
      SupervisorStrategy.restart
  }

  var kv = Map.empty[String, String]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  var pendingPersists = Map.empty[Long, (Persist, Cancellable)]
  var pendingReplications = Map.empty[Long, Map[ActorRef, Cancellable]]
  var events = List.empty[Persist]

  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var unacknowledgedEvents = Map.empty[Long, ActorRef]


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
    pendingPersists = pendingPersists.updated(id, (persistEvent, cancellable))

    context.system.scheduler.scheduleOnce(1 second, self, PersistFailed(id))
  }

  var idGen = IdGenerator(0)

  def nextId(): Int = {
    idGen = IdGenerator(idGen.id + 1)
    idGen.id
  }

  def setupNewSecondaries(newSecondaries: Set[ActorRef]): Map[ActorRef, ActorRef] = {
    newSecondaries.foldLeft(Map.empty[ActorRef, ActorRef])((b, a) => {
      if (a.path.equals(self.path)) {
        b
      } else if (secondaries.contains(a)) {
        b.updated(a, secondaries(a))
      } else {
        val replicator = context.actorOf(Props(new Replicator(a)), name = "Replicator" + nextId)
        log.debug(s"Sending ${events.size} events to $replicator")
        events.reverse.foreach(e => replicator ! Replicate(e.key, e.valueOption, e.id))
        b.updated(a, replicator)
      }
    })
  }

  def removeReplicatorFromPendingReplications(replicator: ActorRef) = {
    pendingReplications.keys.foreach(id => {
      val replicatorToCancellable = pendingReplications(id)
      if (replicatorToCancellable.contains(replicator)) {
        val cancellable = replicatorToCancellable(replicator)
        cancellable.cancel()
        if (replicatorToCancellable.size == 1) {
          self ! AcknowledgeEvent(id, OperationAck(id))
        } else {
          pendingReplications = pendingReplications updated (id, replicatorToCancellable - replicator)
        }
      }
    })
  }

  def retireOldSecondaries(newSecondaries: Map[ActorRef, ActorRef]): Unit = {
    secondaries.keys.foreach(secondary => {
      if (!newSecondaries.contains(secondary)) {
        val replicator = secondaries(secondary)
        context.stop(replicator)
        removeReplicatorFromPendingReplications(replicator)
      }
    })

  }

  def replicate(k: String, v: Option[String], id: Long): Unit = {
    if (secondaries.nonEmpty) {
      val replicateMsg = Replicate(k, v, id)
      val pendingReplicationsForOperation = secondaries.values.foldLeft(Map.empty[ActorRef, Cancellable])((b, replicator) => {
        val cancellable = context.system.scheduler.schedule(0 millis, 100 millis, replicator, replicateMsg)
        b.updated(replicator, cancellable)
      })

      pendingReplications = pendingReplications.updated(id, pendingReplicationsForOperation)

      context.system.scheduler.scheduleOnce(1 second, self, CancelReplication(id))
    }
  }

  def resendOutstandingPersistMessages(): Unit = {
    pendingPersists = pendingPersists.map(kv => {
      val (persistEvent, cancellable) = kv._2
      cancellable.cancel()
      val newCancellable = context.system.scheduler.schedule(0 millis, 100 millis, persister, persistEvent)
      (kv._1, (persistEvent, newCancellable))
    })
  }

  val leader: Receive = {
    case Insert(k, v, id) =>
      unacknowledgedEvents = unacknowledgedEvents.updated(id, sender())
      persist(k, Some(v), id)
      replicate(k, Some(v), id)

    case Remove(k, id) =>
      unacknowledgedEvents = unacknowledgedEvents.updated(id, sender())
      persist(k, None, id)
      replicate(k, None, id)

    case Get(k, id) => sender() ! GetResult(k, kv.get(k), id)

    case Replicas(replicas) =>
      val newSecondaries = setupNewSecondaries(replicas)
      retireOldSecondaries(newSecondaries)
      secondaries = newSecondaries

    case Persisted (k, id) =>
      cancelPersists(id)
      if (!pendingReplications.contains(id) || secondaries.isEmpty) {
        self ! AcknowledgeEvent(id, OperationAck(id))
      }

    case PersistFailed(id) =>
      if (pendingPersists.contains(id)) {
        val (persistEvent, _) = pendingPersists(id)
        cancelPersists(id)
        self ! AcknowledgeEvent(id, OperationFailed(id))

        updateState(Persist(persistEvent.key, None, id))
        pendingPersists = pendingPersists - id
      }

    case Replicated(k, id) =>
      if (pendingReplications.contains(id)) {
        val pendingReplicationsForOperation = pendingReplications(id)

        val replicator = sender()

        if (pendingReplicationsForOperation.contains(replicator)) {
          pendingReplicationsForOperation(replicator).cancel()
          pendingReplications = pendingReplications.updated(id, pendingReplicationsForOperation - replicator)
        }

        if (pendingReplicationsForOperation.size == 1) {
          log.debug("Received all acknowledgments of replication")
          if (pendingPersists.contains(id)) {
            log.debug("Withhold OperationAck for id={} until Persist has been acknowledged", id)
          } else {
            self ! AcknowledgeEvent(id, OperationAck(id))
          }
        } else {
          log.debug("Awaiting {} more acknowledgments of replication", pendingReplicationsForOperation.size)
        }
      } else {
        log.debug ("Received acknowledgment of replication for id={}.  Ack is too late.  Ignoring!", id)
      }

    case CancelReplication(id) =>
      val pendingReplicationsForOperation = pendingReplications(id)

      pendingReplicationsForOperation.keys.foreach(replicator => {
        pendingReplicationsForOperation(replicator).cancel()
      })

      self ! AcknowledgeEvent(id, OperationFailed(id))

      log.debug("Not all Acknowlegdments of Replication arrived.  Removing id={} from pendingReplications", id)
      pendingReplications = pendingReplications - id

    case AcknowledgeEvent(id, msg) =>
      if (unacknowledgedEvents.contains(id)) {
        unacknowledgedEvents(id) ! msg
        unacknowledgedEvents = unacknowledgedEvents - id
      }

    case RetryPersistence =>
      log.info("Retrying persistence")
      resendOutstandingPersistMessages()
  }


  def updateState(e: Persist): Unit = {
    events = e :: events

    e.valueOption match {
      case Some(value) => kv = kv.updated(e.key, value)
      case None => kv = kv - e.key
    }
    expectedId += 1
  }

  def cancelPersists(id: Long): Unit = {
    val (persist, cancellable) = pendingPersists(id)
    pendingPersists = pendingPersists - id
    cancellable.cancel()
  }


  val replica: Receive = {
    case Get(k, id) =>
      sender() ! GetResult(k, kv.get(k), id)

    case Snapshot(k, v, id) =>
      if (id == expectedId) {
        persist(k, v, id)
        unacknowledgedEvents = unacknowledgedEvents.updated(id, sender())
      } else if (id < expectedId) {
        sender() ! SnapshotAck(k, id)
      } else {
        ()
      }

    case Persisted (k, id) =>
      cancelPersists(id)
      self ! AcknowledgeEvent(id, SnapshotAck(k, id))

    case PersistFailed(id) => // Ignoring

    case AcknowledgeEvent(id, msg) =>
      if (unacknowledgedEvents.contains(id)) {
        unacknowledgedEvents(id) ! msg
        unacknowledgedEvents = unacknowledgedEvents - id
      }

    case RetryPersistence =>
      log.info("Retrying persistence")
      resendOutstandingPersistMessages()

      
    case Terminated(deadActorRef) =>
      log.info("Received Terminated message: {}", deadActorRef.path)
      if (deadActorRef.path.equals(persister.path)) {
        log.info("Persister has died")

//        persister = refreshPersister
        resendOutstandingPersistMessages()
      }

    case x => log.info("Unhandled message {}", x)
  }
}

