package kvstore

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import kvstore.Arbiter._
import kvstore.Persistence.{Persisted, Persist}

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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends PersistentActor with AtLeastOnceDelivery with ActorLogging {

  import Replica._
  import Replicator._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var expectedId = 0L

  arbiter ! Join

//  override def persistenceId: String = self.path.toStringWithoutAddress

  def DeliverSnapshotAck(k: String, id: Long, l1: Long) = SnapshotAck(k, id)

  def receiveCommand = {
    case JoinedSecondary =>
      context.become(replica)
    case JoinedPrimary =>
      context.become(leader)
  }

  val leader: Receive = {
    case Insert(k, v, id) => kv = kv updated(k, v); sender() ! OperationAck(id)
    case Remove(k, id) => kv = kv - k; sender() ! OperationAck(id)
    case Get(k, id) => sender() ! GetResult(k, kv.get(k), id)
  }


  def updateState(e: Persist): Unit = {
    e.valueOption match {
      case Some(value) => kv = kv.updated(e.key, value)
      case None => kv = kv - e.key
    }
    expectedId += 1
  }


  val replica: Receive = {
    case Get(k, id) =>
      sender() ! GetResult(k, kv.get(k), id)

    case Snapshot(k, v, id) =>
      if (id == expectedId) {
        persist(Persist(k, v, id))(updateState)
        deliver(sender().path, DeliverSnapshotAck(k, id, _))
//          { e =>
//          v match {
//            case Some(value) => kv = kv.updated(k, value)
//            case None => kv = kv - k
//          }
//          expectedId += 1
//          deliver(sender().path, DeliverSnapshotAck(k, id, _))
//        }
      } else if (id < expectedId) {
        log.info("id < expectedId")
        deliver(sender().path, DeliverSnapshotAck(k, id, _))
      } else {
        ()
      }

    case Persisted (k, id) => confirmDelivery(id)

    case x => log.info("Unhandled message {}", x)
  }

  override def receiveRecover: Receive = {
    case Persist(k, v, id) => deliver(sender().path, DeliverSnapshotAck(k, id, _))
    case Persisted(k, deliveryId) => confirmDelivery(deliveryId)
    case _ => ()
  }
}

