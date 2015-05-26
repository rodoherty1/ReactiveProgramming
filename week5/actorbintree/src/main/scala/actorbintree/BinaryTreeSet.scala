/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._

import scala.collection.immutable.{Map, Queue}

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {

  import BinaryTreeNode._
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(props(0, initiallyRemoved = true))

  var root = createRoot

  var newRoot: Option[ActorRef] = None

  log.info("Created root {}", root)

  // optional
  var pendingQueue = Queue.empty[Any]

  // optional
  var pendingResults = Map[Int, Option[ActorRef]]()

  // optional
  def receive = normal

  // optional
  def processPendingQueue(): Unit= {
    def loop(): Unit = {
      if (pendingQueue.nonEmpty) {
        val (op, newPendingQueue) = pendingQueue.dequeue
        op match {
          case Insert(requester, opId, elem) =>
            pendingQueue = newPendingQueue
            pendingResults = pendingResults.updated(opId, Some(requester))
            root ! Insert(self, opId, elem)
          case Remove(requester, opId, elem) =>
            pendingQueue = newPendingQueue
            pendingResults = pendingResults.updated(opId, Some(requester))
            root ! Remove(self, opId, elem)
          case Contains(requester, opId, elem) =>
            pendingQueue = newPendingQueue
            pendingResults = pendingResults.updated(opId, Some(requester))
            root ! Contains(self, opId, elem)
          case GC =>
            log.info("Starting enqueued GC")
            pendingQueue = newPendingQueue
            pendingResults = pendingResults.updated(-1, None)
            newRoot = Some(createRoot)
            gcInProgress = true
            root ! CopyTo(newRoot.get)

        }
      }
    }
    loop()
  }

  def enqueueMessage(operation: Any) = {
    log.debug("Enqueuing {}", operation)
    pendingQueue = pendingQueue.enqueue(operation)
  }

  var gcInProgress: Boolean = false

  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requester, opId, elem) =>
      if (pendingResults.isEmpty) {
        pendingResults = pendingResults.updated(opId, Some(requester))
        root ! Insert(self, opId, elem)
      } else {
        enqueueMessage(Insert(requester, opId, elem))
      }
    case Remove(requester, opId, elem) =>
      if (pendingResults.isEmpty) {
        pendingResults = pendingResults.updated(opId, Some(requester))
        root ! Remove(self, opId, elem)
      } else {
        enqueueMessage(Remove(requester, opId, elem))
      }
    case Contains(requester, opId, elem) =>
      if (pendingResults.isEmpty) {
        pendingResults = pendingResults.updated(opId, Some(requester))
        root ! Contains(self, opId, elem)
      } else {
        enqueueMessage(Contains(requester, opId, elem))
      }
    case ContainsResult(opId, result) =>
      pendingResults(opId) match {
        case Some(requester) =>
          requester ! ContainsResult(opId, result)
          pendingResults -= opId
          processPendingQueue()
        case None => log.warning("No requester found for opId {}", opId)
      }
    case OperationFinished(opId) =>
      pendingResults(opId) match {
        case Some(requester) =>
          requester ! OperationFinished(opId)
          pendingResults -= opId
          processPendingQueue()
        case None => log.warning("No requester found for opId {}", opId)
      }
    case GC =>
      if (!gcInProgress) {
        if (pendingResults.isEmpty) {
          log.info("Starting GC")
          pendingResults = pendingResults.updated(key = -1, None)
          newRoot = Some(createRoot)
          gcInProgress = true
          root ! CopyTo(newRoot.get)
        } else {
          enqueueMessage(GC)
        }
      }

    case CopyFinished =>
      log.info("GC Finished")
      pendingResults -= -1
      root ! PoisonPill
      root = newRoot.get
      newRoot = None
      gcInProgress = false
      processPendingQueue()
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def insert(pos: Position, requester: ActorRef, opId: Integer, newElem: Int): Unit = {
    if (subtrees.contains(pos)) {
      subtrees(pos) ! Insert(requester, opId, newElem)
    } else {
      log.info("Inserting {} -> {}", pos, newElem)
      val newNode = pos -> context.actorOf(props(newElem, initiallyRemoved = false))
      subtrees += newNode
      requester ! OperationFinished(opId)
    }
  }

  def remove(pos: Position, requester: ActorRef, opId: Integer, newElem: Int): Unit = {
    if (subtrees.contains(pos)) {
      log.debug("Telling {} node to remove element {}", pos, newElem)
      subtrees(pos) ! Remove(requester, opId, newElem)
    } else {
      log.debug("Skipping removal of {} because it is not in the tree", newElem)
      requester ! OperationFinished(opId)
    }
  }


  def subtreesContains(requester: ActorRef, opId: Integer, value: Int): Unit = {
    if (subtrees.isEmpty) {
      log.info("Responding with ContainsResult(opId: {} for value: {}, isContained: false)", opId, value)
      requester ! ContainsResult(opId, result = false)
    } else {
      val containsRequesters = List.fill(subtrees.size)(requester)
      pendingContainsResults = pendingContainsResults.updated(opId, containsRequesters)

      if (subtrees.contains(Left)) {
        log.debug("Checking Left subtree for {}", value)
        subtrees(Left) ! Contains(self, opId, value)
      }
      if (subtrees.contains(Right)) {
        log.debug("Checking Right subtree for {}", value)
        subtrees(Right) ! Contains(self, opId, value)
      }
    }
  }

  var pendingContainsResults = Map[Int, List[ActorRef]]()

  var pendingGCResults: Int = 0


  def copyToSubtrees(root: ActorRef): Unit = {
    if (subtrees.contains(Left)) {
      pendingGCResults += 1
      subtrees(Left) ! CopyTo(root)
    }
    if (subtrees.contains(Right)) {
      pendingGCResults += 1
      subtrees(Right) ! CopyTo(root)
    }
  }

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) =>
      if (newElem < elem) {
        insert(Left, requester, id, newElem)
      } else if (newElem > elem) {
        insert(Right, requester, id, newElem)
      } else {
        log.debug("Tree already contains {}.  Skipping insert.", newElem)
        removed = false
        requester ! OperationFinished(id)
      }

    case Contains(requester, id, value) =>
      if (value == elem) {
        log.info("Responding with ContainsResult(opId: {}, value: {}, isContained {})", id, value, !removed)
        requester ! ContainsResult(id, !removed)
      } else {
        subtreesContains(requester, id, value)
      }

    case Remove(requester, id, value) =>
      log.debug("Received Remove opId: {}, value: {}", id, value)
      if (value < elem) {
        remove(Left, requester, id, value)
      } else if (value > elem) {
        remove(Right, requester, id, value)
      } else {
        log.info("Marking value {} as removed", value)
        removed = true
        requester ! OperationFinished(id)
      }

    case ContainsResult(opId, isContained) =>
      if (pendingContainsResults.contains(opId)) {
        val requesters = pendingContainsResults(opId)
        pendingContainsResults -= opId

        requesters match {
          case Nil => log.warning(s"pendingContainsResults should not be empty")
          case h :: Nil =>
            if (isContained) {
              log.info("Child contains element.  Sending ContainsResult opId: {}, result: {}", opId, true)
              h ! ContainsResult(opId, result = true)
            } else {
              log.info("Child does not contain element.  Sending ContainsResult opId: {}, result: {}", opId, false)
              h ! ContainsResult(opId, result = false)
            }

          case h :: t =>
            if (isContained) {
              log.info("Child contains element.  Sending ContainsResult opId: {}, result: {}", opId, true)
              h ! ContainsResult(opId, result = true)
            } else {
              log.debug("Received ContainsResult false from a child.  Awaiting another response")
              pendingContainsResults = pendingContainsResults.updated(opId, t)
            }
        }
      }

    case CopyTo(root) =>
      if (!removed) {
        log.info("Copy {} to new tree", elem)
        root ! Insert(self, 0, elem)
      }
      if (subtrees.nonEmpty) copyToSubtrees(root)
      else sender() ! CopyFinished

    case CopyFinished =>
      pendingGCResults -= 1

      if (pendingGCResults == 0) {
        context.parent ! CopyFinished
        context.stop(self)
      }

    case PoisonPill =>
      context.children.foreach(_ ! PoisonPill)
  }


  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
