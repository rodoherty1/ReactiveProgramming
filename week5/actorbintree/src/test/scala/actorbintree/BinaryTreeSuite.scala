/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeSet.OperationFinished
import akka.actor.{ Props, ActorRef, ActorSystem }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec }
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import org.scalatest.Matchers
import scala.util.Random
import scala.concurrent.duration._
import org.scalatest.FunSuiteLike

class BinaryTreeSuite(_system: ActorSystem) extends TestKit(_system) with FunSuiteLike with Matchers with BeforeAndAfterAll with ImplicitSender
{

  def this() = this(ActorSystem("BinaryTreeSuite"))

  override def afterAll: Unit = system.shutdown()

  import actorbintree.BinaryTreeSet._

  def receiveN(requester: TestProbe, ops: Seq[Operation], expectedReplies: Seq[OperationReply]): Unit =
    requester.within(5.seconds) {
      val repliesUnsorted = for (i <- 1 to ops.size) yield try {
        requester.expectMsgType[OperationReply]
      } catch {
        case ex: Throwable if ops.size > 10 => fail(s"failure to receive confirmation $i/${ops.size}", ex)
        case ex: Throwable                  =>         println (ex)
          fail(s"failure to receive confirmation $i/${ops.size}\nRequests:" + ops.mkString("\n    ", "\n     ", ""), ex)
      }
      val replies = repliesUnsorted.sortBy(_.id)
      if (replies != expectedReplies) {
        val pairs = (replies zip expectedReplies).zipWithIndex filter (x => x._1._1 != x._1._2)
        fail("unexpected replies:" + pairs.map(x => s"at index ${x._2}: got ${x._1._1}, expected ${x._1._2}").mkString("\n    ", "\n    ", ""))
      }
    }

  def verify(probe: TestProbe, ops: Seq[Operation], expected: Seq[OperationReply]): Unit = {
    val topNode = system.actorOf(Props[BinaryTreeSet])

    ops foreach { op =>
      topNode ! op
    }

    receiveN(probe, ops, expected)
    // the grader also verifies that enough actors are created
  }



  ignore("proper inserts and lookups") {
    val topNode = system.actorOf(Props[BinaryTreeSet])

    topNode ! Contains(testActor, id = 1, 1)
    expectMsg(ContainsResult(1, false))

    topNode ! Insert(testActor, id = 2, 1)
    topNode ! Contains(testActor, id = 3, 1)

    expectMsg(OperationFinished(2))
    expectMsg(ContainsResult(3, true))
  }


  ignore("Simple GC") {
    val topNode = system.actorOf(Props[BinaryTreeSet])

    topNode ! Insert(testActor, id = 10, 1)
    topNode ! Contains(testActor, id = 20, 1)
    topNode ! GC
    topNode ! Contains(testActor, id = 30, 1)
    topNode ! Insert(testActor, id = 40, 5)
    topNode ! Remove(testActor, id = 50, 5)
    topNode ! GC
    topNode ! Contains(testActor, id = 60, 5)

    expectMsg(OperationFinished(10))
    expectMsg(ContainsResult(20, result = true))
    expectMsg(ContainsResult(30, result = true))
    expectMsg(OperationFinished(40))
    expectMsg(OperationFinished(50))
    expectMsg(ContainsResult(60, result = false))
  }

  test("Properly remove non-leaves") {
    val topNode = system.actorOf(Props[BinaryTreeSet])

    topNode ! Insert(testActor, id = 10, 5)
    topNode ! Insert(testActor, id = 20, 3)
    topNode ! Insert(testActor, id = 30, 10)
    topNode ! Insert(testActor, id = 40, 3)
    topNode ! Insert(testActor, id = 50, 7)
    expectMsg(OperationFinished(10))
    expectMsg(OperationFinished(20))
    expectMsg(OperationFinished(30))
    expectMsg(OperationFinished(40))
    expectMsg(OperationFinished(50))

    topNode ! Contains(testActor, id = 60, 10)
    expectMsg(ContainsResult(60, result = true))

    topNode ! Remove(testActor, id = 70, 10)
    expectMsg(OperationFinished(70))

    topNode ! Contains(testActor, id = 80, 10)
    expectMsg(ContainsResult(80, result = false))
  }



  test("Insert after being removed") {
    val topNode = system.actorOf(Props[BinaryTreeSet])

    topNode ! Insert(testActor, id = 10, 5)
    expectMsg(OperationFinished(10))

    topNode ! Contains(testActor, id = 15, 5)
    expectMsg(ContainsResult(15, result = true))

    topNode ! Remove(testActor, id = 20, 5)
    expectMsg(OperationFinished(20))

    topNode ! Contains(testActor, id = 20, 5)
    expectMsg(ContainsResult(20, result = false))

    topNode ! Insert(testActor, id = 30, 5)
    expectMsg(OperationFinished(30))

    topNode ! Contains(testActor, id = 40, 5)
    expectMsg(ContainsResult(40, result = true))
  }


  test("Testing inserts and contains for a few more nodes") {
    val topNode = system.actorOf(Props[BinaryTreeSet])

    topNode ! Insert(testActor, id = 1, 1)
    expectMsg(OperationFinished(1))

    topNode ! Insert(testActor, id = 2, 5)
    expectMsg(OperationFinished(2))

    topNode ! Insert(testActor, id = 3, 2)
    expectMsg(OperationFinished(3))

    topNode ! Insert(testActor, id = 4, 3)
    expectMsg(OperationFinished(4))

    topNode ! Insert(testActor, id = 5, 1)
    expectMsg(OperationFinished(5))

    topNode ! Contains(testActor, id = 6, 1)
    expectMsg(ContainsResult(6, result = true))

    topNode ! Contains(testActor, id = 7, 2)
    expectMsg(ContainsResult(7, true))

    topNode ! Contains(testActor, id = 8, 3)
    expectMsg(ContainsResult(8, true))

    topNode ! Contains(testActor, id = 9, 4)
    expectMsg(ContainsResult(9, false))

    topNode ! Contains(testActor, id = 10, 5)
    expectMsg(ContainsResult(10, true))

    topNode ! Remove(testActor, id = 11, 1)
    expectMsg(OperationFinished(11))

    topNode ! Contains(testActor, id = 12, 1)
    expectMsg(ContainsResult(12, false))

  }


  test("instruction example") {
    val requester = TestProbe()
    val requesterRef = requester.ref
    val ops = List(
      Insert(requesterRef, id=100, 1),
      Contains(requesterRef, id=50, 2),
      Remove(requesterRef, id=10, 1),
      Insert(requesterRef, id=20, 2),
      Contains(requesterRef, id=80, 1),
      Contains(requesterRef, id=70, 2)
      )

    val expectedReplies = List(
      OperationFinished(id=10),
      OperationFinished(id=20),
      ContainsResult(id=50, result = false),
      ContainsResult(id=70, result = true),
      ContainsResult(id=80, result = false),
      OperationFinished(id=100)
      )

    verify(requester, ops, expectedReplies)
  }

  test("More instruction examples") {
    val requester = TestProbe()
    val requesterRef = requester.ref
    val ops = List(
      Insert(requesterRef, id=100, 3),
      Insert(requesterRef, id=101, 5),
      Insert(requesterRef, id=102, 2),
      Contains(requesterRef, id=104, 5),
      Insert(requesterRef, id=105, 6)
    )

    val expectedReplies = List(
      OperationFinished(id=100),
      OperationFinished(id=101),
      OperationFinished(id=102),
      ContainsResult(id=104, result = true),
      OperationFinished(id=105)
    )

    verify(requester, ops, expectedReplies)
  }





  test("behave identically to built-in set (includes GC)") {
    val rnd = new Random()
    def randomOperations(requester: ActorRef, count: Int): Seq[Operation] = {
      def randomElement: Int = rnd.nextInt(100)
      def randomOperation(requester: ActorRef, id: Int): Operation = rnd.nextInt(4) match {
        case 0 => Insert(requester, id, randomElement)
        case 1 => Insert(requester, id, randomElement)
        case 2 => Contains(requester, id, randomElement)
        case 3 => Remove(requester, id, randomElement)
      }

      for (seq <- 0 until count) yield randomOperation(requester, seq)
    }

    def referenceReplies(operations: Seq[Operation]): Seq[OperationReply] = {
      var referenceSet = Set.empty[Int]
      def replyFor(op: Operation): OperationReply = op match {
        case Insert(_, seq, elem) =>
          referenceSet = referenceSet + elem
          OperationFinished(seq)
        case Remove(_, seq, elem) =>
          referenceSet = referenceSet - elem
          OperationFinished(seq)
        case Contains(_, seq, elem) =>
          ContainsResult(seq, referenceSet(elem))
      }

      for (op <- operations) yield replyFor(op)
    }

    val requester = TestProbe()
    val topNode = system.actorOf(Props[BinaryTreeSet])
    val count = 1000

    val ops = randomOperations(requester.ref, count)
    val expectedReplies = referenceReplies(ops)

    ops foreach { op =>
      topNode ! op
      if (rnd.nextDouble() < 0.1) topNode ! GC
    }
    receiveN(requester, ops, expectedReplies)
  }
}
