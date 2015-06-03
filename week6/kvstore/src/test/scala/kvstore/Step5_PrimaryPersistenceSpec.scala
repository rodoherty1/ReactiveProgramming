package kvstore

import akka.actor.{Props, Actor, ActorLogging, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kvstore.Arbiter._
import kvstore.Persistence._
import kvstore.Replicator._
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.duration._

class Step5_PrimaryPersistenceSpec extends TestKit(ActorSystem("Step5PrimaryPersistenceSpec"))
with FunSuiteLike
with BeforeAndAfterAll
with Matchers
with ConversionCheckedTripleEquals
with ImplicitSender
with Tools {

  override def afterAll(): Unit = {
    system.shutdown()
  }

  test("case1: Primary does not acknowledge updates which have not been persisted") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "case1-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val setId = client.set("foo", "bar")
    val persistId = persistence.expectMsgPF() {
      case Persist("foo", Some("bar"), id) => id
    }

    client.nothingHappens(100.milliseconds)
    persistence.reply(Persisted("foo", persistId))
    client.waitAck(setId)
  }

  test("case2: Primary retries persistence every 100 milliseconds") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "case2-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val setId = client.set("foo", "bar")
    val persistId = persistence.expectMsgPF() {
      case Persist("foo", Some("bar"), id) => id
    }
    // Retries form above
    persistence.expectMsg(200.milliseconds, Persist("foo", Some("bar"), persistId))
    persistence.expectMsg(200.milliseconds, Persist("foo", Some("bar"), persistId))

    client.nothingHappens(100.milliseconds)
    persistence.reply(Persisted("foo", persistId))
    client.waitAck(setId)
  }

  test("case3: Primary generates failure after 1 second if persistence fails") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "case3-primary")
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val setId = client.set("foo", "bar")
    persistence.expectMsgType[Persist]
    client.nothingHappens(800.milliseconds) // Should not fail too early
    client.waitFailed(setId)
  }

  test("case4: Primary generates failure after 1 second if global acknowledgement fails") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case4-primary")
    val secondary = TestProbe()
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    client.probe.within(1.second, 2.seconds) {
      val setId = client.set("foo", "bar")
      secondary.expectMsgType[Snapshot](200.millis)
      client.waitFailed(setId)
    }
  }

  test("case5: Primary acknowledges only after persistence and global acknowledgement") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case5-primary")
    val secondaryA, secondaryB = TestProbe()
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondaryA.ref, secondaryB.ref)))

    val setId = client.set("foo", "bar")
    val seqA = secondaryA.expectMsgType[Snapshot].seq
    val seqB = secondaryB.expectMsgType[Snapshot].seq
    client.nothingHappens(300.milliseconds)
    secondaryA.reply(SnapshotAck("foo", seqA))
    client.nothingHappens(300.milliseconds)
    secondaryB.reply(SnapshotAck("foo", seqB))
    client.waitAck(setId)
  }

  test("case6: Primary generates failure after 1 second if global acknowledgement fails with flaky persistence") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case6-primary")
    val secondary = TestProbe()
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    client.probe.within(1.second, 2.seconds) {
      val setId = client.set("foo", "bar")
      secondary.expectMsgType[Snapshot](200.millis)
      client.waitFailed(setId)
    }
  }

  test("case7: Primary acknowledges only after persistence and global acknowledgement with flaky persistence") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case7-primary")
    val secondaryA, secondaryB = TestProbe()
    val client = session(primary)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary, secondaryA.ref, secondaryB.ref)))

    val setId = client.set("foo", "bar")
    val seqA = secondaryA.expectMsgType[Snapshot].seq
    val seqB = secondaryB.expectMsgType[Snapshot].seq
    client.nothingHappens(300.milliseconds)
    secondaryA.reply(SnapshotAck("foo", seqA))
    client.nothingHappens(300.milliseconds)
    secondaryB.reply(SnapshotAck("foo", seqB))
    client.waitAck(setId)
  }

  test("case8: Primary and secondaries must work in concert when persistence and communication to secondaries is unreliable") {

    val arbiter = TestProbe()

    val (primary, user) = createPrimary(arbiter, "case8-primary", flakyPersistence = true)

    user.setAcked("k1", "v1")

    val (secondary1, replica1) = createSecondary(arbiter, "case8-secondary1", flakyForwarder = false, flakyPersistence = false)
    val (secondary2, replica2) = createSecondary(arbiter, "case8-secondary2", flakyForwarder = false, flakyPersistence = false)

    arbiter.send(primary, Replicas(Set(primary, secondary1, secondary2)))

    val options = Set(None, Some("v1"))
    options should contain(replica1.get("k1"))
    options should contain(replica2.get("k1"))

    user.setAcked("k1", "v2")
    assert(replica1.get("k1") === Some("v2"))
    assert(replica2.get("k1") === Some("v2"))

    val (secondary3, replica3) = createSecondary(arbiter, "case8-secondary3", flakyForwarder = false, flakyPersistence = false)

    arbiter.send(primary, Replicas(Set(primary, secondary1, secondary3)))

    replica3.nothingHappens(500.milliseconds)

    assert(replica3.get("k1") === Some("v2"))

    user.removeAcked("k1")
    assert(replica1.get("k1") === None)
    assert(replica2.get("k1") === Some("v2"))
    assert(replica3.get("k1") === None)

    user.setAcked("k1", "v4")
    assert(replica1.get("k1") === Some("v4"))
    assert(replica2.get("k1") === Some("v2"))
    assert(replica3.get("k1") === Some("v4"))

    user.setAcked("k2", "v1")
    user.setAcked("k3", "v1")

    user.setAcked("k1", "v5")
    user.removeAcked("k1")
    user.setAcked("k1", "v7")
    user.removeAcked("k1")
    user.setAcked("k1", "v9")
    assert(replica1.get("k1") === Some("v9"))
    assert(replica2.get("k1") === Some("v2"))
    assert(replica3.get("k1") === Some("v9"))

    assert(replica1.get("k2") === Some("v1"))
    assert(replica2.get("k2") === None)
    assert(replica3.get("k2") === Some("v1"))

    assert(replica1.get("k3") === Some("v1"))
    assert(replica2.get("k3") === None)
    assert(replica3.get("k3") === Some("v1"))
  }

  def createPrimary(arbiter: TestProbe, name: String, flakyPersistence: Boolean) = {

    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), name)

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    (primary, session(primary))
  }

  def createSecondary(arbiter: TestProbe, name: String, flakyForwarder: Boolean, flakyPersistence: Boolean) = {

    val secondary =
      if (flakyForwarder) system.actorOf(Props(new FlakyForwarder(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), name)), s"flaky-$name")
      else system.actorOf(Replica.props(arbiter.ref, Persistence.props(flakyPersistence)), name)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    (secondary, session(secondary))
  }
}

class FlakyForwarder(targetProps: Props, name: String) extends Actor with ActorLogging {

  val child = context.actorOf(targetProps, name)

  var flipFlop = true

  def receive = {

    case msg if sender == child =>
      context.parent forward msg

    case msg: Snapshot =>
      if (flipFlop) child forward msg
      else log.debug(s"Dropping $msg")
      flipFlop = !flipFlop

    case msg =>
      child forward msg
  }
}
