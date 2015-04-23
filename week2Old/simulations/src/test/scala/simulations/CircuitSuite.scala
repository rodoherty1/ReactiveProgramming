package simulations

import org.scalacheck.Prop._
import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers

@RunWith(classOf[JUnitRunner])
class CircuitSuite extends CircuitSimulator with FunSuite with Checkers {
  val InverterDelay = 1
  val AndGateDelay = 3
  val OrGateDelay = 5
  
  test("andGate example") {
    val in1, in2, out = new Wire
    andGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    
    assert(out.getSignal === false, "and 1")

    in1.setSignal(true)
    run
    
    assert(out.getSignal === false, "and 2")

    in2.setSignal(true)
    run
    
    assert(out.getSignal === true, "and 3")
  }


  test("orGate example") {
    val in1, in2, out = new Wire
    orGate(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    assert(out.getSignal === false, "or 1")

    in1.setSignal(true)
    run
    assert(out.getSignal === true, "or 2")

    in2.setSignal(true)
    run
    assert(out.getSignal === true, "or 3")

    in1.setSignal(false)
    run
    assert(out.getSignal === true, "or 4")

    in2.setSignal(false)
    run
    assert(out.getSignal === false, "or 5")

  }

  test("orGate2 example") {
    val in1, in2, out = new Wire
    orGate2(in1, in2, out)
    in1.setSignal(false)
    in2.setSignal(false)
    run
    assert(out.getSignal === false, "or 1")

    in1.setSignal(true)
    run
    assert(out.getSignal === true, "or 2")

    in2.setSignal(true)
    run
    assert(out.getSignal === true, "or 3")

    in1.setSignal(false)
    run
    assert(out.getSignal === true, "or 4")

    in2.setSignal(false)
    run
    assert(out.getSignal === false, "or 5")
  }

  extends Properties("Wires")
  property("p1") = forAll { w: Wire =>
    w.getSignal == (true || false)
  }

  test("Test Wire Generator.") {
    check(new QuickCheckWires)
  }

  test("demux") {

  }
}
