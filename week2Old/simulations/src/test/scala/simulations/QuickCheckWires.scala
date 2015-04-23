package simulations

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck._


/**
 * Created by rob on 04/02/15.
 */
class QuickCheckWires {

  lazy val genWire: Gen[Wire] = {
    for {
      b <- arbitrary[Boolean]
    } yield createWire(b)
  }

  def bools: Gen[Boolean] = Gen.oneOf(true, false)

  def createWire(signal: Boolean): Wire = {
    val wire = new Wire
    wire.setSignal(signal)
    wire
  }

  val genWireList = Gen.listOfN(8, genWire)

  implicit lazy val arbWire: Arbitrary[Wire] = Arbitrary(genWire)
  implicit lazy val arbWireList: Arbitrary[List[Wire]] = Arbitrary(genWireList)
}
