package quickcheck

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: A =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min-of-two-values") = forAll {
    (a: A, b: A) =>
      val h = insert(b, insert(a, empty))
      findMin(h) == (if (a < b) a else b)
  }

  property("Inserting into empty heap and then deleting the value, leaves an empty heap") = forAll {
    (a: A) =>
      val h1 = insert(a, empty)
      val h2 = deleteMin(h1)
      h2 == empty
  }

  property("Finding the minimum of the melding of any two heaps should return the minimum of one or the other") = forAll {
    (h1: H, h2: H) =>
      val min1 = findMin(h1)
      val min2 = findMin(h2)

      Math.min(min1, min2) == findMin(meld(h1, h2))
  }

  property("For any heap, you should get a sorted sequence when continually finding and deleting minima") = forAll {
    (h: H) =>
      val l = removeMins(h, List[A]())

      listsAreEqual(l, l.sorted)
  }

  property("For every element you insert, deleting the same number of deletes should clear the heap") = forAll {
    (l: List[Int]) =>
      val l1 = l.sorted
      val h = l1.foldRight(empty) {(v, h) => insert(v, h)}

      val l2 = removeMins(h, List[Int]())

      listsAreEqual(l1, l2)
  }

  property("Any two heaps, when melded together should be the same size as the sum of the size of each heap") = forAll {
    (h1: H, h2: H) =>
      val s1 = heapSize (h1, 0)
      val s2 = heapSize (h2, 0)

      val h3 = meld(h1, h2)
      heapSize(h3, 0) == s1 + s2
  }

  property("For any two heaps, moving a value from one heap to another should result in the same melding of heaps") = forAll {
    (h1: H, h2: H) =>
      val meld1 = meld(h1, h2)

      val d1 = findMin(h1)
      val h3 = deleteMin(h1)
      val h4 = insert(d1, h2)
      val meld2 = meld(h3, h4)

      val l1 = removeMins(meld1, List())
      val l2 = removeMins(meld2, List())
      listsAreEqual(l1, l2)

  }

  def listsAreEqual(l1: List[A], l2: List[A]): Boolean = {
    (l1, l2) match {
      case (Nil, Nil) => true
      case (Nil, xs) => false
      case (xs, Nil) => false
      case ((x :: xs), (y :: ys)) => if (x.equals(y)) {
        listsAreEqual(xs, ys)
      } else false
    }
  }

  def removeMins(h: H, l: List[A]): List[A] = {
    if (isEmpty(h)) l
    else removeMins(deleteMin(h), l :+ findMin(h))
  }

  def heapSize(h: H, s: Int): Int = if (isEmpty(h)) s else heapSize(deleteMin(h), s+1)

  lazy val genList: Gen[List[Int]] = {
    Gen.listOf(Gen.choose(0, 100))
  }

  lazy val genHeap: Gen[H] = {
    for {
      v <- arbitrary[Int]
      m <- oneOf(value(empty), genHeap)
    } yield insert(v, m)
  }

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)
  implicit lazy val arbList: Arbitrary[List[Int]] = Arbitrary(genList)

}
