package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal(Math.pow(b(), 2) - (4 * a() * c()))
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {

    Signal {
      val dd = computeDelta(a, b, c)()

      if (dd < 0) Set[Double]()
      else {
        val rootDelta = Math.sqrt(dd)
        val doubleA = 2 * a()
        val negativeB = -b()

        Set[Double]((negativeB + rootDelta) / doubleA, (negativeB - rootDelta) / doubleA)
      }
    }
  }
}
