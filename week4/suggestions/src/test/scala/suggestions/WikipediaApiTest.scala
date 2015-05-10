package suggestions


import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import rx.lang.scala._
import suggestions.gui._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}


@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite with Eventually with Matchers {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }

    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable.just("erik", "erik meijer", "martin")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true
    )
    assert(completed && count == 3, "completed: " + completed + ", event count: " + count)
  }
  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (n: Int) => Observable.just(0 to n: _*)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }


  test("WikipediaApi should properly recover") {
    val thrw = new Throwable
    val failedObservable = Observable.just(1, 2, 3) ++ Observable.error(thrw)

    val recoveredObservable = failedObservable.recovered
    //    val observed = mutable.Buffer[Any]()

    recoveredObservable.toBlocking.toList
    //    val sub = recoveredObservable subscribe {
    //      observed += _
    //    }

    assert(recoveredObservable.toBlocking.toList == Seq(Success(1), Success(2), Success(3), Failure(thrw)))
  }


  test("Observable should complete before timeout") {
    val start = System.currentTimeMillis
    val timedOutStream = Observable.from(1 to 3).zip(Observable.interval(100 millis)).timedOut(3L)
    val contents = timedOutStream.toBlocking.toList
    val totalTime = System.currentTimeMillis - start
    assert(contents == List((1, 0), (2, 1), (3, 2)))
    assert(totalTime <= 1000)
  }

  test("Observable(1, 2, 3).zip(Observable.interval(400 millis)).timedOut(1L) should return the first two values, and complete without errors") {
    val timedOutStream = Observable.from(1 to 3).zip(Observable.interval(400 millis)).timedOut(1L)
    val contents = timedOutStream.toBlocking.toList
    assert(contents == List((1, 0), (2, 1)))
  }

  test("Observable(1, 2, 3).zip(Observable.interval(700 millis)).timedOut(1L) should return the first value, and complete without errors") {
    val timedOutStream = Observable.from(1 to 3).zip(Observable.interval(700 millis)).timedOut(1L)
    val contents = timedOutStream.toBlocking.toList
    assert(contents == List((1, 0)))
  }

  test("WikipediaApi should correctly use concatRecovered - 2") {
    val requests = Observable.just(1, 2, 3, 4, 5)
    val computation = (num: Int) => if (num != 4) Observable.just(num) else Observable.error(new Exception)
    val responses = requests concatRecovered computation
    var gotException = false
    var completed = true
    var sum: Int = 0
    responses.subscribe(
      onNext = {
        case Success(x) =>
          sum = sum + x
          println(s"In test: $x")
        case Failure(ex) =>
          gotException = true
          println(s"Found an exception: $ex")
      },

      onError = ex => {
        println(s"Found an exception: $ex"); gotException = true
      },

      onCompleted = () => completed = true
    )

    eventually(timeout(5 seconds), interval(5 millis)) {
      completed should be(true)
    }

    assert(sum == (11) && gotException && completed, s"Sum: $sum, gotException: $gotException, completed: $completed")
  }


}
