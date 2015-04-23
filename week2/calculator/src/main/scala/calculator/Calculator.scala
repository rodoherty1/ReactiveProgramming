package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {

    /*
    Below you can see that I have removed k from namedExpressions.
    I am reading some of the tips on the coursera page (https://class.coursera.org/reactive-002/forum/thread?thread_id=257)
    and they are saying things like "look at the map and notice what is in there that shouldn't be in there".
    Also in the lecture, Martin said something about seeing if the list of observers of an expression contains the
    expression itself.

    E.g. a = b + 1 and b = a + 1 then a = (a + 1) + 1 which is invalid.

    So, do I need to remove k from namedExpressions and then evaluate it out and make sure that k never comes up again?
    Gotta hit the hay now !!!!
     */
    namedExpressions.map {
      case (k, v) => (k, Signal { eval(v(), namedExpressions - k) })
    }

  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    expr match {
      case Literal(v) => v
      case Plus(e1, e2) => eval(e1, references) + eval(e2, references)
      case Minus(e1, e2) => eval(e1, references) - eval(e2, references)
      case Times(e1, e2) => eval(e1, references) * eval(e2, references)
      case Divide(e1, e2) => eval(e1, references) / eval(e2, references)
      case Ref(name) => eval(getReferenceExpr(name, references), references)
      case _ => Double.NaN
    }
  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]): Expr = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
