package calculator

import scala.collection.immutable.Iterable

sealed abstract class Expr

final case class Literal(v: Double) extends Expr

final case class Ref(name: String) extends Expr

final case class Plus(a: Expr, b: Expr) extends Expr

final case class Minus(a: Expr, b: Expr) extends Expr

final case class Times(a: Expr, b: Expr) extends Expr

final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {

    namedExpressions.map {
      case (k, v) => (k, Var { eval(v(), namedExpressions) })
    }
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    expr match {
      case Literal(v) => v
      case Plus(e1, e2) => eval(e1, references) + eval(e2, references)
      case Minus(e1, e2) => eval(e1, references) - eval(e2, references)
      case Times(e1, e2) => eval(e1, references) * eval(e2, references)
      case Divide(e1, e2) => eval(e1, references) / eval(e2, references)
      case Ref(name) => eval(getReferenceExpr(name, references), references filter (kv => kv._1 != name))
      case _ => Double.NaN
    }
  }


  /** Get the Expr for a referenced variables.
    * If the variable is not known, returns a literal NaN.
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
