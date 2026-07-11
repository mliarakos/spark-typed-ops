package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.tags.NameOf
import org.apache.spark.sql._

import scala.collection.immutable._
import scala.reflect.macros.blackbox
import scala.reflect.macros.whitebox
import scala.annotation.tailrec

object TypedColumnOpsMacroImpl2 {

  def tag[A: c.WeakTypeTag](c: whitebox.Context)(value: c.Expr[A], name: c.Expr[String]): c.Expr[_ <: A] = {
    tagWithName(c)(value, name)
  }

  def nameOf[A <: String: c.WeakTypeTag](c: blackbox.Context): c.Expr[NameOf[A]] = {
    import c.universe._

    @tailrec def extract(tpe: Type): String = tpe match {
      case ConstantType(Constant(name: String)) => name                          // Literal constant type
      case TypeRef(_, symbol, _)                => extract(symbol.typeSignature) // Type alias, symbols, or refs that eventually resolve to a constant
      case AnnotatedType(_, underlying)         => extract(underlying)           // Existential or refined types that wrap a constant
      case other                                => c.abort(c.enclosingPosition, s"Expected a constant string type, but got: $other")
    }

    // val name = weakTypeOf[A] match {
    //   case ConstantType(Constant(name: String)) => name
    //   case other                                => c.abort(c.enclosingPosition, s"Expected a constant type, but got: $other")
    // }

    val name   = extract(weakTypeOf[A])
    val nameOf = q"new _root_.com.github.mliarakos.spark.sql.typed.tags.NameOf($name)"

    c.Expr[NameOf[A]](nameOf)
  }

  def column[A: c.WeakTypeTag, B: c.WeakTypeTag](c: whitebox.Context)(selector: c.Expr[A => B]): c.Expr[_ <: TypedColumn[A, B]] = {
    import c.universe._

    val dataset     = extractDataset(c)(c.prefix)
    val columnName  = extractSelectorName(c)(selector)
    val columnType  = weakTypeOf[B]
    val typedColumn = c.Expr[TypedColumn[A, B]](q"$dataset.col($columnName).as[$columnType]")

    tagWithName(c)(typedColumn, columnName)
  }

  def field[A: c.WeakTypeTag, B: c.WeakTypeTag](c: whitebox.Context)(selector: c.Expr[A => B]): c.Expr[_ <: TypedColumn[A, B]] = {
    import c.universe._

    val typedColumn = extractTypedColumn(c)(c.prefix)
    val columnName  = extractSelectorName(c)(selector)
    val columnType  = weakTypeOf[B]
    val fieldColumn = c.Expr[TypedColumn[A, B]](q"$typedColumn.getField($columnName).as($columnName).as[$columnType]")

    tagWithName(c)(fieldColumn, columnName)
  }

  def rename[A: c.WeakTypeTag, B: c.WeakTypeTag](c: whitebox.Context)(name: c.Expr[String]): c.Expr[_ <: TypedColumn[A, B]] = {
    import c.universe._

    val typedColumn   = extractTypedColumn(c)(c.prefix)
    val inputType     = weakTypeOf[A]
    val fieldType     = weakTypeOf[B]
    val renamedColumn = c.Expr[TypedColumn[A, B]](q"$typedColumn.as($name).asInstanceOf[_root_.org.apache.spark.sql.TypedColumn[$inputType, $fieldType]]")

    tagWithName(c)(renamedColumn, name)
  }

  def renameTo[A: c.WeakTypeTag, B: c.WeakTypeTag](c: whitebox.Context)(selector: c.Expr[A => B]): c.Expr[_ <: TypedColumn[A, B]] = {
    import c.universe._

    val typedColumn   = extractTypedColumn(c)(c.prefix)
    val columnName    = extractSelectorName(c)(selector)
    val columnType    = weakTypeOf[B]
    val renamedColumn = c.Expr[TypedColumn[A, B]](q"$typedColumn.as($columnName).as[$columnType]")

    tagWithName(c)(renamedColumn, columnName)
  }

  private def tagWithName[A: c.WeakTypeTag, B <: A](c: whitebox.Context)(value: c.Expr[A], name: c.Expr[String]): c.Expr[B] = {
    import c.universe._

    val valueType = weakTypeOf[A]
    val nameType  = name.tree match {
      case Literal(Constant(_: String)) => c.typecheck(name.tree).tpe
      case other                        => c.abort(c.enclosingPosition, s"Expected a literal string, but got: $other")
    }

    val taggedType = tq"_root_.com.github.mliarakos.spark.sql.typed.tags.Tagged[$valueType, $nameType]"
    val taggedTree = q"$value.asInstanceOf[$taggedType]"

    c.Expr[B](taggedTree)
  }

  private def extractDataset(c: blackbox.Context)(expr: c.Expr[_]): c.Expr[Dataset[_]] = {
    import c.universe._

    // Assume access is happening via the extension method in the DatasetTransformTypedOps implicit class
    // Get the name of the Dataset from the single argument of the implicit class instantiation
    val dataset = expr.tree match {
      case Apply(_, List(arg)) => arg
      case _                   => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
    }

    c.Expr[Dataset[_]](dataset)
  }

  private def extractTypedColumn(c: blackbox.Context)(expr: c.Expr[_]): c.Expr[TypedColumn[_, _]] = {
    import c.universe._

    // Assume access is happening via the extension method in the TypedColumnTransformTypedOps implicit class
    // Get the name of the TypedColumn from the single argument of the implicit class instantiation
    val typedColumn = expr.tree match {
      case Apply(_, List(arg)) => arg
      case _                   => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
    }

    c.Expr[TypedColumn[_, _]](typedColumn)
  }

  private def extractSelectorName(c: blackbox.Context)(expr: c.Expr[Any]): c.Expr[String] = {
    import c.universe._

    def extract(tree: c.Tree, param: Symbol): List[String] = tree match {
      case Select(qualifier, name)          => extract(qualifier, param) :+ name.decodedName.toString // _.foo.bar
      case Function(params, body)           => extract(body, params.head.symbol)                      // x => x.foo.bar
      case Block(_, expr)                   => extract(expr, param)                                   // Block wrapper
      case Apply(func, _)                   => extract(func, param)                                   // Function apply wrapper
      case TypeApply(func, _)               => extract(func, param)                                   // Type application wrapper
      case Ident(_) if tree.symbol == param => Nil                                                    // Valid root identifier (e.g., x$1)
      case Ident(other)                     => c.abort(c.enclosingPosition, s"Expression must start from the lambda parameter, but found reference to: $other")
      case other                            => c.abort(c.enclosingPosition, s"Unsupported expression for name extraction: $other")
    }

    val name = extract(expr.tree, NoSymbol).mkString(".")
    if (name.isEmpty) c.abort(c.enclosingPosition, s"Could not extract name from: $expr")

    c.Expr[String](q"$name")
  }

}
