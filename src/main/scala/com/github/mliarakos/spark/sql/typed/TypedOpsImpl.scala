package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.collection.immutable._
import scala.reflect.macros.blackbox

private[typed] object TypedOpsImpl {

  def name(c: blackbox.Context)(name: c.Expr[Any]): c.Expr[String] = {
    getColumnName(c)(name)
  }

  def names(c: blackbox.Context)(names: c.Expr[Any]*): c.Expr[Seq[String]] = {
    import c.universe._

    val fields = names.map(getColumnName(c)(_))
    val seq    = q"Seq(..$fields)"

    c.Expr[Seq[String]](seq)
  }

  def column(c: blackbox.Context)(col: c.Expr[Any]): c.Expr[Column] = {
    getColumn(c)(col, None)
  }

  def columns(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[Seq[Column]] = {
    import c.universe._

    val columns = cols.map(getColumn(c)(_, None))
    val seq     = q"Seq(..$columns)"

    c.Expr[Seq[Column]](seq)
  }

  def typedColumn[A, B](
      c: blackbox.Context
  )(col: c.Expr[Any])(implicit tagA: c.WeakTypeTag[A], tagB: c.WeakTypeTag[B]): c.Expr[TypedColumn[A, B]] = {
    getTypedColumn[A, B](c)(col, tagB, None)
  }

  def datasetColumn(c: blackbox.Context)(col: c.Expr[Any]): c.Expr[Column] = {
    val dataset = getDataset(c)(c.prefix)
    getColumn(c)(col, Some(dataset))
  }

  def datasetColumns(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[Seq[Column]] = {
    import c.universe._

    val dataset = getDataset(c)(c.prefix)
    val columns = cols.map(getColumn(c)(_, Some(dataset)))
    val seq     = q"Seq(..$columns)"

    c.Expr[Seq[Column]](seq)
  }

  def datasetCube(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[RelationalGroupedDataset] = {
    datasetMethod[RelationalGroupedDataset](c)("cube")(cols: _*)
  }

  def datasetDescribe(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[DataFrame] = {
    datasetMethod[DataFrame](c)("describe")(cols: _*)
  }

  def datasetDrop(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[DataFrame] = {
    datasetMethod[DataFrame](c)("drop")(cols: _*)
  }

  def datasetDropDuplicates[A](c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[Dataset[A]] = {
    datasetMethod[Dataset[A]](c)("dropDuplicates")(cols: _*)
  }

  def datasetGroupBy(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[RelationalGroupedDataset] = {
    datasetMethod[RelationalGroupedDataset](c)("groupBy")(cols: _*)
  }

  def datasetOrderBy[A](c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[Dataset[A]] = {
    datasetMethod[Dataset[A]](c)("orderBy")(cols: _*)
  }

  def datasetRollup(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[RelationalGroupedDataset] = {
    datasetMethod[RelationalGroupedDataset](c)("rollup")(cols: _*)
  }

  def datasetSelect(c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[DataFrame] = {
    import c.universe._

    val dataset = getDataset(c)(c.prefix)
    val columns = cols.map(getColumn(c)(_, Some(dataset)))
    val select  = q"$dataset.select(..$columns)"

    c.Expr[DataFrame](select)
  }

  def datasetSort[A](c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[Dataset[A]] = {
    datasetMethod[Dataset[A]](c)("sort")(cols: _*)
  }

  def datasetSortWithinPartitions[A](c: blackbox.Context)(cols: c.Expr[Any]*): c.Expr[Dataset[A]] = {
    datasetMethod[Dataset[A]](c)("sortWithinPartitions")(cols: _*)
  }

  def datasetSelectTyped1[B1](
      c: blackbox.Context
  )(c1: c.Expr[Any])(implicit tag1: c.WeakTypeTag[B1]): c.Expr[Dataset[B1]] = {
    val select = datasetSelectTyped(c)(c1)(tag1)
    c.Expr[Dataset[B1]](select.tree)
  }

  def datasetSelectTyped2[B1, B2](c: blackbox.Context)(
      c1: c.Expr[Any],
      c2: c.Expr[Any]
  )(implicit tag1: c.WeakTypeTag[B1], tag2: c.WeakTypeTag[B2]): c.Expr[Dataset[(B1, B2)]] = {
    val select = datasetSelectTyped(c)(c1, c2)(tag1, tag2)
    c.Expr[Dataset[(B1, B2)]](select.tree)
  }

  def datasetSelectTyped3[B1, B2, B3](c: blackbox.Context)(
      c1: c.Expr[Any],
      c2: c.Expr[Any],
      c3: c.Expr[Any]
  )(
      implicit tag1: c.WeakTypeTag[B1],
      tag2: c.WeakTypeTag[B2],
      tag3: c.WeakTypeTag[B3]
  ): c.Expr[Dataset[(B1, B2, B3)]] = {
    val select = datasetSelectTyped(c)(c1, c2, c3)(tag1, tag2, tag3)
    c.Expr[Dataset[(B1, B2, B3)]](select.tree)
  }

  def datasetSelectTyped4[B1, B2, B3, B4](c: blackbox.Context)(
      c1: c.Expr[Any],
      c2: c.Expr[Any],
      c3: c.Expr[Any],
      c4: c.Expr[Any]
  )(
      implicit tag1: c.WeakTypeTag[B1],
      tag2: c.WeakTypeTag[B2],
      tag3: c.WeakTypeTag[B3],
      tag4: c.WeakTypeTag[B4]
  ): c.Expr[Dataset[(B1, B2, B3, B4)]] = {
    val select = datasetSelectTyped(c)(c1, c2, c3, c4)(tag1, tag2, tag3, tag4)
    c.Expr[Dataset[(B1, B2, B3, B4)]](select.tree)
  }

  def datasetSelectTyped5[B1, B2, B3, B4, B5](c: blackbox.Context)(
      c1: c.Expr[Any],
      c2: c.Expr[Any],
      c3: c.Expr[Any],
      c4: c.Expr[Any],
      c5: c.Expr[Any]
  )(
      implicit tag1: c.WeakTypeTag[B1],
      tag2: c.WeakTypeTag[B2],
      tag3: c.WeakTypeTag[B3],
      tag4: c.WeakTypeTag[B4],
      tag5: c.WeakTypeTag[B5]
  ): c.Expr[Dataset[(B1, B2, B3, B4, B5)]] = {
    val select = datasetSelectTyped(c)(c1, c2, c3, c4, c5)(tag1, tag2, tag3, tag4, tag5)
    c.Expr[Dataset[(B1, B2, B3, B4, B5)]](select.tree)
  }

  def datasetWithColumnRenamed(
      c: blackbox.Context
  )(existingName: c.Expr[Any], newName: c.Expr[String]): c.Expr[DataFrame] = {
    import c.universe._

    val dataset = getDataset(c)(c.prefix)
    val name    = getColumnName(c)(existingName)
    val renamed = q"$dataset.withColumnRenamed($name, $newName)"

    c.Expr[DataFrame](renamed)
  }

  private def datasetSelectTyped(
      c: blackbox.Context
  )(cols: c.Expr[Any]*)(tags: c.WeakTypeTag[_]*): c.Expr[Dataset[_]] = {
    import c.universe._

    val dataset = getDataset(c)(c.prefix)
    val columns = cols.zip(tags).map({ case (col, tag) => getTypedColumn(c)(col, tag, Some(dataset)) })
    val select  = q"$dataset.select(..$columns)"

    c.Expr[Dataset[_]](select)
  }

  private def datasetMethod[A](c: blackbox.Context)(method: String)(cols: c.Expr[Any]*): c.Expr[A] = {
    import c.universe._

    val dataset = getDataset(c)(c.prefix)
    val columns = cols.map(getColumnName(c)(_))
    val op      = q"$dataset.${TermName(method)}(..$columns)"

    c.Expr[A](op)
  }

  private def getDataset(c: blackbox.Context)(expr: c.Expr[_]): c.Expr[Dataset[_]] = {
    import c.universe._

    // Assume access is happening via the extension method in the DatasetColumn implicit class
    // Get the name of the dataset from the single argument of the implicit class instantiation
    val dataset = expr.tree match {
      case Apply(_, List(arg)) => arg
      case _                   => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
    }

    c.Expr[Dataset[_]](dataset)
  }

  private def getTypedColumn[A, B](
      c: blackbox.Context
  )(
      column: c.Expr[_],
      tag: c.WeakTypeTag[B],
      dataset: Option[c.Expr[Dataset[_]]] = None
  ): c.Expr[TypedColumn[A, B]] = {
    import c.universe._

    val col      = getColumn(c)(column, dataset)
    val typedCol = q"$col.as[$tag]"

    c.Expr[TypedColumn[A, B]](typedCol)
  }

  private def getColumn(
      c: blackbox.Context
  )(column: c.Expr[_], dataset: Option[c.Expr[Dataset[_]]] = None): c.Expr[Column] = {
    import c.universe._

    val columnName = getColumnName(c)(column)
    val col        = dataset.fold(q"org.apache.spark.sql.functions.col($columnName)")(ds => q"$ds.col($columnName)")

    c.Expr[Column](col)
  }

  private def getColumnName(c: blackbox.Context)(column: c.Expr[_]): c.Expr[String] = {
    import c.universe._

    def extractNames(tree: c.Tree): List[c.Name] = {
      tree.children.headOption match {
        case Some(child) => extractNames(child) :+ tree.symbol.name
        case None        => List(tree.symbol.name)
      }
    }

    @tailrec def extract(tree: c.Tree): List[c.Name] = tree match {
      case Ident(n)           => List(n)
      case Select(tree, n)    => extractNames(tree) :+ n
      case Function(_, body)  => extract(body)
      case Block(_, expr)     => extract(expr)
      case Apply(func, _)     => extract(func)
      case TypeApply(func, _) => extract(func)
      case _                  => c.abort(c.enclosingPosition, s"Unsupported expression: $column")
    }

    // drop sth like x$1
    val columnName = extract(column.tree).drop(1).mkString(".")

    c.Expr[String](q"$columnName")
  }

}
