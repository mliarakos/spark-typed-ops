package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.tags.Tagged
import com.github.mliarakos.spark.sql.typed.tags.TaggedWith
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql._

import scala.collection.immutable._
import scala.reflect.macros.blackbox
import scala.reflect.macros.whitebox

object TypedColumnOpsMacroImpl2 {

  def tag[A: c.WeakTypeTag](c: whitebox.Context)(value: c.Expr[A], name: c.Expr[String]): c.Expr[_ <: A] = {
    tagWithName(c)(value, name)
  }

  def taggedWith[A <: String: c.WeakTypeTag](c: blackbox.Context): c.Expr[TaggedWith[A]] = {
    import c.universe._

    val name       = getNameFromTag(c)(weakTypeOf[A])
    val taggedWith = q"new _root_.com.github.mliarakos.spark.sql.typed.tags.TaggedWith($name)"

    c.Expr[TaggedWith[A]](taggedWith)
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

  def udfTransform[A: c.WeakTypeTag, B: c.WeakTypeTag, Name <: String](c: whitebox.Context)(func: c.Expr[A => B]): c.Expr[Tagged[TypedColumn[Any, B], Name]] = {
    import c.universe._

    val inputType  = weakTypeOf[A]
    val returnType = weakTypeOf[B]

    // Construct input TypeTag[A] and return TypeTag[B]
    val inputTypeTag  = q"_root_.scala.reflect.runtime.universe.typeTag[$inputType]"
    val returnTypeTag = q"_root_.scala.reflect.runtime.universe.typeTag[$returnType]"

    // Get or create udf from registry to prevent repeated creation of the same udf
    // Pass implicit TypeTag[A] and TypeTag[B] directly
    val cachedUdf   = q"_root_.com.github.mliarakos.spark.sql.typed.TypedUdfRegistry.getOrCreate($func)($inputTypeTag, $returnTypeTag)"
    val typedColumn = extractTaggedTypedColumn(c)(c.prefix)
    val (_, name)   = extractTaggedColumnByExpr(c)(typedColumn)

    val transformedTree = c.Expr[TypedColumn[Any, B]] {
      q"$cachedUdf.apply($typedColumn).as($name).as[$returnType]"
    }

    tagWithName(c)(transformedTree, c.Expr[String](q"$name"))
  }

  def datasetTransformTo[B: c.WeakTypeTag](c: blackbox.Context)(cols: c.Expr[_ => Tagged[TypedColumn[_, _], _]]*): c.Expr[Dataset[B]] = {
    import c.universe._

    // Extract case class fields of target type and validate column count
    val (targetType, targetFields) = extractAndValidateTargetType[B](c)(cols.length)

    // Extract (column name, column type, column expr) from each (_ => Tagged[TypedColumn[_, _], _]) expression
    val columnDetails = cols.toList.map { colExpr =>
      val selectorType = c.typecheck(colExpr.tree).tpe
      selectorType match {
        // Match type [_ => Tagged[TypedColumn[_, fieldType], tag]] to extract field type and tag
        case TypeRef(_, _, List(_, taggedColumnType)) =>
          val (columnType, name) = extractTaggedColumnByType(c)(taggedColumnType)
          (name, columnType, colExpr)
        case _ => c.abort(colExpr.tree.pos, s"Expected expression resulting in a Tagged[TypedColumn[_, _], _], but found: $selectorType")
      }
    }

    val dataset        = extractDataset(c)(c.prefix)
    val orderedColumns = orderColumns(c)(columnDetails, targetFields).map(colExpr => q"$colExpr.apply($dataset)")

    c.Expr[Dataset[B]] {
      q"$dataset.select(..$orderedColumns).as[$targetType]"
    }
  }

  def columnTransformTo[B: c.WeakTypeTag, Name <: String](c: whitebox.Context)(
      fields: c.Expr[_ => Tagged[TypedColumn[_, _], _]]*
  ): c.Expr[Tagged[TypedColumn[Any, B], Name]] = {
    import c.universe._

    // Extract case class fields of target type and validate column count
    val (targetType, targetFields) = extractAndValidateTargetType[B](c)(fields.length)

    // Extract (column name, column type, column expr) from each (_ => Tagged[TypedColumn[_, _], _]) expression
    val columnDetails = fields.toList.map { colExpr =>
      val selectorType = c.typecheck(colExpr.tree).tpe
      selectorType match {
        // Match type [_ => Tagged[TypedColumn[_, fieldType], tag]] to extract field type and tag
        case TypeRef(_, _, List(_, taggedColumnType)) =>
          val (columnType, name) = extractTaggedColumnByType(c)(taggedColumnType)
          (name, columnType, colExpr)
        case _ => c.abort(colExpr.tree.pos, s"Expected expression resulting in a Tagged[TypedColumn[_, _], _], but found: $selectorType")
      }
    }

    val typedColumn    = extractTaggedTypedColumn(c)(c.prefix)
    val orderedColumns = orderColumns(c)(columnDetails, targetFields).map(colExpr => q"$colExpr.apply($typedColumn)")

    val (_, name) = extractTaggedColumnByExpr(c)(typedColumn)
    val struct    = c.Expr[TypedColumn[Any, B]] {
      q"_root_.org.apache.spark.sql.functions.struct(..$orderedColumns).as($name).as[$targetType]"
    }

    tagWithName(c)(struct, c.Expr[String](q"$name"))
  }

  def struct[B: c.WeakTypeTag](c: blackbox.Context)(cols: c.Expr[Tagged[org.apache.spark.sql.TypedColumn[_, _], _]]*): c.Expr[TypedColumn[Any, B]] = {
    import c.universe._

    // Extract case class fields of target type and validate column count
    val (targetType, targetFields) = extractAndValidateTargetType[B](c)(cols.length)

    // Extract (column name, column type, column expr) from each Tagged[TypedColumn[_, _], _] expression
    val columnDetails = cols.toList.map { colExpr =>
      val taggedColumnType   = c.typecheck(colExpr.tree).tpe
      val (columnType, name) = extractTaggedColumnByType(c)(taggedColumnType)
      (name, columnType, colExpr)
    }

    val orderedColumns = orderColumns(c)(columnDetails, targetFields)

    c.Expr[TypedColumn[Any, B]] {
      q"_root_.org.apache.spark.sql.functions.struct(..$orderedColumns).as[$targetType]"
    }
  }

  private def tagWithName[A: c.WeakTypeTag, B <: A](c: whitebox.Context)(value: c.Expr[A], name: c.Expr[String]): c.Expr[B] = {
    import c.universe._

    val valueType = weakTypeOf[A]
    val nameType  = name.tree match {
      case Literal(Constant(_: String)) => c.typecheck(name.tree).tpe
      case other                        => c.abort(c.enclosingPosition, s"Expected a String literal, but found: $other")
    }

    val taggedType = tq"_root_.com.github.mliarakos.spark.sql.typed.tags.Tagged[$valueType, $nameType]"
    val taggedTree = q"$value.asInstanceOf[$taggedType]"

    c.Expr[B](taggedTree)
  }

  /** Extract the type and case class fields of the provided target type and validate that the actual count of input columns/fields matches the target field
    * count
    */
  private def extractAndValidateTargetType[B: c.WeakTypeTag](c: blackbox.Context)(actualCount: Int): (c.Type, List[(String, c.Type)]) = {
    import c.universe._

    // Extract case class fields of target type in constructor order
    val targetType   = weakTypeOf[B]
    val targetFields = targetType.decls.collect { case m: MethodSymbol if m.isCaseAccessor => (m.name.decodedName.toString, m.returnType) }.toList

    // Validate column count
    val expectedCount = targetFields.length
    if (actualCount != expectedCount) {
      c.abort(c.enclosingPosition, s"Expected $expectedCount column(s), but found $actualCount")
    }

    (targetType, targetFields)
  }

  /** Match input columns to the fields of the target type in case class constructor order and validate that every field of the target type is matched exactly
    * once and that the matching column has the correct type
    */
  private def orderColumns[A](c: blackbox.Context)(
      columnDetails: List[(String, c.Type, c.Expr[A])],
      targetFields: List[(String, c.Type)]
  ): List[c.Expr[A]] = {
    // Iterate through each field of the target type
    targetFields.map { case (fieldName, fieldType) =>
      // Find matching input column(s) by name
      columnDetails.filter { case (columnName, _, _) => columnName == fieldName } match {
        // One match
        case (_, resultType, colExpr) :: Nil =>
          // Validate type
          if (resultType <:< fieldType) {
            colExpr
          } else {
            c.abort(colExpr.tree.pos, s"Column for field '$fieldName' has type $resultType but expected type $fieldType")
          }
        // Fail on multiple matches
        case (_, _, colExpr) :: tail => c.abort(colExpr.tree.pos, s"Column for field '$fieldName' occurs more than once (${tail.length} extra time(s))")
        // Fail on no matches
        case Nil => c.abort(c.enclosingPosition, s"Missing column for field '$fieldName' of type $fieldType")
      }
    }
  }

  /** Extract the field type (`F`) and tag name (`N`) from the `Tagged[TypedColumn[_, F], N]` expression */
  private def extractTaggedColumnByExpr(c: blackbox.Context)(taggedColumn: c.Expr[Tagged[TypedColumn[_, _], _]]): (c.Type, String) = {
    val taggedColumnType = c.typecheck(taggedColumn.tree).tpe

    extractTaggedColumnByType(c)(taggedColumnType)
  }

  /** Extract the field type (`F`) and tag name (`N`) from the `Tagged[TypedColumn[_, F], N]` type */
  private def extractTaggedColumnByType(c: blackbox.Context)(taggedColumnType: c.Type): (c.Type, String) = {
    import c.universe._

    val (fieldType, name) = taggedColumnType match {
      // Match type Tagged[TypedColumn[_, fieldType], tag] to extract field type and tag
      case TypeRef(_, _, List(TypeRef(_, _, List(_, fieldType)), tag)) => (fieldType, getNameFromTag(c)(tag))
      case other => c.abort(c.enclosingPosition, s"Expected expression resulting in a Tagged[TypedColumn[_, _], _], but found: $other")
    }

    (fieldType, name)
  }

  private def getNameFromTag(c: blackbox.Context)(tagType: c.Type): String = {
    import c.universe._

    tagType match {
      case ConstantType(Constant(name: String)) => name
      case other                                => c.abort(c.enclosingPosition, s"Expected a String constant type, but found: $other")
    }
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

  private def extractTaggedTypedColumn(c: blackbox.Context)(expr: c.Expr[_]): c.Expr[Tagged[TypedColumn[_, _], _]] = {
    import c.universe._

    // Assume access is happening via the extension method in the TypedColumnTransformTypedOps implicit class
    // Get the name of the TypedColumn from the single argument of the implicit class instantiation
    val taggedTypedColumn = expr.tree match {
      case Apply(_, List(arg)) => arg
      case _                   => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
    }

    c.Expr[Tagged[TypedColumn[_, _], _]](taggedTypedColumn)
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
