package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql._

import scala.collection.immutable._
import scala.reflect.macros.blackbox

object TypedColumnOpsMacroImpl {

  def column[A, B: c.WeakTypeTag](c: blackbox.Context)(selector: c.Expr[A => B]): c.Expr[TypedColumn[A, B]] = {
    import c.universe._

    val dataset         = extractDataset(c)(c.prefix)
    val columnName      = extractSelectorName(c)(selector)
    val columnType      = weakTypeOf[B]
    val typedColumnTree = q"$dataset($columnName).as($columnName).as[$columnType]"

    c.Expr[TypedColumn[A, B]](typedColumnTree)
  }

  def field[T, B: c.WeakTypeTag](c: blackbox.Context)(selector: c.Expr[T => B]): c.Expr[TypedColumn[T, B]] = {
    import c.universe._

    val typedColumn     = extractTypedColumn(c)(c.prefix)
    val columnName      = extractSelectorName(c)(selector)
    val columnType      = weakTypeOf[B]
    val typedColumnTree = q"$typedColumn.getField($columnName).as($columnName).as[$columnType]"

    c.Expr[TypedColumn[T, B]](typedColumnTree)
  }

  def renameTo[A, B: c.WeakTypeTag](c: blackbox.Context)(selector: c.Expr[A => B]): c.Expr[TypedColumn[A, B]] = {
    import c.universe._

    val typedColumn = extractTypedColumn(c)(c.prefix)
    val columnName  = extractSelectorName(c)(selector)
    val columnType  = weakTypeOf[B]
    val mappedTree  = q"$typedColumn.as($columnName).as[$columnType]"

    c.Expr[TypedColumn[A, B]](mappedTree)
  }

  def udfTransform[A: c.WeakTypeTag, B: c.WeakTypeTag](c: blackbox.Context)(func: c.Expr[A => B]): c.Expr[TypedColumn[Any, B]] = {
    import c.universe._

    val inputType  = weakTypeOf[A]
    val returnType = weakTypeOf[B]

    // Construct TypeTag[T] and TypeTag[B]
    val inputTypeTag  = q"_root_.scala.reflect.runtime.universe.typeTag[$inputType]"
    val returnTypeTag = q"_root_.scala.reflect.runtime.universe.typeTag[$returnType]"

    // Get or create udf from registry to prevent repeated creation of the same udf
    // Pass implicit TypeTag[T] and TypeTag[B] directly
    val udf             = q"_root_.com.github.mliarakos.spark.sql.typed.TypedUdfRegistry.getOrCreate($func)($inputTypeTag, $returnTypeTag)"
    val typedColumn     = extractTypedColumn(c)(c.prefix)
    val transformedTree = q"$udf.apply($typedColumn).as[$returnType]"

    c.Expr[TypedColumn[Any, B]](transformedTree)
  }

  def selectTo[B: c.WeakTypeTag](c: blackbox.Context)(cols: c.Expr[org.apache.spark.sql.TypedColumn[B, _]]*): c.Expr[Dataset[B]] = {
    import c.universe._

    // Extract case class fields of B in constructor order
    val bType   = weakTypeOf[B]
    val bFields = bType.decls.collect { case m: MethodSymbol if m.isCaseAccessor => (m.name.decodedName.toString, m.returnType) }.toList

    // Validate column count
    val expectedCount = bFields.length
    val actualCount   = cols.length
    if (actualCount != expectedCount)
      c.abort(c.enclosingPosition, s"Expected $expectedCount columns for selectTo[$bType] but found $actualCount")

    // Extract (column name, result type, column expr) from each TypedColumn[B, X]
    val columnDetails = cols.toList.map { colExpr =>
      val columnType = c.typecheck(colExpr.tree).tpe
      columnType match {
        case TypeRef(_, _, List(_, resultType)) =>
          // Extract the column name from the .as("name") alias
          val name = colExpr.tree match {
            case q"($_.as($alias).as[$_]($_): $_)" =>
              alias match {
                case Literal(Constant(name: String)) => name
                case _                               => c.abort(alias.pos, s"Column alias must be a literal string")
              }
            case _ => c.abort(colExpr.tree.pos, s"""TypedColumn must include .as("fieldName") to match fields by name""")
          }

          (name, resultType, colExpr)
        case _ => c.abort(colExpr.tree.pos, s"Expected type TypedColumn[$bType, _] but found type $columnType")
      }
    }

    // Collect columns in case class constructor order of B
    // Validate that every field in B appears exactly once and that the matching column has the correct type
    val orderedColumns =
      bFields.map { case (fieldName, fieldType) =>
        columnDetails.filter { case (columnName, _, _) => columnName == fieldName } match {
          // One match
          case (_, resultType, colExpr) :: Nil =>
            // Validate type
            if (resultType <:< fieldType) {
              colExpr
            } else {
              c.abort(colExpr.tree.pos, s"Column for field '$fieldName' has type $resultType but expected type $fieldType")
            }
          // Multiple matches
          case (_, _, colExpr) :: tail => c.abort(colExpr.tree.pos, s"Column for field '$fieldName' occurs more than once")
          // No matches
          case Nil => c.abort(c.enclosingPosition, s"Missing column for field '$fieldName' of type $fieldType")
        }
      }

    val dataset      = extractDataset(c)(c.prefix)
    val selectAsTree = q"$dataset.select(..$orderedColumns).as[$bType]"

    c.Expr[Dataset[B]](selectAsTree)
  }

  def datasetTransformTo[B: c.WeakTypeTag](c: blackbox.Context)(cols: c.Expr[_ => org.apache.spark.sql.TypedColumn[B, _]]*): c.Expr[Dataset[B]] = {
    import c.universe._

    // Extract case class fields of B in constructor order
    val bType   = weakTypeOf[B]
    val bFields = bType.decls.collect { case m: MethodSymbol if m.isCaseAccessor => (m.name.decodedName.toString, m.returnType) }.toList

    // Validate column count
    val expectedCount = bFields.length
    val actualCount   = cols.length
    if (actualCount != expectedCount)
      c.abort(c.enclosingPosition, s"Expected $expectedCount columns for transformTo[$bType] but found $actualCount")

    // Extract (column name, result type, column expr) from each (_ => TypedColumn[B, X]) expression
    val columnDetails = cols.toList.map { colExpr =>
      val columnType = c.typecheck(colExpr.tree).tpe
      columnType match {
        // Match [TypedColumn[_, _] => TypedColumn[_, resultType]] to extract column type
        case TypeRef(_, _, List(_, TypeRef(_, _, List(_, resultType)))) =>
          // Extract the column name from the .as("name") alias
          val name = colExpr.tree match {
            case q"(($_) => ($_.as($alias).as[$_]($_): $_))" =>
              alias match {
                case Literal(Constant(name: String)) => name
                case _                               => c.abort(alias.pos, s"Column alias must be a literal string")
              }
            case _ => c.abort(colExpr.tree.pos, s"""TypedColumn must include .as("name") to match fields by name""")
          }

          (name, resultType, colExpr)
        case _ => c.abort(colExpr.tree.pos, s"Expected type TypedColumn[$bType, _] but found type $columnType")
      }
    }

    // Collect columns in case class constructor order of B
    // Validate that every field in B appears exactly once and that the matching column has the correct type
    val dataset        = extractDataset(c)(c.prefix)
    val orderedColumns =
      bFields.map { case (fieldName, fieldType) =>
        columnDetails.filter { case (columnName, _, _) => columnName == fieldName } match {
          // One match
          case (_, resultType, colExpr) :: Nil =>
            // Validate type
            if (resultType <:< fieldType) {
              // Apply column function to dataset to get the column
              q"$colExpr.apply($dataset)"
            } else {
              c.abort(colExpr.tree.pos, s"Column for field '$fieldName' has type $resultType but expected type $fieldType")
            }
          // Multiple matches
          case (_, _, colExpr) :: tail => c.abort(colExpr.tree.pos, s"Column for field '$fieldName' occurs more than once (${tail.length} extra times)")
          // No matches
          case Nil => c.abort(c.enclosingPosition, s"Missing column for field '$fieldName' of type $fieldType")
        }
      }

    val selectAsTree = q"$dataset.select(..$orderedColumns).as[$bType]"

    c.Expr[Dataset[B]](selectAsTree)
  }

  def transformTo[B: c.WeakTypeTag](c: blackbox.Context)(fields: c.Expr[_ => org.apache.spark.sql.TypedColumn[B, _]]*): c.Expr[TypedColumn[Any, B]] = {
    import c.universe._

    // Extract case class fields of B in constructor order
    val bType   = weakTypeOf[B]
    val bFields = bType.decls.collect { case m: MethodSymbol if m.isCaseAccessor => (m.name.decodedName.toString, m.returnType) }.toList

    // Validate column count
    val expectedCount = bFields.length
    val actualCount   = fields.length
    if (actualCount != expectedCount)
      c.abort(c.enclosingPosition, s"Expected $expectedCount fields for transformTo[$bType] but found $actualCount")

    // Extract (column name, result type, column expr) from each (_ => TypedColumn[B, X]) expression
    val columnDetails = fields.toList.map { colExpr =>
      val columnType = c.typecheck(colExpr.tree).tpe
      columnType match {
        // Match [TypedColumn[_, _] => TypedColumn[_, resultType]] to extract column type
        case TypeRef(_, _, List(_, TypeRef(_, _, List(_, resultType)))) =>
          // Extract the column name from the .as("name") alias
          val name = colExpr.tree match {
            case q"(($_) => ($_.as($alias).as[$_]($_): $_))" =>
              alias match {
                case Literal(Constant(name: String)) => name
                case _                               => c.abort(alias.pos, s"Column alias must be a literal string")
              }
            case _ => c.abort(colExpr.tree.pos, s"""TypedColumn must include .as("name") to match fields by name""")
          }

          (name, resultType, colExpr)
        case _ => c.abort(colExpr.tree.pos, s"Expected type TypedColumn[$bType, _] but found type $columnType")
      }
    }

    // Collect columns in case class constructor order of B
    // Validate that every field in B appears exactly once and that the matching column has the correct type
    val typedColumn    = extractTypedColumn(c)(c.prefix)
    val orderedColumns =
      bFields.map { case (fieldName, fieldType) =>
        columnDetails.filter { case (columnName, _, _) => columnName == fieldName } match {
          // One match
          case (_, resultType, colExpr) :: Nil =>
            // Validate type
            if (resultType <:< fieldType) {
              // Apply column function to column to get the field
              q"$colExpr.apply($typedColumn)"
            } else {
              c.abort(colExpr.tree.pos, s"Column for field '$fieldName' has type $resultType but expected type $fieldType")
            }
          // Multiple matches
          case (_, _, colExpr) :: tail => c.abort(colExpr.tree.pos, s"Column for field '$fieldName' occurs more than once (${tail.length} extra times)")
          // No matches
          case Nil => c.abort(c.enclosingPosition, s"Missing column for field '$fieldName' of type $fieldType")
        }
      }

    val structAsTree = q"_root_.org.apache.spark.sql.functions.struct(..$orderedColumns).as[$bType]"

    c.Expr[TypedColumn[Any, B]](structAsTree)
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
