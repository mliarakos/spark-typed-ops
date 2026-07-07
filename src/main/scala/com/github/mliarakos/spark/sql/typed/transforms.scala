package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.{functions => F}
import com.github.mliarakos.spark.sql.typed.{functions => TypedF}

import scala.annotation.tailrec
import scala.collection.immutable._
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.runtime.universe._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

object transforms {

  implicit final private[typed] class TypedColumnEncoderOps[A, B](val column: TypedColumn[A, B]) extends AnyVal {
    def encoder: Encoder[B] = TypedColumnUtil.getEncoder(column)
  }

  implicit def typedLiteralConversion[A: Encoder: TypeTag](value: A): TypedColumn[Any, A] = TypedF.lit[A](value)

  /** Type-safe transformation extension methods for [[Dataset]]s. */
  implicit final class DatasetTransformTypedOps[A <: Product](val ds: Dataset[A]) extends AnyVal {

    /** Get a [[TypedColumn]] based on the selected field
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.column(_.name)
      *   ds.col("name").as[String]
      * }}}
      */
    def column[B](selector: A => B): TypedColumn[A, B] = macro TypedColumnOpsMacroImpl.column[A, B]

    /** Transform the columns of this [[Dataset]] to map it to a new [[Dataset]] of the specified type
      *
      * The columns are validated to ensure they match the expected field names and types of the target type.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   inputs.transformTo[Output](
      *     _.column(_.id).renameTo[Output](_.recordId),
      *     _.column(_.start_date).renameTo[Output](_.startDate)
      *   )
      *   inputs.select(
      *     inputs("id").as[String].as("recordId").as[String],
      *     inputs("start_date").as[String].as("startDate").as[String]
      *   ).as[Output]
      * }}}
      */
    def transformTo[B <: Product](cols: Dataset[A] => TypedColumn[B, _]*): Dataset[B] = macro TypedColumnOpsMacroImpl.datasetTransformTo[B]
  }

  /** Type-safe transformation extension methods for [[DataFrame]]s. */
  implicit final class DataFrameTransformTypedOps(val df: DataFrame) extends AnyVal {

    /** Select [[TypedColumn]]s to map the [[DataFrame]] to a [[Dataset]] of the specified type
      *
      * The selected columns are validated to ensure they match the expected field names and types of the target type.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   inputs.selectTo[Output](
      *     typedColFrom[Input](_.id).renameTo[Output](_.recordId),
      *     typedColFrom[Input](_.start_date).renameTo[Output](_.startDate)
      *   )
      *   inputs.select(
      *     col("id").as[String].as("recordId").as[String],
      *     col("start_date").as[String].as("startDate").as[String]
      *   ).as[Output]
      * }}}
      */
    def selectTo[B](cols: TypedColumn[B, _]*): Dataset[B] = macro TypedColumnOpsMacroImpl.selectTo[B]
  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s. */
  implicit final class TypedColumnTransformTypedOps[Input, Field](val column: TypedColumn[Input, Field]) extends AnyVal {

    /** Get the name of this [[TypedColumn]] */
    def getName: String = {
      findColumnName(column.expr)
    }

    @tailrec private def findColumnName(expr: Expression): String = {
      expr match {
        case attr: UnresolvedAttribute         => attr.name
        case Alias(_, name)                    => name
        case NamedExpression(name, _)          => name
        case UnaryExpression(child)            => findColumnName(child)
        case expr if expr.children.length == 1 => findColumnName(expr.children.head)
        case other                             => throw new UnsupportedOperationException(s"Unsupported expression to get column name: $other")
      }
    }

    /** Rename this [[TypedColumn]] and maintain its type
      */
    def rename(name: String): TypedColumn[Input, Field] = column.as(name).asInstanceOf[TypedColumn[Input, Field]]

    /** Rename this [[TypedColumn]] based on the selected field of the specified type
      *
      * The type of the original column and the selected field must match.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   column.renameTo[Person](_.name)
      *   column.as("name").as[String]
      * }}}
      */
    def renameTo[From](selector: From => Field): TypedColumn[From, Field] = macro TypedColumnOpsMacroImpl.renameTo[From, Field]

    /** Transform this [[TypedColumn]] using the provided column transformation function into a new [[TypedColumn]]
      *
      * The column transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      *
      * These statements are equivalent:
      * {{{
      *   column.transform(col => upper(col).as[String])
      *   upper(column).as[String]
      * }}}
      */
    def transform[B](func: TypedColumn[Input, Field] => TypedColumn[Any, B]): TypedColumn[Any, B] = func(column)

    /** Transform this [[TypedColumn]] using a UDF of the provided function into a new [[TypedColumn]]
      *
      * The Scala function is converted to a UDF. The created UDFs are cached to prevent repeated creation of the same UDF. The type of the resulting column is
      * the same as the return type of the function.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   column.udfTransform(_.toUpperCase)
      *   udf((input: String) => input.toUpperCase).apply(column).as[String]
      * }}}
      */
    def udfTransform[B](func: Field => B): TypedColumn[Any, B] = macro TypedColumnOpsMacroImpl.udfTransform[Field, B]

  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s of [[Product]]s. */
  implicit final class TypedColumnProductTransformTypedOps[Input, Field <: Product](val column: TypedColumn[Input, Field]) extends AnyVal {

    /** Get a field as a [[TypedColumn]] based on the selected field
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   column.field(_.name)
      *   column.getField("name").as[String]
      * }}}
      */
    def field[B](selector: Field => B): TypedColumn[Field, B] = macro TypedColumnOpsMacroImpl.field[Field, B]

    /** Transform the fields of this [[TypedColumn]] to map it to a new [[TypedColumn]] of the specified type
      *
      * The fields are validated to ensure they match the expected field names and types of the target type. The name of the column is preserved.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   column.as("data").transformTo[Output](
      *     _.field(_.id).renameTo[Output](_.recordId),
      *     _.field(_.start_date).renameTo[Output](_.startDate)
      *   )
      *   struct(
      *    column.getField("id").as[String].as("recordId").as[String],
      *    column.getField("start_date").as[String].as("startDate").as[String],
      *   ).as("data").as[Output]
      * }}}
      */
    def transformTo[B <: Product](fields: TypedColumn[Input, Field] => TypedColumn[B, _]*): TypedColumn[Any, B] = macro TypedColumnOpsMacroImpl.transformTo[B]
  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s of [[Iterable]]s. */
  implicit final class TypedColumnSeqTransformTypedOps[Elem, Coll[T] <: Iterable[T]](val column: TypedColumn[_, Coll[Elem]]) extends AnyVal {

    /** Flat-map the elements of the collection in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").flatMap(col => split(col, "-").as[Seq[String]]) // TypedColumn[_, String]
      *   flatten(transform(column.as("data"), col => split(col, "-"))).as("data").as[String]
      * }}}
      */
    def flatMap[B, F[_]](
        func: TypedColumn[_, Elem] => TypedColumn[_, F[B]]
    )(implicit flattener: SparkFlattener[F[B]], enc1: Encoder[Elem], enc2: Encoder[Coll[B]]): TypedColumn[Any, Coll[B]] = {
      val name = column.getName
      flattener.flatten(F.transform(column, col => func(col.as[Elem]))).as(name).as[Coll[B]]
    }

    /** Flat-map the elements of the collection in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF. The created UDFs are cached to prevent repeated creation of the same UDF. The type of the resulting element is
      * the same as the element type of the return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").udfFlatMap(_.split("-")) // TypedColumn[_, String]
      *   flatten(transform(column.as("data"), col => udf((input: String) => input.split("-")).apply(col))).as("data").as[String]
      * }}}
      */
    def udfFlatMap[B, F[_]](
        func: Elem => F[B]
    )(implicit flattener: SparkFlattener[F[B]], tag1: TypeTag[Elem], tag2: TypeTag[F[B]], enc2: Encoder[Seq[B]]): TypedColumn[Any, Seq[B]] = {
      val name      = column.getName
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      flattener.flatten(F.transform(column, col => cachedUdf.apply(col))).as(name).as[Seq[B]]
    }

    /** Map the elements of the collection in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").map(col => upper(col).as[String])
      *   transform(column.as("data"), col => upper(col)).as("data").as[Seq[String]]
      * }}}
      */
    def map[B](func: TypedColumn[_, Elem] => TypedColumn[_, B])(implicit enc1: Encoder[Elem], enc2: Encoder[Coll[B]]): TypedColumn[Any, Coll[B]] = {
      val name = column.getName
      F.transform(column, col => func(col.as[Elem])).as(name).as[Coll[B]]
    }

    /** Map the elements of the collection in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF. The created UDFs are cached to prevent repeated creation of the same UDF. The type of the resulting element is
      * the same as the return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").map(_.toUppercase)
      *   transform(column.as("data"), col => udf((input: String) => input.toUpperCase).apply(col)).as("data").as[Seq[String]]
      * }}}
      */
    def udfMap[B: TypeTag](func: Elem => B)(implicit tag: TypeTag[Elem], enc: Encoder[Coll[B]]): TypedColumn[Any, Coll[B]] = {
      val name      = column.getName
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      F.transform(column, col => cachedUdf.apply(col)).as(name).as[Coll[B]]
    }
  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s of [[Iterable]]s. */
  implicit final class TypedColumnOptionTransformTypedOps[Elem](val column: TypedColumn[_, Option[Elem]]) extends AnyVal {

    /** Flat-map the optional element in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").flatMap(col => split(col, "-").as[Seq[String]]) // TypedColumn[_, String]
      *   flatten(transform(column.as("data"), col => split(col, "-"))).as("data").as[String]
      * }}}
      */
    def flatMap[B](
        func: TypedColumn[_, Elem] => TypedColumn[_, Option[B]]
    )(implicit enc1: Encoder[Elem], enc2: Encoder[Option[B]]): TypedColumn[Any, Option[B]] = {
      val name = column.getName
      F.when(column.isNotNull, func(column.as[Elem])).as(name).as[Option[B]]
    }

    /** Flat-map the optional element in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF. The created UDFs are cached to prevent repeated creation of the same UDF. The type of the resulting element is
      * the same as the element type of the return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").udfFlatMap(_.split("-")) // TypedColumn[_, String]
      *   flatten(transform(column.as("data"), col => udf((input: String) => input.split("-")).apply(col))).as("data").as[String]
      * }}}
      */
    def udfFlatMap[B](
        func: Elem => Option[B]
    )(implicit tag1: TypeTag[Elem], tag2: TypeTag[Option[B]], enc2: Encoder[Option[B]]): TypedColumn[Any, Option[B]] = {
      val name      = column.getName
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      F.when(column.isNotNull, cachedUdf.apply(column)).as(name).as[Option[B]]
    }

    /** Flat-map the optional element in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").map(col => upper(col).as[String])
      *   transform(column.as("data"), col => upper(col)).as("data").as[Seq[String]]
      * }}}
      */
    def map[B](func: TypedColumn[_, Elem] => TypedColumn[_, B])(implicit enc1: Encoder[Elem], enc2: Encoder[Option[B]]): TypedColumn[Any, Option[B]] = {
      val name = column.getName
      F.when(column.isNotNull, func(column.as[Elem])).as(name).as[Option[B]]
    }

    /** Flat-map the optional element in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF. The created UDFs are cached to prevent repeated creation of the same UDF. The type of the resulting element is
      * the same as the return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").map(_.toUppercase)
      *   transform(column.as("data"), col => udf((input: String) => input.toUpperCase).apply(col)).as("data").as[Seq[String]]
      * }}}
      */
    def udfMap[B: TypeTag](func: Elem => B)(implicit tag: TypeTag[Elem], enc: Encoder[Option[B]]): TypedColumn[Any, Option[B]] = {
      val name      = column.getName
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      F.when(column.isNotNull, cachedUdf.apply(column)).as(name).as[Option[B]]
    }
  }

  implicit final class TypedColumnOptionSeqTypedOps[Input, Elem, Coll[T] <: Seq[T]](val column: TypedColumn[Input, Option[Coll[Elem]]]) extends AnyVal {

    /** Unwrap this [[TypedColumn]] containing an optional collection into the collection if defined or an empty collection if not
      *
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").as[Option[Seq[String]]].orEmpty // TypedColumn[_, Seq[String]]
      *   coalesce(column.as("data"), array()).as("data").as[Seq[String]]
      * }}}
      */
    def orEmpty(implicit enc: Encoder[Coll[Elem]]): TypedColumn[Input, Coll[Elem]] = {
      val name = column.getName
      F.coalesce(column, F.array()).as(name).as[Coll[Elem]]
    }
  }

  sealed trait SparkFlattener[-T] {
    def flatten(col: Column): Column
  }

  object SparkFlattener {
    def apply[T](implicit flatten: SparkFlattener[T]): SparkFlattener[T] = flatten

    implicit def optionFlattener[A]: SparkFlattener[Option[A]] = new SparkFlattener[Option[A]] {
      def flatten(col: Column): Column = F.array_compact(col)
    }

    implicit def seqFlattener[A]: SparkFlattener[scala.Seq[A]] = new SparkFlattener[scala.Seq[A]] {
      def flatten(col: Column): Column = F.flatten(col)
    }
  }

}
