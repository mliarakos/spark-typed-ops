package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.tags._
import com.github.mliarakos.spark.sql.typed.{functions => TypedF}
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.TypedColumnUtil
import org.apache.spark.sql.{functions => F}

import scala.language.experimental.macros
import scala.language.higherKinds
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

object transforms {

  implicit final private[typed] class TypedColumnEncoderOps[A, B](val column: TypedColumn[A, B]) extends AnyVal {
    def encoder: Encoder[B] = TypedColumnUtil.getEncoder(column)
  }

  implicit def typedLiteralConversion[A: Encoder: TypeTag](value: A): TypedColumn[Any, A] = TypedF.lit[A](value)

  /** Type-safe transformation extension methods for [[Dataset]]s. */
  implicit final class DatasetTransformTypedOps2[A <: Product](val ds: Dataset[A]) extends AnyVal {

    /** Get a [[TypedColumn]] based on the selected field
      *
      * These statements are equivalent:
      * {{{
      *   ds.column(_.name)
      *   ds.col("name").as[String]
      * }}}
      */
    def column[B](selector: A => B): TypedColumn[A, B] = macro TypedColumnOpsMacroImpl.column[A, B]

    /** Transform the columns of this [[Dataset]] into a new [[Dataset]] of the specified type
      *
      * The columns are validated to ensure they match the expected field names and types of the target type.
      *
      * These statements are equivalent:
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
    def transformTo[B <: Product](cols: Dataset[A] => Tagged[TypedColumn[_, _], _]*): Dataset[B] = macro TypedColumnOpsMacroImpl.datasetTransformTo[B]
  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s. */
  implicit final class TypedColumnTransformTypedOps[Input, Field](val column: TypedColumn[Input, Field]) extends AnyVal {

    /** Tag this [[TypedColumn]] with the name from the given name type */
    private[typed] def tagWith[N <: String]: Tagged[TypedColumn[Input, Field], N] = tag[TypedColumn[Input, Field], N](column)

    /** Rename this [[TypedColumn]] while maintaining its type
      */
    def rename(name: String): TypedColumn[Input, Field] = macro TypedColumnOpsMacroImpl.rename[Input, Field]

    /** Rename this [[TypedColumn]] based on the selected field of the specified type
      *
      * The type of the original column and the selected field must match.
      *
      * These statements are equivalent:
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
    def transform[A, B](func: TypedColumn[Input, Field] => TypedColumn[A, B]): TypedColumn[A, B] = func(column)

  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s of [[Product]]s. */
  implicit final class TypedColumnProductTransformTypedOps[Input, Field <: Product](val column: TypedColumn[Input, Field]) extends AnyVal {

    /** Get a field as a [[TypedColumn]] based on the selected field
      *
      * These statements are equivalent:
      * {{{
      *   column.field(_.name)
      *   column.getField("name").as[String]
      * }}}
      */
    def field[B](selector: Field => B): TypedColumn[Field, B] = macro TypedColumnOpsMacroImpl.field[Field, B]

  }

  /** Type-safe transformation extension methods for tagged [[TypedColumn]]s. */
  implicit final class TaggedTypedColumnTransformTypedOps[Input, Field, Name <: String](val column: Tagged[TypedColumn[Input, Field], Name]) extends AnyVal {

    /** Get the name of this [[TypedColumn]] */
    def getName(implicit tag: TaggedWith[Name]): String = tag.name

    /** Transform this [[TypedColumn]] using the provided column transformation function into a new [[TypedColumn]]
      *
      * The column transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").transform(col => upper(col).as[String])
      *   upper(column).as("data").as[String]
      * }}}
      */
    def transform[B](func: TypedColumn[Input, Field] => TypedColumn[_, B])(implicit taggedWith: TaggedWith[Name]): Tagged[TypedColumn[Any, B], Name] = {
      val result = func(column)
      result.as(taggedWith.name).as[B](result.encoder).tagWith[Name]
    }

    /** Transform this [[TypedColumn]] using a UDF of the provided function into a new [[TypedColumn]]
      *
      * The Scala function is converted to a UDF and cached to prevent repeated creation of the same UDF. The type of the resulting column is the same as the
      * return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").udfTransform(_.toUpperCase)
      *   udf((input: String) => input.toUpperCase).apply(column).as("data").as[String]
      * }}}
      */
    def udfTransform[B](func: Field => B): Tagged[TypedColumn[Any, B], Name] = macro TypedColumnOpsMacroImpl.udfTransform[Field, B, Name]

    /** Transform the fields of this [[TypedColumn]] into a new [[TypedColumn]] of the specified type
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
    def transformTo[B <: Product](fields: TypedColumn[Input, Field] => Tagged[TypedColumn[_, _], _]*): Tagged[TypedColumn[Any, B], Name] =
      macro TypedColumnOpsMacroImpl.columnTransformTo[B, Name]

  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s of [[Iterable]]s. */
  implicit final class TypedColumnSeqTransformTypedOps[Elem, Coll[T] <: Iterable[T], Name <: String](val column: Tagged[TypedColumn[_, Coll[Elem]], Name])
      extends AnyVal {

    /** Flat-map the elements of the collection in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").flatMap(col => split(col, "-").as[Seq[String]]) // TypedColumn[_, Seq[String]]
      *   flatten(transform(column.as("data"), col => split(col, "-"))).as("data").as[Seq[String]]
      * }}}
      */
    def flatMap[B, F[_]](
        func: TypedColumn[_, Elem] => TypedColumn[_, F[B]]
    )(implicit
        flattener: SparkFlattener[F[B]],
        enc1: Encoder[Elem],
        enc2: Encoder[Coll[B]],
        taggedWith: TaggedWith[Name]
    ): Tagged[TypedColumn[Any, Coll[B]], Name] = {
      flattener.flatten(F.transform(column, col => func(col.as[Elem]))).as(taggedWith.name).as[Coll[B]].tagWith[Name]
    }

    /** Flat-map the elements of the collection in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF and cached to prevent repeated creation of the same UDF. The type of the resulting element is the same as the
      * element type of the return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").udfFlatMap(_.split("-")) // TypedColumn[_, Seq[String]]
      *   flatten(transform(column), col => udf((input: String) => input.split("-")).apply(col))).as("data").as[Seq[String]]
      * }}}
      */
    def udfFlatMap[B, F[_]](
        func: Elem => F[B]
    )(implicit
        flattener: SparkFlattener[F[B]],
        tag1: TypeTag[Elem],
        tag2: TypeTag[F[B]],
        enc2: Encoder[Coll[B]],
        taggedWith: TaggedWith[Name]
    ): Tagged[TypedColumn[Any, Coll[B]], Name] = {
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      flattener.flatten(F.transform(column, col => cachedUdf.apply(col))).as(taggedWith.name).as[Coll[B]].tagWith[Name]
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
      *   transform(column, col => upper(col)).as("data").as[Seq[String]]
      * }}}
      */
    def map[B](
        func: TypedColumn[_, Elem] => TypedColumn[_, B]
    )(implicit enc1: Encoder[Elem], enc2: Encoder[Coll[B]], taggedWith: TaggedWith[Name]): Tagged[TypedColumn[Any, Coll[B]], Name] = {
      F.transform(column, col => func(col.as[Elem])).as(taggedWith.name).as[Coll[B]].tagWith[Name]
    }

    /** Map the elements of the collection in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF and cached to prevent repeated creation of the same UDF. The type of the resulting element is the same as the
      * return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").map(_.toUppercase)
      *   transform(column, col => udf((input: String) => input.toUpperCase).apply(col)).as("data").as[Seq[String]]
      * }}}
      */
    def udfMap[B: TypeTag](
        func: Elem => B
    )(implicit tag: TypeTag[Elem], enc: Encoder[Coll[B]], taggedWith: TaggedWith[Name]): Tagged[TypedColumn[Any, Coll[B]], Name] = {
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      F.transform(column, col => cachedUdf.apply(col)).as(taggedWith.name).as[Coll[B]].tagWith[Name]
    }
  }

  /** Type-safe transformation extension methods for [[TypedColumn]]s of [[Option]]s. */
  implicit final class TaggedTypedColumnOptionTransformTypedOps[Elem, Name <: String](val column: Tagged[TypedColumn[_, Option[Elem]], Name]) extends AnyVal {

    /** Flat-map the optional element in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").flatMap(col => when(col === "target", col).as[Option[String]]) // TypedColumn[_, Option[String]]
      *   when(column.isNotNull, when(column === "target", column)).as("data").as[Option[String]]
      * }}}
      */
    def flatMap[B](
        func: Tagged[TypedColumn[_, Elem], Name] => TypedColumn[_, Option[B]]
    )(implicit enc1: Encoder[Elem], enc2: Encoder[Option[B]], taggedWith: TaggedWith[Name]): Tagged[TypedColumn[Any, Option[B]], Name] = {
      F.when(column.isNotNull, func(column.as[Elem].tagWith[Name])).as(taggedWith.name).as[Option[B]].tagWith[Name]
    }

    /** Flat-map the optional element in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF and cached to prevent repeated creation of the same UDF. The type of the resulting element is the same as the
      * element type of the return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").udfFlatMap(value => if (value == "target") Some(value) else None) // TypedColumn[_, Option[String]]
      *   when(column.isNotNull, udf((input: String) => if (value == "target") Some(value) else None).apply(column)).as("data").as[Option[String]]
      * }}}
      */
    def udfFlatMap[B](
        func: Elem => Option[B]
    )(implicit tt1: TypeTag[Elem], tt2: TypeTag[Option[B]], enc: Encoder[Option[B]], tag: TaggedWith[Name]): Tagged[TypedColumn[Any, Option[B]], Name] = {
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      F.when(column.isNotNull, cachedUdf.apply(column)).as(tag.name).as[Option[B]].tagWith[Name]
    }

    /** Map the optional element in this [[TypedColumn]] using the provided transformation function
      *
      * The transformation function must return a [[TypedColumn]]. If using untyped [[Column]]s or functions the result must be cast using `.as[Type]`.
      * Alternatively, the typed functions in `com.github.mliarakos.spark.sql.typed.funcions` can be used as typed equivalents of the built-in Spark functions.
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").map(col => upper(col).as[String])
      *   when(column.isNotNull, upper(column)).as("data").as[Option[String]]
      * }}}
      */
    def map[B](
        func: Tagged[TypedColumn[_, Elem], Name] => TypedColumn[_, B]
    )(implicit enc1: Encoder[Elem], enc2: Encoder[Option[B]], tag: TaggedWith[Name]): Tagged[TypedColumn[Any, Option[B]], Name] = {
      F.when(column.isNotNull, func(column.as[Elem].tagWith[Name])).as(tag.name).as[Option[B]].tagWith[Name]
    }

    /** Map the optional element in this [[TypedColumn]] using a UDF of the provided transformation function
      *
      * The Scala function is converted to a UDF and cached to prevent repeated creation of the same UDF. The type of the resulting element is the same as the
      * return type of the function. The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").udfMap(_.toUppercase)
      *   when(column.isNotNull, udf((input: String) => input.toUpperCase).apply(column)).as("data").as[Option[String]]
      * }}}
      */
    def udfMap[B: TypeTag](
        func: Elem => B
    )(implicit tt: TypeTag[Elem], enc: Encoder[Option[B]], tag: TaggedWith[Name]): Tagged[TypedColumn[Any, Option[B]], Name] = {
      val cachedUdf = TypedUdfRegistry.getOrCreate(func)
      F.when(column.isNotNull, cachedUdf.apply(column)).as(tag.name).as[Option[B]].tagWith[Name]
    }

  }

  implicit final class TaggedTypedColumnOptionSeqTypedOps[Input, Elem, Coll[T] <: Seq[T], Name <: String](
      val column: Tagged[TypedColumn[Input, Option[Coll[Elem]]], Name]
  ) extends AnyVal {

    /** Unwrap this [[TypedColumn]] containing an optional collection into the collection if defined or an empty collection if not
      *
      * The name of the column is preserved.
      *
      * These statements are equivalent:
      * {{{
      *   column.as("data").as[Option[Seq[String]]].orEmpty // TypedColumn[_, Seq[String]]
      *   coalesce(column, array()).as("data").as[Seq[String]]
      * }}}
      */
    def orEmpty(implicit enc: Encoder[Coll[Elem]], taggedWith: TaggedWith[Name]): Tagged[TypedColumn[Input, Coll[Elem]], Name] = {
      F.coalesce(column, F.array()).as(taggedWith.name).as[Coll[Elem]].tagWith[Name]
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
