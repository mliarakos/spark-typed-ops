package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql._

import scala.collection.immutable._
import scala.language.experimental.macros

object ops {

  /**
    * Returns the name of a field.
    *
    * Uses a macro to rewrite the statement:
    * {{{
    *   nameFrom[User](_.name)
    *   "name"
    * }}}
    */
  def nameFrom[A <: Product](name: A => Any): String = macro TypedOpsImpl.name

  /**
    * Returns a sequence of field names.
    *
    * Uses a macro to rewrite the statement:
    * {{{
    *   namesFrom[User](_.id, _.name)
    *   Seq("id", "name")
    * }}}
    */
  def namesFrom[A <: Product](names: A => Any*): Seq[String] = macro TypedOpsImpl.names

  /**
    * Returns a [[Column]] based on the given field.
    *
    * Uses a macro to rewrite the statement:
    * {{{
    *   colFrom[User](_.name)
    *   col("name")
    * }}}
    *
    * @see [[org.apache.spark.sql.functions.col]] for full Spark usage.
    */
  def colFrom[A <: Product](col: A => Any): Column = macro TypedOpsImpl.column

  /**
    * Returns a sequence of [[Column]]s based on the given fields.
    *
    * Uses a macro to rewrite the statement:
    * {{{
    *   colsFrom[User](_.id, _.name)
    *   Seq(col("id"), col("name"))
    * }}}
    *
    * @see [[org.apache.spark.sql.functions.col]] for full Spark usage.
    */
  def colsFrom[A <: Product](cols: A => Any*): Seq[Column] = macro TypedOpsImpl.columns

  /**
    * Returns a [[Column]] based on the given field.
    *
    * Uses a macro to rewrite the statement:
    * {{{
    *   $[User](_.name)
    *   col("name")
    * }}}
    *
    * @see [[org.apache.spark.sql.functions.col]] for full Spark usage.
    */
  def $[A <: Product](col: A => Any): Column = macro TypedOpsImpl.column

  sealed trait typedColFrom[A <: Product] {
    def apply[B](col: A => B): TypedColumn[A, B] = macro TypedOpsImpl.typedColumn[A, B]
  }

  object typedColFrom {

    /**
      * Returns a [[TypedColumn]] based on the given field.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   typedColFrom[User](_.name)
      *   col("name").as[String]
      * }}}
      *
      * @see [[org.apache.spark.sql.functions.col]] and [[org.apache.spark.sql.Column.as]] for full Spark usage.
      */
    def apply[A <: Product]: typedColFrom[A] = new typedColFrom[A] {}
  }

  /**
    * Type-safe extension methods for [[Dataset]]s.
    */
  implicit class DatasetOps[A <: Product](val ds: Dataset[A]) extends AnyVal with Serializable {

    /**
      * Get the name of a Dataset field.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.nameFrom(_.name)
      *   "name"
      * }}}
      */
    def nameFrom(name: A => Any): String = macro TypedOpsImpl.name

    /**
      * Get the names of Dataset fields.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.namesFrom(_.id, _.name)
      *   Seq("id", "name")
      * }}}
      */
    def namesFrom(names: A => Any*): Seq[String] = macro TypedOpsImpl.names

    /**
      * Select a column based on the field and return it as a [[Column]].
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.colFrom(_.name)
      *   ds.col("name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.col]] for full Spark usage.
      */
    def colFrom(col: A => Any): Column = macro TypedOpsImpl.datasetColumn

    /**
      * Select columns based on the fields and return them as a sequence of [[Column]]s.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.colsFrom(_.id, _.name)
      *   Seq(ds.col("id"), ds.col("name"))
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.col]] for full Spark usage.
      */
    def colsFrom(cols: A => Any*): Seq[Column] = macro TypedOpsImpl.datasetColumns

    /**
      * Dataset `cube` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.cubeFrom(_.id, _.name)
      *   ds.cube("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.cube]] for full Spark usage.
      */
    def cubeFrom(cols: A => Any*): RelationalGroupedDataset = macro TypedOpsImpl.datasetCube

    /**
      * Dataset `describe` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.describeFrom(_.id, _.name)
      *   ds.describe("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.describe]] for full Spark usage.
      */
    def describeFrom(cols: A => Any*): DataFrame = macro TypedOpsImpl.datasetDescribe

    /**
      * Dataset `drop` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.dropFrom(_.id, _.name)
      *   ds.drop("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.drop]] for full Spark usage.
      */
    def dropFrom(cols: A => Any*): DataFrame = macro TypedOpsImpl.datasetDrop

    /**
      * Dataset `dropDuplicates` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.dropDuplicatesFrom(_.id, _.name)
      *   ds.dropDuplicates("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.dropDuplicates]] for full Spark usage.
      */
    def dropDuplicatesFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetDropDuplicates[A]

    /**
      * Dataset `groupBy` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.groupByFrom(_.id, _.name)
      *   ds.groupBy("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.groupBy]] for full Spark usage.
      */
    def groupByFrom(cols: A => Any*): RelationalGroupedDataset = macro TypedOpsImpl.datasetGroupBy

    /**
      * Dataset `rollup` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.rollupFrom(_.id, _.name)
      *   ds.rollup("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.rollup]] for full Spark usage.
      */
    def rollupFrom(cols: A => Any*): RelationalGroupedDataset = macro TypedOpsImpl.datasetRollup

    /**
      * Dataset `withColumnRenamed` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.withColumnRenamedFrom(_.id, "recordId")
      *   ds.withColumnRenamed("id", "recordId")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.withColumnRenamed]] for full Spark usage.
      */
    def withColumnRenamedFrom(existingName: A => Any, newName: String): DataFrame =
      macro TypedOpsImpl.datasetWithColumnRenamed

    /**
      * Dataset `orderBy` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.orderByFrom(_.id, _.name)
      *   ds.orderBy("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.orderBy]] for full Spark usage.
      */
    def orderByFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetOrderBy[A]

    /**
      * Dataset `select` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.selectFrom(_.id, _.name)
      *   ds.select(ds.col("id"), ds.col("name"))
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.select]] for full Spark usage.
      */
    def selectFrom(cols: A => Any*): DataFrame = macro TypedOpsImpl.datasetSelect

    /**
      * Dataset `sort` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.sortFrom(_.id, _.name)
      *   ds.sort("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.sort]] for full Spark usage.
      */
    def sortFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetSort[A]

    /**
      * Dataset `sortWithinPartitions` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.sortWithinPartitionsFrom(_.id, _.name)
      *   ds.sortWithinPartitions("id", "name")
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.sortWithinPartitions]] for full Spark usage.
      */
    def sortWithinPartitionsFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetSortWithinPartitions[A]

    /**
      * Dataset typed `select` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.selectFromTyped(_.id, _.name)
      *   ds.select(ds.col("id").as[Int], ds.col("name").as[String])
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.select]] for full Spark usage.
      */
    def selectFromTyped[B1](c1: A => B1): Dataset[B1] =
      macro TypedOpsImpl.datasetSelectTyped1[B1]

    /**
      * Dataset typed `select` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.selectFromTyped(_.id, _.name)
      *   ds.select(ds.col("id").as[Int], ds.col("name").as[String])
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.select]] for full Spark usage.
      */
    def selectFromTyped[B1, B2](c1: A => B1, c2: A => B2): Dataset[(B1, B2)] =
      macro TypedOpsImpl.datasetSelectTyped2[B1, B2]

    /**
      * Dataset typed `select` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.selectFromTyped(_.id, _.name)
      *   ds.select(ds.col("id").as[Int], ds.col("name").as[String])
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.select]] for full Spark usage.
      */
    def selectFromTyped[B1, B2, B3](c1: A => B1, c2: A => B2, c3: A => B3): Dataset[(B1, B2, B3)] =
      macro TypedOpsImpl.datasetSelectTyped3[B1, B2, B3]

    /**
      * Dataset typed `select` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.selectFromTyped(_.id, _.name)
      *   ds.select(ds.col("id").as[Int], ds.col("name").as[String])
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.select]] for full Spark usage.
      */
    def selectFromTyped[B1, B2, B3, B4](
        c1: A => B1,
        c2: A => B2,
        c3: A => B3,
        c4: A => B4
    ): Dataset[(B1, B2, B3, B4)] =
      macro TypedOpsImpl.datasetSelectTyped4[B1, B2, B3, B4]

    /**
      * Dataset typed `select` method with type-safe column access.
      *
      * Uses a macro to rewrite the statement:
      * {{{
      *   ds.selectFromTyped(_.id, _.name)
      *   ds.select(ds.col("id").as[Int], ds.col("name").as[String])
      * }}}
      *
      * @see [[org.apache.spark.sql.Dataset.select]] for full Spark usage.
      */
    def selectFromTyped[B1, B2, B3, B4, B5](
        c1: A => B1,
        c2: A => B2,
        c3: A => B3,
        c4: A => B4,
        c5: A => B5
    ): Dataset[(B1, B2, B3, B4, B5)] =
      macro TypedOpsImpl.datasetSelectTyped5[B1, B2, B3, B4, B5]

    /**
      * Project the Dataset record type to another case class.
      *
      * The target case class must have a subset of the columns with the same names and types in any order. These
      * conditions are verified at compile time.
      */
    def project[B <: Product](implicit p: Projection[A, B]): Dataset[B] = p.apply(ds)

  }

}
