package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql._

import scala.collection.immutable._
import scala.language.experimental.macros

object ops {

  def nameFrom[A <: Product](name: A => Any): String = macro TypedOpsImpl.name

  def namesFrom[A <: Product](names: A => Any*): Seq[String] = macro TypedOpsImpl.names

  def colFrom[A <: Product](col: A => Any): Column = macro TypedOpsImpl.column

  def colsFrom[A <: Product](cols: A => Any*): Seq[Column] = macro TypedOpsImpl.columns

  def $[A <: Product](col: A => Any): Column = macro TypedOpsImpl.column

  sealed trait typedColFrom[A <: Product] {
    def apply[B](col: A => B): TypedColumn[A, B] = macro TypedOpsImpl.typedColumn[A, B]
  }

  object typedColFrom {
    def apply[A <: Product]: typedColFrom[A] = new typedColFrom[A] {}
  }

  implicit class DatasetOps[A <: Product](val ds: Dataset[A]) extends AnyVal with Serializable {

    def nameFrom(name: A => Any): String = macro TypedOpsImpl.name

    def namesFrom(names: A => Any*): Seq[String] = macro TypedOpsImpl.names

    def colFrom(col: A => Any): Column = macro TypedOpsImpl.datasetColumn

    def colsFrom(cols: A => Any*): Seq[Column] = macro TypedOpsImpl.datasetColumns

    def cubeFrom(cols: A => Any*): RelationalGroupedDataset = macro TypedOpsImpl.datasetCube

    def describeFrom(cols: A => Any*): DataFrame = macro TypedOpsImpl.datasetDescribe

    def dropFrom(cols: A => Any*): DataFrame = macro TypedOpsImpl.datasetDrop

    def dropDuplicatesFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetDropDuplicates[A]

    def groupByFrom(cols: A => Any*): RelationalGroupedDataset = macro TypedOpsImpl.datasetGroupBy

    def rollupFrom(cols: A => Any*): RelationalGroupedDataset = macro TypedOpsImpl.datasetRollup

    def withColumnRenamedFrom(existingName: A => Any, newName: String): DataFrame =
      macro TypedOpsImpl.datasetWithColumnRenamed

    def orderByFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetSort[A]

    def selectFrom(cols: A => Any*): DataFrame = macro TypedOpsImpl.datasetSelect

    def sortFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetSort[A]

    def sortWithinPartitionsFrom(cols: A => Any*): Dataset[A] = macro TypedOpsImpl.datasetSortWithinPartitions[A]

    def selectFromTyped[B1](c1: A => B1): Dataset[B1] =
      macro TypedOpsImpl.datasetSelectTyped1[B1]

    def selectFromTyped[B1, B2](c1: A => B1, c2: A => B2): Dataset[(B1, B2)] =
      macro TypedOpsImpl.datasetSelectTyped2[B1, B2]

    def selectFromTyped[B1, B2, B3](c1: A => B1, c2: A => B2, c3: A => B3): Dataset[(B1, B2, B3)] =
      macro TypedOpsImpl.datasetSelectTyped3[B1, B2, B3]

    def selectFromTyped[B1, B2, B3, B4](
        c1: A => B1,
        c2: A => B2,
        c3: A => B3,
        c4: A => B4
    ): Dataset[(B1, B2, B3, B4)] =
      macro TypedOpsImpl.datasetSelectTyped4[B1, B2, B3, B4]

    def selectFromTyped[B1, B2, B3, B4, B5](
        c1: A => B1,
        c2: A => B2,
        c3: A => B3,
        c4: A => B4,
        c5: A => B5
    ): Dataset[(B1, B2, B3, B4, B5)] =
      macro TypedOpsImpl.datasetSelectTyped5[B1, B2, B3, B4, B5]

    def project[B <: Product](implicit p: Projection[A, B]): Dataset[B] = p.apply(ds)

  }

}
