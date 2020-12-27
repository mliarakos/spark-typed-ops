package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql.{Dataset, Encoder}
import shapeless.ops.{hlist, record}
import shapeless.{HList, LabelledGeneric}

import scala.annotation.implicitNotFound
import scala.collection.immutable._

@implicitNotFound(
  "Unable to project ${A} to ${B}, ensure that ${A} and ${B} are case classes and that the fields of ${B} are a subset of ${A}"
)
sealed trait Projection[A <: Product, B <: Product] {
  def apply(ds: Dataset[A]): Dataset[B]
}

object Projection {
  def apply[A <: Product, B <: Product](implicit projection: Projection[A, B]): Projection[A, B] = projection

  def createProjection[A <: Product, B <: Product](f: Dataset[A] => Dataset[B]): Projection[A, B] = {
    new Projection[A, B] {
      def apply(ds: Dataset[A]): Dataset[B] = f(ds)
    }
  }

  implicit def genericProjection[
      A <: Product,
      B <: Product,
      ARepr <: HList,
      BRepr <: HList,
      Common <: HList,
      KeysRepr <: HList
  ](
      implicit
      aGen: LabelledGeneric.Aux[A, ARepr],
      bGen: LabelledGeneric.Aux[B, BRepr],
      inter: hlist.Intersection.Aux[ARepr, BRepr, Common],
      align: hlist.Align[Common, BRepr],
      keys: record.Keys.Aux[BRepr, KeysRepr],
      traversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
      enc: Encoder[B]
  ): Projection[A, B] = {
    createProjection[A, B](ds => {
      val cols = keys().toList.map(symbol => ds.col(symbol.name))
      ds.select(cols: _*).as[B]
    })
  }
}
