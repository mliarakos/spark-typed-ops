package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.transforms.TypedColumnEncoderOps
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.{functions => F}

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag
import scala.language.experimental.macros

object functions {

  def concat(exprs: TypedColumn[_, String]*)(implicit enc: Encoder[String]): TypedColumn[Any, String] = {
    F.concat(exprs: _*).as[String]
  }

  def lit[A: Encoder: TypeTag](literal: A): TypedColumn[Any, A] = {
    F.typedlit[A](literal).as[A]
  }

  def lower(e: TypedColumn[_, String]): TypedColumn[Any, String] = {
    F.lower(e).as[String](e.encoder)
  }

  def replace(src: TypedColumn[_, String], search: TypedColumn[_, String]): TypedColumn[Any, String] = {
    F.replace(src, search).as[String](src.encoder)
  }

  def replace(src: TypedColumn[_, String], search: TypedColumn[_, String], replace: TypedColumn[_, String]): TypedColumn[Any, String] = {
    F.replace(src, search, replace).as[String](src.encoder)
  }

  def split(e: TypedColumn[_, String], pattern: String)(implicit enc: Encoder[Seq[String]]): TypedColumn[Any, Seq[String]] = {
    F.split(e, pattern).as[Seq[String]]
  }

  // def struct[A <: Product: Encoder](cols: TypedColumn[_, _]*): TypedColumn[Any, A] = F.struct(cols: _*).as[A]

  def struct[A <: Product](cols: TypedColumn[_, _]*): TypedColumn[Any, A] = macro TypedColumnOpsMacroImpl.struct[A]

  def transform[A: Encoder, B, F[T] <: Seq[T]](
      e: TypedColumn[_, F[A]],
      f: TypedColumn[_, A] => TypedColumn[_, B]
  )(implicit enc: Encoder[F[B]]): TypedColumn[Any, F[B]] = {
    F.transform(e, col => f(col.as[A])).as[F[B]]
  }

  def upper(e: TypedColumn[_, String]): TypedColumn[Any, String] = {
    F.upper(e).as[String](e.encoder)
  }

}
