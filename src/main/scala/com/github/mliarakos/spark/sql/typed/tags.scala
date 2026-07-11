package com.github.mliarakos.spark.sql.typed

import scala.language.experimental.macros

object tags {
  sealed trait NameTag[N <: String]
  type Tagged[V, N <: String] = V with NameTag[N]

  /** Tag the value with the name from the provided string */
  def tag[V](value: V, name: String): Tagged[V, _ <: String] = macro TypedColumnOpsMacroImpl2.tag[V]

  /** Tag the value with the name from the provided name type */
  def tag[V, N <: String](value: V): Tagged[V, N] = value.asInstanceOf[Tagged[V, N]]

  /** Remove the name tag from the value */
  def untag[V, N <: String](value: Tagged[V, N]): V = value.asInstanceOf[V]

  /** Get the name from the tagged value */
  def tagOf[V, N <: String: NameOf](value: Tagged[V, N]): String = implicitly[NameOf[N]].name

  /** Get the name from the name type */
  def tagOf[N <: String: NameOf]: String = implicitly[NameOf[N]].name

  /** Type-class to get the name from a tagged value */
  final class NameOf[A](val name: String)

  object NameOf {
    implicit def nameOf[N <: String]: NameOf[N] = macro TypedColumnOpsMacroImpl2.nameOf[N]
  }
}
