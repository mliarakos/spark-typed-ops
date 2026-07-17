package com.github.mliarakos.spark.sql.typed

import scala.annotation.unused
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
  def tagOf[V, N <: String: TaggedWith](@unused value: Tagged[V, N]): String = implicitly[TaggedWith[N]].name

  /** Get the name from the name type */
  def tagOf[N <: String: TaggedWith]: String = implicitly[TaggedWith[N]].name

  /** Type-class to get the name from a tagged value */
  final class TaggedWith[A](val name: String)

  object TaggedWith {
    def apply[N <: String](implicit taggedWith: TaggedWith[N]): TaggedWith[N] = taggedWith
    implicit def taggedWith[N <: String]: TaggedWith[N] = macro TypedColumnOpsMacroImpl2.taggedWith[N]
  }
}
