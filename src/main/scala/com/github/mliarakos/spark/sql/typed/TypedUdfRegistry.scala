package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe._

object TypedUdfRegistry {
  private[this] lazy val cache = new ConcurrentHashMap[(Type, Type, Int), UserDefinedFunction]()

  def getOrCreate[A: TypeTag, B: TypeTag](func: A => B): UserDefinedFunction = {
    val key = (typeOf[A], typeOf[B], System.identityHashCode(func))
    cache.computeIfAbsent(key, _ => udf(func))
  }
}
