package org.apache.spark.sql

object TypedColumnUtil {
  def getEncoder[A, B](col: TypedColumn[A, B]): Encoder[B] = col.encoder
}
