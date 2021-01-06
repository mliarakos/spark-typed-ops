package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.SparkMatchers.SparkExecutionUsesObjectSerializationMatcher
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.scalactic.Equality
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.reflect.ClassTag
import scala.util.Try

trait SparkMatchers { _: DatasetSuiteBase =>

  implicit val columnEquality: Equality[Column] = new Equality[Column] {
    override def areEqual(a: Column, b: Any): Boolean = b match {
      case _: Column => a.toString == b.toString
      case _         => false
    }
  }

  implicit val dataFrameEquality: Equality[DataFrame] = new Equality[DataFrame] {
    override def areEqual(a: DataFrame, b: Any): Boolean = b match {
      case df: Dataset[Row @unchecked] => Try(assertDataFrameEquals(a, df)).map(_ => true).get
      case _                           => false
    }
  }

  implicit def datasetEquality[A: ClassTag]: Equality[Dataset[A]] = new Equality[Dataset[A]] {
    override def areEqual(a: Dataset[A], b: Any): Boolean = b match {
      case ds: Dataset[A @unchecked] => Try(assertDatasetEquals(a, ds)).map(_ => true).get
      case _                         => false
    }
  }

  def useObjectSerialization: Matcher[Dataset[_]] = SparkExecutionUsesObjectSerializationMatcher
}

object SparkMatchers {

  object SparkExecutionUsesObjectSerializationMatcher extends Matcher[Dataset[_]] {
    def apply(left: Dataset[_]): MatchResult = {
      val plan = left.queryExecution.executedPlan.toString
      MatchResult(
        plan.contains("DeserializeToObject") || plan.contains("SerializeFromObject"),
        "Spark execution plan did not use object serialization",
        "Spark execution plan used object serialization"
      )
    }
  }

}
