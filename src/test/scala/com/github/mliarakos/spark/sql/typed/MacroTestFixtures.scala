package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

trait MacroTestFixtures { _: Matchers with SparkMatchers =>
  def validate[A: ClassTag](ds: Dataset[A], expected: Dataset[A]): Unit = {
    ds shouldNot useObjectSerialization
    ds.columns shouldBe expected.columns
    ds shouldEqual expected
  }
}
