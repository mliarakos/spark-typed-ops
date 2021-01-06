package com.github.mliarakos.spark.sql.typed

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}
import shapeless.test.illTyped

import scala.collection.immutable._
import scala.reflect.ClassTag

case class Big(id: String, name: String, age: Int, pets: Seq[String])
case class Reorder(age: Int, pets: Seq[String], name: String, id: String)
case class Small(name: String, age: Int)
case class SmallReorder(age: Int, name: String)
case class Wrong(recordId: String, name: String, age: Int)

class ProjectionSpec extends FlatSpec with Matchers with SparkMatchers with DatasetSuiteBase {

  import ops._
  import spark.implicits._

  private val data = Seq(
    Big("1", "John", 18, Seq("dog", "cat")),
    Big("2", "Sally", 23, Seq("dog"))
  )

  private def validate[A: ClassTag](ds: Dataset[A], expected: Dataset[A]): Unit = {
    ds.columns shouldBe expected.columns
    ds shouldEqual expected
  }

  it should "project to itself" in {
    val ds  = data.toDS()
    val big = ds.project[Big]

    big shouldNot useObjectSerialization
    validate(big, ds.map(record => record))
    validate(big, ds.select("*").as[Big])
  }

  it should "project the same columns in a different order" in {
    val ds      = data.toDS()
    val reorder = ds.project[Reorder]

    reorder shouldNot useObjectSerialization
    validate(reorder, ds.map(record => Reorder(record.age, record.pets, record.name, record.id)))
    validate(reorder, ds.select("age", "pets", "name", "id").as[Reorder])
  }

  it should "project to fewer columns in the same order" in {
    val ds    = data.toDS()
    val small = ds.project[Small]

    small shouldNot useObjectSerialization
    validate(small, ds.map(record => Small(record.name, record.age)))
    validate(small, ds.select("name", "age").as[Small])
  }

  it should "project the fewer columns in a different order" in {
    val ds           = data.toDS()
    val smallReorder = ds.project[SmallReorder]

    smallReorder shouldNot useObjectSerialization
    validate(smallReorder, ds.map(record => SmallReorder(record.age, record.name)))
    validate(smallReorder, ds.select("age", "name").as[SmallReorder])
  }

  it should "not project invalid columns" in {
    val ds = data.toDS()
    illTyped { "ds.project[Wrong]" }
  }

}
