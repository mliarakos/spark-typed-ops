package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.MacroTestFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class MacroSpec extends FlatSpec with Matchers with SparkMatchers with DatasetSuiteBase with MacroTestFixtures {

  import ops._
  import spark.implicits._

  it should "get a name" in {
    nameFrom[Person](_.id) shouldBe "id"
    nameFrom[Person](_.address.street) shouldBe "address.street"
  }

  it should "get names" in {
    namesFrom[Person](_.id, _.address.street) shouldBe Seq("id", "address.street")
  }

  it should "get a name from a dataset" in {
    val people = peopleData.toDS()
    people.nameFrom(_.id) shouldBe "id"
    people.nameFrom(_.address.street) shouldBe "address.street"
  }

  it should "get names from a dataset" in {
    val people = peopleData.toDS()
    people.namesFrom(_.id, _.address.street) shouldBe Seq("id", "address.street")
  }

  it should "create a column" in {
    colFrom[Person](_.id) shouldEqual col("id")
    colFrom[Person](_.address.street) shouldEqual col("address.street")
  }

  it should "create columns" in {
    colsFrom[Person](_.id, _.address.street) shouldEqual Seq(col("id"), col("address.street"))
  }

  it should "create a column using $" in {
    $[Person](_.id) shouldEqual col("id")
    $[Person](_.id) shouldEqual $"id"
    $[Person](_.address.street) shouldEqual col("address.street")
    $[Person](_.address.street) shouldEqual $"address.street"
  }

  it should "create a column from a dataset" in {
    val people = peopleData.toDS()
    people.colFrom(_.id) shouldEqual people("id")
    people.colFrom(_.address.street) shouldEqual people("address.street")
  }

  it should "create columns from a dataset" in {
    val people  = peopleData.toDS()
    val columns = people.colsFrom(_.id, _.address.street)
    (columns should contain).theSameElementsInOrderAs(Seq(people("id"), people("address.street")))
  }

  it should "cube columns on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.cubeFrom(_.id, _.address.street).count()
    validate(df, people.cube("id", "address.street").count())
  }

  it should "describe columns on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.describeFrom(_.id, _.address.street)
    validate(df, people.describe("id", "address.street"))
  }

  it should "drop columns on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.dropFrom(_.id, _.address.street)
    validate(df, people.drop("id", "address.street"))
  }

  it should "drop duplicate columns on a dataset" in {
    val people = peopleData.toDS()
    val ds     = people.dropDuplicatesFrom(_.id, _.address)
    validate(ds, people.dropDuplicates("id", "address"))
  }

  it should "group columns on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.groupByFrom(_.id, _.address.street).count()
    validate(df, people.groupBy("id", "address.street").count())
  }

  it should "rollup columns on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.rollupFrom(_.id, _.address.street).count()
    validate(df, people.rollup("id", "address.street").count())
  }

  it should "select columns on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.selectFrom(_.id, _.address.street)
    validate(df, people.select(people("id"), people("address.street")))
  }

  it should "order columns on a dataset" in {
    val people = peopleData.toDS()
    val ds     = people.orderByFrom(_.id, _.address.street)
    validate(ds, people.orderBy("id", "address.street"))
  }

  it should "sort columns on a dataset" in {
    val people = peopleData.toDS()
    val ds     = people.sortFrom(_.id, _.address.street)
    validate(ds, people.sort("id", "address.street"))
  }

  it should "sort partitions on a dataset" in {
    val people = peopleData.toDS()
    val ds     = people.sortWithinPartitionsFrom(_.id, _.address.street)
    validate(ds, people.sortWithinPartitions("id", "address.street"))
  }

  it should "rename a column on a dataset" in {
    val people = peopleData.toDS()
    val df     = people.withColumnRenamedFrom(_.id, "recordId")
    validate(df, people.withColumnRenamed("id", "recordId"))
  }

  it should "create a typed column" in {
    typedColFrom[Person](_.id) shouldEqual col("id").as[String]
    typedColFrom[Person](_.age) shouldEqual col("age").as[Int]
    typedColFrom[Person](_.address.street) shouldEqual col("address.street").as[String]
  }

  it should "select typed columns on a dataset" in {
    val people = peopleData.toDS()

    val id      = people("id").as[String]
    val name    = people("name").as[String]
    val age     = people("age").as[Int]
    val address = people("address").as[Address]
    val street  = people("address.street").as[String]
    val city    = people("address.city").as[String]

    validate(
      people.selectFromTyped(_.id),
      people.select(id)
    )
    validate(
      people.selectFromTyped(_.id, _.name),
      people.select(id, name)
    )
    validate(
      people.selectFromTyped(_.id, _.name, _.age),
      people.select(id, name, age)
    )
    validate(
      people.selectFromTyped(_.id, _.name, _.age, _.address),
      people.select(id, name, age, address)
    )
    validate(
      people.selectFromTyped(_.id, _.name, _.age, _.address.street),
      people.select(id, name, age, street)
    )
    validate(
      people.selectFromTyped(_.id, _.name, _.age, _.address.street, _.address.city),
      people.select(id, name, age, street, city)
    )
  }

}
