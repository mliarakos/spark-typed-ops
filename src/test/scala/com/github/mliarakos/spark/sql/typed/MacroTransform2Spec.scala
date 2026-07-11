package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.MacroTestData._
import com.github.mliarakos.spark.sql.typed.transforms2._
import com.github.mliarakos.spark.sql.typed.{functions => TypedF}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{functions => F}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable._
import com.github.mliarakos.spark.sql.typed.tags._

class MacroTransform2Spec extends AnyFlatSpec with Matchers with SparkMatchers with DatasetSuiteBase with MacroTestFixtures with MacroTestData {
  import spark.implicits._

  it should "get a column name tag" in {
    tagOf(people.column(_.id)) shouldBe "id"
    tagOf(people.column(_.address.street)) shouldBe "address.street"
  }

  it should "get column name from a dataset column" in {
    people.column(_.id).getName shouldBe "id"
    people.column(_.address.street).getName shouldBe "address.street"
  }

  it should "get column from a dataset" in {
    people.column(_.id) shouldEqual people("id").as[String]
    // people.column(_.address.street) shouldEqual people("address.street").as[String]
  }

  it should "rename a column from a dataset" in {
    people.column(_.id).rename("identifier") shouldEqual people.col("id").as("identifier").as[String]
  }

  it should "rename a column from a dataset using a selection" in {
    inputs.column(_.start_date).renameTo[Output](_.startDate) shouldEqual inputs.col("start_date").as("startDate").as[String]
  }

  it should "get field from column from a dataset" in {
    people.column(_.address).field(_.street) shouldEqual people.col("address").getField("street").as("street").as[String]
  }

  it should "use orEmpty on a column with type Option[Seq[_]]" in {
    val result   = events.select(events.column(_.details).orEmpty)
    val expected = events.select(F.coalesce(events.col("details"), F.array()).as("details").as[Seq[RawDetail]])

    validate(result, expected)
  }

  it should "wip" in {
    val x = events.column(_.details).map(col => F.size(col).as[Long])
    println(x)
  }

}
