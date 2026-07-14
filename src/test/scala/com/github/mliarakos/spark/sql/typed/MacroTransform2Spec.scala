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

  it should "transform a dataset using transformTo" in {
    val result =
      inputs.transformTo[Output](
        _.column(_.id).renameTo[Output](_.id),
        _.column(_.start_date).renameTo[Output](_.startDate)
      )

    val expected =
      inputs
        .select(
          inputs.col("id"),
          inputs.col("start_date").as("startDate")
        )
        .as[Output]

    validate(result, expected)
  }

  // it should "transform a dataset using transformTo including a column transformTo" in {
  //   val result =
  //     people.transformTo[Person](
  //       _.column(_.id),
  //       _.column(_.name),
  //       _.column(_.age),
  //       _.column(_.address)
  //         .transformTo[Address](_.field(_.street), _.field(_.city))
  //         .renameTo[Person](_.address)
  //     )

  //   val expected =
  //     people
  //       .select(
  //         people.col("id"),
  //         people.col("name"),
  //         people.col("age"),
  //         F.struct(
  //           people.col("address").getField("street").as("street"),
  //           people.col("address").getField("city").as("city")
  //         ).as("address")
  //       )
  //       .as[Person]

  //   validate(result, expected)
  // }

  it should "transform a dataset using transformTo including a column transform" in {
    val result =
      inputs.transformTo[Output](
        _.column(_.id),
        _.column(_.start_date).transform(col => TypedF.replace(col, "-", "_")).renameTo[Output](_.startDate)
      )

    val expected =
      inputs
        .select(
          inputs.col("id"),
          F.replace(inputs.col("start_date"), F.lit("-"), F.lit("_")).as("startDate")
        )
        .as[Output]

    validate(result, expected)
  }

  it should "transform a dataset using transformTo including a column udfTransform" in {
    val result =
      inputs.transformTo[Output](
        _.column(_.id).renameTo[Output](_.id),
        _.column(_.start_date).udfTransform(_.replace("-", "_")).renameTo[Output](_.startDate)
      )

    val expected =
      inputs
        .select(
          inputs.col("id"),
          F.replace(inputs.col("start_date"), F.lit("-"), F.lit("_")).as("startDate")
        )
        .as[Output]

    validate(result, expected)
  }

  it should "transform a dataset using transformTo adding a constant column" in {
    val result =
      inputs.transformTo[Output](
        _.column(_.id),
        _ => TypedF.lit("2000-01-02").renameTo[Output](_.startDate)
      )

    val expected =
      inputs
        .select(
          inputs.col("id"),
          F.lit("2000-01-02").as("startDate")
        )
        .as[Output]

    validate(result, expected)
  }

  it should "use orEmpty on a column with type Option[Seq[_]]" in {
    val result   = events.select(events.column(_.details).orEmpty)
    val expected = events.select(F.coalesce(events.col("details"), F.array()).as("details").as[Seq[RawDetail]])

    validate(result, expected)
  }

  it should "wip" in {
    val x = events.column(_.details).map(col => F.size(col).as[Long])
    // println(x)
  }

}
