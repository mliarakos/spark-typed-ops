package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.MacroTestData._
import com.github.mliarakos.spark.sql.typed.transforms._
import com.github.mliarakos.spark.sql.typed.{functions => TypedF}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{functions => F}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable._

class MacroTransformSpec extends AnyFlatSpec with Matchers with SparkMatchers with DatasetSuiteBase with MacroTestFixtures with MacroTestData {
  import spark.implicits._

  it should "get column name from an unresolved column" in {
    F.col("id").as[Int].getName shouldBe "id"
    F.col("address.street").as[String].getName shouldBe "address.street"
  }

  it should "get column name from a dataset column" in {
    people.column(_.id).getName shouldBe "id"
    people.column(_.address.street).getName shouldBe "address.street"
  }

  it should "get column from a dataset" in {
    people.column(_.id) shouldEqual people.col("id").as("id").as[Int]
    people.column(_.address.street) shouldEqual people.col("address.street").as("address.street").as[String]
  }

  it should "rename a column from a dataset" in {
    people.column(_.id).rename("identifier") shouldEqual people.col("id").as("id").as("identifier").as[Int]
  }

  it should "rename a column from a dataset using a selection" in {
    inputs.column(_.start_date).renameTo[Output](_.startDate) shouldEqual inputs.col("start_date").as("start_date").as("startDate").as[String]
  }

  it should "get field from column from a dataset" in {
    people.column(_.address).field(_.street) shouldEqual people.col("address").as("address").getField("street").as("street").as[String]
  }

  it should "use orEmpty on a column with type Option[Seq[_]]" in {
    val result   = events.select(events.column(_.details).orEmpty)
    val expected = events.select(F.coalesce(events.col("details"), F.array()).as("details").as[Seq[RawDetail]])

    validate(result, expected)
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

  it should "transform a dataset using transformTo including a column transformTo" in {
    val result =
      people.transformTo[Person](
        _.column(_.id),
        _.column(_.name),
        _.column(_.age),
        _.column(_.address)
          .transformTo[Address](_.field(_.street), _.field(_.city))
          .renameTo[Person](_.address)
      )

    val expected =
      people
        .select(
          people.col("id"),
          people.col("name"),
          people.col("age"),
          F.struct(
            people.col("address").getField("street").as("street"),
            people.col("address").getField("city").as("city")
          ).as("address")
        )
        .as[Person]

    validate(result, expected)
  }

  it should "transform a dataset using transformTo including a column transform" in {
    val result =
      inputs.transformTo[Output](
        _.column(_.id).renameTo[Output](_.id),
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
        _.column(_.id).renameTo[Output](_.id),
        _ => TypedF.lit("2000-01-02").renameTo[Output](_.startDate)
      )

    val expected =
      inputs.select(inputs.col("id"), F.lit("2000-01-02").as("startDate")).as[Output]

    validate(result, expected)
  }

  it should "map an iterable column" in {
    val result =
      properties.select(
        properties.column(_.values).map(col => TypedF.upper(col))
      )

    val expected =
      properties.select(
        F.transform(properties.col("values"), col => F.upper(col)).as("values").as[Seq[String]]
      )

    validate(result, expected)
  }

  it should "udfMap an iterable column" in {
    val result =
      properties.select(
        properties.column(_.values).udfMap(_.toUpperCase)
      )

    val upperUdf = F.udf((value: String) => value.toUpperCase)
    val expected =
      properties.select(
        F.transform(properties.col("values"), col => upperUdf.apply(col)).as("values").as[Seq[String]]
      )

    validate(result, expected)
  }

  it should "flatMap an iterable column" in {
    val result =
      properties.select(
        properties.column(_.values).flatMap(col => TypedF.split(col, ""))
      )

    val expected =
      properties.select(
        F.flatten(F.transform(properties.col("values"), col => F.split(col, ""))).as("values").as[Seq[String]]
      )

    validate(result, expected)
  }

  it should "udfFlatMap an iterable column" in {
    val result =
      properties.select(
        properties.column(_.values).udfFlatMap(_.split("").toSeq)
      )

    val splitUdf = F.udf((value: String) => value.split(""))
    val expected =
      properties.select(
        F.flatten(F.transform(properties.col("values"), col => splitUdf.apply(col))).as("values").as[Seq[String]]
      )

    validate(result, expected)
  }

  it should "map an optional column" in {
    val result =
      properties.select(
        properties.column(_.group).map(col => TypedF.upper(col))
      )

    val expected =
      properties.select(
        F.upper(properties.col("group")).as("group").as[Option[String]]
      )

    validate(result, expected)
  }

  it should "udfMap an optional column" in {
    val result =
      properties.select(
        properties.column(_.group).udfMap(_.toUpperCase)
      )

    val upperUdf = F.udf((value: String) => value.toUpperCase)
    val expected =
      properties.select(
        F.when(properties.col("group").isNotNull, upperUdf.apply(properties.col("group"))).as("group").as[Option[String]]
      )

    validate(result, expected)
  }

  it should "flatMap an optional column" in {
    val result =
      properties.select(
        properties.column(_.group).flatMap(col => F.when(col === "GG", col).as[Option[String]])
      )

    val expected =
      properties.select(
        F.when(properties.col("group") === "GG", properties.col("group")).as("group").as[Option[String]]
      )

    validate(result, expected)
  }

  it should "udfFlatMap an optional column" in {
    val result =
      properties.select(
        properties.column(_.group).udfFlatMap(value => if (value == "GG") Some(value) else None)
      )

    val filterUdf = F.udf((value: String) => if (value == "GG") Some(value) else None)
    val expected  =
      properties.select(
        F.when(properties.col("group").isNotNull, filterUdf.apply(properties.col("group"))).as("group").as[Option[String]]
      )

    validate(result, expected)
  }

  it should "wip" in {
    val result =
      events.transformTo[ParsedEvent](
        _.column(_.event_id).renameTo[ParsedEvent](_.eventId),
        _.column(_.event_date).renameTo[ParsedEvent](_.eventDate),
        _.column(_.details).orEmpty
          .flatMap { details =>
            details.field(_.value).map(value => TypedF.structOld[ParsedDetail](details.field(_.key), value.renameTo[ParsedDetail](_.value)))
          }
          .renameTo[ParsedEvent](_.details)
      )

    result.explain(true)
    result.printSchema()
    result.show(false)
  }

}
