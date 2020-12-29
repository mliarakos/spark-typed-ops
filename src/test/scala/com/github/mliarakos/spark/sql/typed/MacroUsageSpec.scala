package com.github.mliarakos.spark.sql.typed

import com.github.mliarakos.spark.sql.typed.MacroTestFixtures._
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.functions.{col, concat, upper}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable._

class MacroUsageSpec extends FlatSpec with Matchers with SparkMatchers with DatasetSuiteBase with MacroTestFixtures {

  import ops._
  import spark.implicits._

  it should "test" in {
    val people    = peopleData.toDS()
    val usernames = usernameData.toDS()

//    usernames.orderBy(people.nameFrom(_.id))
    usernames.orderBy(colFrom[Person](_.id))
  }

  it should "use a name for a join column" in {
    val people    = peopleData.toDS()
    val usernames = usernameData.toDS()

    val usersByName        = people.join(usernames, Seq(nameFrom[Person](_.id)))
    val usersByDatasetName = people.join(usernames, Seq(people.nameFrom(_.id)))
    val expected           = people.join(usernames, Seq("id"))

    validate(usersByName, expected)
    validate(usersByDatasetName, expected)
  }

  it should "get a column for a select" in {
    val people    = peopleData.toDS()
    val addresses = people.select(colFrom[Person](_.id), colFrom[Person](_.address.street))
    validate(addresses, people.select("id", "address.street"))
  }

  it should "create a column expression" in {
    colFrom[Person](_.age) > 0 shouldEqual col("age") > 0
    colFrom[Person](_.age) + 1 shouldEqual col("age") + 1
    upper(colFrom[Person](_.name)) shouldEqual upper(col("name"))
    concat(colFrom[Person](_.id), colFrom[Person](_.name)) shouldEqual concat(col("id"), col("name"))
  }

  it should "use a column expression" in {
    val people = peopleData.toDS()
    val ages   = people.select(colFrom[Person](_.id), (colFrom[Person](_.age) + 1).as("age"))
    validate(ages, people.select(col("id"), (col("age") + 1).as("age")))

    val filtered = people.select(colFrom[Person](_.id), colFrom[Person](_.age)).filter(colFrom[Person](_.age) < 21)
    validate(filtered, people.select("id", "age").filter(col("age") < 21))
  }

  it should "get a column on a dataset for a select" in {
    val people    = peopleData.toDS()
    val addresses = people.select(people.colFrom(_.id), people.colFrom(_.address.street))
    validate(addresses, people.select(people("id"), people("address.street")))
  }

  it should "use a column on a dataset for a join condition" in {
    val people    = peopleData.toDS()
    val usernames = usernameData.toDS()

    val condition = people.colFrom(_.id) === usernames.colFrom(_.id)
    condition shouldEqual people("id") === usernames("id")
  }

  it should "use a column on a dataset in a join condition" in {
    val people    = peopleData.toDS()
    val usernames = usernameData.toDS()

    val users = people.join(usernames, people.colFrom(_.id) === usernames.colFrom(_.id))
    validate(users, people.join(usernames, people("id") === usernames("id")))
  }

  it should "use a column on a dataset in a joinWith condition" in {
    val people    = peopleData.toDS()
    val usernames = usernameData.toDS()

    val users = people.joinWith(usernames, people.colFrom(_.id) === usernames.colFrom(_.id))
    validate(users, people.joinWith(usernames, people("id") === usernames("id")))
  }

  it should "get a typed column for a select" in {
    val people    = peopleData.toDS()
    val addresses = people.select(typedColFrom[Person](_.id), typedColFrom[Person](_.address.street))
    validate(addresses, people.select(col("id").as[String], col("address.street").as[String]))
  }

}
