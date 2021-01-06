package com.github.mliarakos.spark.sql.typed

import org.apache.spark.sql.Dataset
import org.scalatest.Matchers

import scala.collection.immutable._
import scala.reflect.ClassTag

trait MacroTestFixtures { _: Matchers with SparkMatchers =>
  def validate[A: ClassTag](ds: Dataset[A], expected: Dataset[A]): Unit = {
    ds shouldNot useObjectSerialization
    ds.columns shouldBe expected.columns
    ds shouldEqual expected
  }
}

object MacroTestFixtures {

  case class Address(street: String, city: String)
  case class Person(id: String, name: String, age: Int, address: Address)
  case class Username(id: String, username: String)

  val peopleData: Seq[Person] = Seq(
    Person("1", "John", 18, Address("123 Main St", "Capital City")),
    Person("2", "Sally", 23, Address("456 South St", "Capital City"))
  )
  val usernameData: Seq[Username] = Seq(
    Username("1", "john"),
    Username("2", "sally")
  )

}
