package com.github.mliarakos.spark.sql.typed

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable._
import scala.reflect.ClassTag

trait MacroTestFixtures { _: Matchers with SparkMatchers =>
  def validate[A: ClassTag](ds: Dataset[A], expected: Dataset[A]): Unit = {
    ds shouldNot useObjectSerialization
    ds.columns shouldBe expected.columns
    ds shouldEqual expected
  }
}

trait MacroTestData { _: DatasetSuiteBase =>
  import MacroTestFixtures._
  import spark.implicits._

  protected val peopleDataset: Dataset[Person]  = peopleData.toDS()
  protected val inputDataset: Dataset[Input]    = inputData.toDS()
  protected val eventDataset: Dataset[RawEvent] = rawEventData.toDS()

}

object MacroTestFixtures {

  final case class Address(street: String, city: String)
  final case class Person(id: String, name: String, age: Int, address: Address)
  final case class Username(id: String, username: String)

  final case class Input(id: Int, start_date: String)
  final case class Output(id: Int, startDate: String)

  final case class RawDetail(key: String, value: Option[String])
  final case class RawEvent(event_id: Int, event_date: String, details: Option[Seq[RawDetail]])

  final case class ParsedDetail(key: String, value: String)
  final case class ParsedEvent(eventId: Int, eventDate: String, details: Seq[ParsedDetail])

  final case class Property(key: Int, group: Option[String], values: Seq[String])

  val peopleData: Seq[Person] = Seq(
    Person("1", "John", 18, Address("123 Main St", "Capital City")),
    Person("2", "Sally", 23, Address("456 South St", "Capital City"))
  )
  val usernameData: Seq[Username] = Seq(
    Username("1", "john"),
    Username("2", "sally")
  )
  val inputData: Seq[Input] = Seq(
    Input(1, "2001-02-03"),
    Input(2, "2004-05-06")
  )
  val rawEventData: Seq[RawEvent] = Seq(
    RawEvent(1, "2001-02-03", Some(Seq(RawDetail("AA", Some("BB")), RawDetail("CC", None)))),
    RawEvent(2, "2004-05-06", Some(Seq.empty)),
    RawEvent(3, "2007-08-09", None)
  )
  val propertyData: Seq[Property] = Seq(
    Property(1, Some("GG"), Seq("AA", "BB")),
    Property(1, Some("FF"), Seq("CC")),
    Property(2, None, Seq.empty)
  )

}
