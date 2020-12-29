# Spark Typed Ops

Spark Typed Ops is a Scala library that provides lightweight type-safe operations for [Spark](https://spark.apache.org/). Perform simple typed [Datasets](https://spark.apache.org/docs/latest/sql-programming-guide.html) operations that compile to efficient DataFrame operations:

```scala
import com.github.mliarakos.spark.sql.typed.ops._

case class User(id: Int, name: String, email: String)

// create column
colFrom[User](_.id) // compiles to: col("id")

val ds: Dataset[User] = ???

// get a column
ds.colFrom(_.id) // compiles to: ds.col("id")

// select columns
ds.selectFrom(_.id, _.name) // compiles to: ds.select(ds.col("id"), ds.col("name"))
```

Dataset columns are accessed in a type-safe manner, so errors (e.g. misspelled or non-existent columns) are caught by the compiler. The operations are then converted to equivalent untyped DataFrame operations for improved runtime performance. In addition, the simple approach to specifying columns is easily supported by IDEs for autocompletion and refactoring.  

This project was inspired by [Frameless](https://github.com/typelevel/frameless) and [scala-nameof](https://github.com/dwickern/scala-nameof). Its goal is to remain lightweight and not to introduce a new API on top of Spark. As such, it provides only simple operations with type-safe column access. For a more complete type-safe extension to Spark, consider [Frameless](https://github.com/typelevel/frameless).

## Getting Started

Add `spark-typed-ops` as dependency to your project:

```scala
"com.github.mliarakos" %% "spark-typed-ops" % "0.1.0"
```

Spark Types Ops intentionally does not have a compile dependency on Spark. This essentially allows you to use any version of Spark Typed Ops with any version of Spark. The following versions are tested with Spark Typed Ops, but others will most likely work:

| Spark Types Ops | Scala | Spark |
| --- | --- | --- |
| 0.1.0 | 2.11 <br/> 2.12 | 2.4.7 <br/> 3.0.1 |

## Motivation

Spark Datasets add type safety to DataFrames, but with a slight trade-off for performance due to the overhead of object serialization and deserialization. There are many common simple use cases where we'd like to avoid the object overhead while maintaining type-safety.

Consider the example of selecting columns from a Dataset as a DataFrame:

```scala
case class User(id: Int, name: String, email: String)
val ds: Dataset[User] = ???

// maintain type-safety, incur object overhead
val df1 = ds.map(user => (user.id, user.name))

// lose type-safety, avoid object overhead
val df2 = ds.select("id", "name")
```

The first approach using `map` maintains type-safety, but incurs object overhead. The columns are derived from type-safe access of the user object. However, as shown in the explain plan, the user object must be deserialized to be used by the function and the resulting tuple must be serialized:

```
== Physical Plan ==
*(1) SerializeFromObject [assertnotnull(input[0, scala.Tuple2, true])._1 AS _1#10, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true, false) AS _2#11]
+- *(1) MapElements <function1>, obj#9: scala.Tuple2
   +- *(1) DeserializeToObject newInstance(class User), obj#8: User
      +- LocalTableScan <empty>, [id#3, name#4, email#5]
```

The second approach using `select` uses unsafe string column names, but avoids object overhead. The column names can only be validated at runtime, not compile time. However, as shown in the explain plan, the columns are directly accessed without having to serialize or deserialize any objects:

```
== Physical Plan ==
LocalTableScan <empty>, [id#3, email#5]
```

Spark Typed Ops provides Dataset extensions to get both benefits. It uses Scala macros to convert type-safe Dataset operations to efficient DataFrame operations at compile time. The added type-safety helps prevents errors (e.g. misspelled or non-existent columns) without sacrificing performance. It also operates only at compile time using only the existing Spark API so there's no runtime impact.

## Usage

Use Spark Type Ops by importing:

```scala
import com.github.mliarakos.spark.sql.typed.ops._
```

This provides type-safe versions of several Spark SQL functions for working with columns:

```scala
case class User(id: Int, name: String, email: String)

// names
nameFrom[User](_.id)          // "id"
namesFrom[User](_.id, _.name) // Seq("id", "name")

// columns
$[User](_.id)                // col("id")
colFrom[User](_.id)          // col("id")
colsFrom[User](_.id, _.name) // Seq(col("id"), col("name"))

// typed columns
typedColFrom[User](_.id)   // col("id").as[Int]
typedColFrom[User](_.name) // col("name").as[String]
```

It also provides type-safe Dataset extensions for many common methods:

```scala
val ds: Dataset[User] = ???

// names
ds.nameFrom(_.id)          // "id"
ds.namesFrom(_.id, _.name) // Seq("id", "name")

// columns
ds.colFrom(_.id)          // ds.col("id")
ds.colsFrom(_.id, _.name) // Seq(ds.col("id"), ds.col("name"))

// select
ds.selectFrom(_.id, _.name)      // ds.select(ds.col("id"), ds.col("name"))
ds.selectFromTyped(_.id, _.name) // ds.select(ds.col("id").as[Int], ds.col("name").as[String])

// other methods
ds.cubeFrom(_.id, _.name)                  // ds.cube("id", "name")
ds.describeFrom(_.id, _.name)              // ds.describe("id", "name")
ds.dropFrom(_.id, _.name)                  // ds.drop("id", "name")
ds.dropDuplicatesFrom(_.id, _.name)        // ds.dropDuplicates("id", "name")
ds.groupByFrom(_.id, _.name)               // ds.groupBy("id", "name")
ds.orderByFrom(_.id, _.name)               // ds.orderBy("id", "name")
ds.rollupFrom(_.id, _.name)                // ds.rollup("id", "name")
ds.sortFrom(_.id, _.name)                  // ds.sort("id", "name")
ds.sortWithinPartitionsFrom(_.id, _.name)  // ds.sortWithinPartitions("id", "name")
ds.withColumnRenamedFrom(_.id, "recordId") // ds.withColumnRenamed("id", "recordId")
```

Nested columns are supported:

```scala
case class Address(street: String, city: String)
case class User(id: Int, name: String, address: Address)
val ds: Dataset[User] = ???

colFrom[User](_.address.street) // col("address.street")
ds.colFrom(_.address.street)    // ds.col("address.street")
ds.selectFrom(_.address.street) // ds.select(ds.col("address.street"))
```

These functions and methods are useful in specifying column expressions and conditions:

```scala
// User with posts
case class User(id: Int, name: String, email: String)
case class Post(id: Int, userId: Int, post: String)

val users: Dataset[User] = ???
val posts: Dataset[Post] = ???

// where conditions
posts.where(length(colFrom[Post](_.post)) > 10) // posts.where(length(col("post")) > 10)
users.where(users.colFrom(_.name).like("a%"))   // users.where(users.col("name").like("a%"))

// sort condition
posts.sort(length(colFrom[Post](_.post)).desc) // posts.sort(length(col("post")).desc)

// join condition
users.join(posts, users.colFrom(_.id) === posts.colFrom(_.userId))
  // users.join(posts, users.col("id") === posts.col("userId"))

// new column expression
posts.withColumn("preview", substring(posts.colFrom(_.post), 0, 10))
  // posts.withColumn("preview", substring(posts.col("post"), 0, 10))
```

Most of the Dataset extension methods are provided for convenience to reduce boilerplate code. They can be written using the name/column methods:

```scala
// these compile to the same expression
// ds.orderBy("id", "name")
users.orderByFrom(_.id, _.name)
users.orderBy(users.nameFrom(_.id), users.nameFrom(_.name))
users.orderBy(nameFrom[User](_.id), nameFrom[User](_.name))

// these compile to an equivalent expression
users.orderBy(users.colFrom(_.id), users.colFrom(_.name))
users.orderBy(users.colsFrom(_.id, _.name): _*)
users.orderBy(colFrom[User](_.id), colFrom[User](_.name))
users.orderBy(colsFrom[User](_.id, _.name): _*)
```

However, the extension methods also help ensure columns are used in the correct context:

```scala
// will succeed at runtime
// the columns are derived from the type of the dataset, so they must be valid on the dataset
users.orderByFrom(_.id, _.name)

// will fail at runtime
// although userId is a valid Post column, it is not a valid column on the users dataset
users.orderBy(nameFrom[Post](_.userId))
users.orderBy(colFrom[Post](_.userId))

// will fail at runtime
// although id is a valid posts column and is also a present in the users dataset, the column references the posts
// dataset and is not actually available on the users dataset
users.orderBy(posts.colFrom(_.id))

// will succeed at runtime, but is fragile to refactoring
// id is a valid Post column and is also present in the users dataset, but the operation succeeds only because the 
// column is specified by name and the names coincidentally match
// refactoring the name of the column on User or Post will cause the operation to fail at runtime
users.orderBy(nameFrom[Post](_.id))
```

In general, the type-safety added by Spark Typed Ops is for column access, not for column usage. Be careful that columns are used in an appropriate context.
