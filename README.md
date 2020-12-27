# Spark Typed Ops

Spark Typed Ops is a Scala library that provides lightweight type-safe operations for [Spark](https://spark.apache.org/) [Datasets](https://spark.apache.org/docs/latest/sql-programming-guide.html). Perform simple Dataset operations with added type-safety that compile to efficient DataFrame operations:

```scala
import com.github.mliarakos.spark.sql.typed.ops._

case class User(id: Int, name: String, email: String)

// create column
colFrom[User](_.id)

// compiles to:
col("id")

val ds: Dataset[User] = ...

// select columns
ds.selectFrom(_.id, _.name)

// compiles to:
ds.select(ds("id"), ds("name"))
```

Dataset columns are specified in a type-safe manner, so errors (e.g. misspelled or non-existent fields) are caught by the compiler, but equivalent untyped DataFrame operations are used at runtime for improved performance. In addition, the simple approach to specifying columns is easily supported by IDEs for autocompletion and refactoring.  

This project was inspired by [Frameless](https://github.com/typelevel/frameless) and [scala-nameof](https://github.com/dwickern/scala-nameof).

## Getting Started

Add `spark-typed-ops` as dependency to your project:

```scala
"com.github.mliarakos" %% "spark-typed-ops" % "0.1.0"
```

Spark Types Ops intentionally does not have a compile dependency on Spark. This essentially allows you to use any version of Spark Typed Ops with any version of Spark. The following versions are tested, but others will most likely work:

| Spark Types Ops | Scala | Spark |
| --- | --- | --- |
| 0.1.0 | 2.11 <br/> 2.12 | 2.4.7 <br/> 3.0.1 |

## Motivation

Spark Datasets add type safety to DataFrames, but with a slight trade-off for performance due to overhead of object serialization and deserialization. There are many common simple use cases where we'd like to avoid the object overhead while maintaining type-safety.

Consider the example of selecting columns from a Dataset as DataFrame:

```scala
case class User(id: Int, name: String, email: String)

val ds: Dataset[User] = ...

// maintain type-safety, incur object overhead
val df1 = ds.map(user => (user.id, user.name))

// lose type-safety, avoid object overhead
val df2 = ds.select("id", "name")
```

The first approach using `map` maintains type-safety, but incurs object overhead. The second approach using `select` uses unsafe string column names, but avoids object overhead. The object overhead can be seen by examining the explain plan of each approach.

For the first approach:

```
== Physical Plan ==
*(1) SerializeFromObject [assertnotnull(input[0, scala.Tuple2, true])._1 AS _1#10, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true, false) AS _2#11]
+- *(1) MapElements <function1>, obj#9: scala.Tuple2
   +- *(1) DeserializeToObject newInstance(class User), obj#8: User
      +- LocalTableScan <empty>, [id#3, name#4, email#5]
```

For the second approach:

```
== Physical Plan ==
LocalTableScan <empty>, [id#3, email#5]
```

Spark Typed Ops provides Dataset extensions to get both benefits:

```scala
import com.github.mliarakos.spark.sql.typed.ops._

val df3 = ds.selectFrom(_.id, _.name)
```

The `selectFrom` method converts the field names to a string column names, so this call compiles to:

```scala
val df3 = ds.select(ds("id"), ds("name"))
```

The field names are accessed in a type-safe manner, so any errors (e.g. misspelled or non-existent fields) are caught by the compiler.