organization := "com.github.mliarakos"
name := "spark-typed-ops"
version := "0.1.0-SNAPSHOT"

val scalaVersions = Seq("2.11.12", "2.12.12")
crossScalaVersions := scalaVersions
scalaVersion := scalaVersions.head
scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls"
)

homepage := Some(url("https://github.com/mliarakos/spark-typed-ops"))
licenses := Seq(("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0")))
organizationHomepage := Some(url("https://github.com/mliarakos"))
pomExtra := {
  <developers>
    <developer>
      <id>mliarakos</id>
      <name>Michael Liarakos</name>
      <url>https://github.com/mliarakos</url>
    </developer>
  </developers>
}
pomIncludeRepository := { _ =>
  false
}
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org"
  if (isSnapshot.value) Some("snapshots".at(s"$nexus/content/repositories/snapshots"))
  else Some("releases".at(s"$nexus/service/local/staging/deploy/maven2"))
}
scmInfo := Some(
  ScmInfo(url("https://github.com/mliarakos/spark-typed-ops"), "scm:git:git@github.com:mliarakos/spark-typed-ops.git")
)

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

libraryDependencies ++= Seq(
  "com.chuusai"   %% "shapeless" % "2.3.3",
  "org.scalatest" %% "scalatest" % "3.0.9" % Test
)

// Use Spark 2.x with Scala 2.11 and Spark 3.x with Scala 2.12
libraryDependencies ++= {
  if (scalaVersion.value.startsWith("2.11")) {
    Seq(
      "org.apache.spark" %% "spark-sql"          % "2.4.7"        % Provided,
      "com.holdenkarau"  %% "spark-testing-base" % "2.4.5_0.14.0" % Test
    )
  } else {
    Seq(
      "org.apache.spark" %% "spark-sql"          % "3.0.1"       % Provided,
      "com.holdenkarau"  %% "spark-testing-base" % "3.0.1_1.0.0" % Test
    )
  }
}
