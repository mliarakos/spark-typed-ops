organization := "com.github.mliarakos"
name         := "spark-typed-ops"
version      := "0.2.0-SNAPSHOT"

val scalaVersions = Seq("2.12.21")
crossScalaVersions := scalaVersions
scalaVersion       := scalaVersions.head
scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls",
  "-Ywarn-unused"
)

homepage             := Some(url("https://github.com/mliarakos/spark-typed-ops"))
licenses             := Seq(("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0")))
organizationHomepage := Some(url("https://github.com/mliarakos"))
pomExtra             := {
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
publishTo         := {
  val nexus = "https://oss.sonatype.org"
  if (isSnapshot.value) Some("snapshots".at(s"$nexus/content/repositories/snapshots"))
  else Some("releases".at(s"$nexus/service/local/staging/deploy/maven2"))
}
scmInfo := Some(
  ScmInfo(url("https://github.com/mliarakos/spark-typed-ops"), "scm:git:git@github.com:mliarakos/spark-typed-ops.git")
)

Test / parallelExecution := false
Test / fork              := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  "com.chuusai"   %% "shapeless" % "2.3.13",
  "org.scalatest" %% "scalatest" % "3.1.4" % Test
)

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-sql"          % "3.5.8"       % Provided,
    "com.holdenkarau"  %% "spark-testing-base" % "3.5.6_3.0.1" % Test
  )
}
