organization := "com.github.mliarakos"
name := "spark-typed-ops"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"
scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlog-reflective-calls"
)

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

libraryDependencies ++= Seq(
  "com.chuusai"      %% "shapeless"          % "2.3.3",
  "org.apache.spark" %% "spark-sql"          % "2.4.7" % Provided,
  "org.scalatest"    %% "scalatest"          % "3.0.9" % Test,
  "com.holdenkarau"  %% "spark-testing-base" % "2.4.5_0.14.0" % Test
)
