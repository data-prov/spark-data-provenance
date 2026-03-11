ThisBuild / organization := "io.github.dataprov"

name := "scala-spark-data-provenance"

ThisBuild / version := "0.0.1"

ThisBuild / scalaVersion := "2.13.18"

val sparkVersion = "4.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"

// test suite settings
Test / fork := true
Test / javaOptions ++= Seq("-Xms512M", "-Xmx2048M")
// Show runtime of tests
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

