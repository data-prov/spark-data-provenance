//
// File: https://github.com/data-prov/spark-data-provenance/blob/main/scala-spark-data-provenance/build.sbt
//

name := "dp-spark"
organization := "org.dataprov.dp"
organizationName := "Fine-grained data provenance"
organizationHomepage := Some(url("http://github.com/data-prov"))
homepage := Some(url("https://github.com/data-prov/spark-data-provenance"))
startYear := Some(2026)
description := "Spark library for fine-grained provenance"
licenses += "MIT" -> url("https://opensource.org/license/mit")

scmInfo := Some(
  ScmInfo(
    url("https://github.com/data-prov/spark-data-provenance"),
    "https://github.com/data-prov/spark-data-provenance.git"
  )
)

developers := List(
  Developer(
    id    = "denis.arnaud",
    name  = "Denis Arnaud",
    email = "denis.arnaud_ossrh@m4x.org",
    url   = url("https://github.com/da115115")
  ),
  Developer(
    id    = "ronan.fruit",
    name  = "Ronan Fruit",
    email = "ronan@fruit.nom.fr",
    url   = url("https://github.com/RonanFR")
  )
)

//useGpg := true

version := scala.io.Source.fromFile("VERSION").getLines.toList.head
scalaVersion := "2.13.18"
val sparkVersion = "4.1.1"

crossScalaVersions := Seq("2.13.17", "2.13.18")

update / checksums  := Nil
lazy val root = project in file(".")

/**
  * Latest releases:
  * log4j: https://logging.apache.org/log4j/2.x/download.html
  * As log4j is part of the Spark distribution, check its version from
  * the installed PySpark module, e.g.:
  * ~/.pyenv/versions/${PYTHON_VERSION}/lib/python3.12/site-packages/pyspark/jars/
  * scopt: https://github.com/scopt/scopt
  * nscala-time: https://github.com/nscala-time/nscala-time
  * ScalaTest / Scalactic: https://www.scalatest.org/ / https://www.scalactic.org/
  * Spark-fast-test: https://github.com/MrPowers/spark-fast-tests
  * Spec2-core: https://github.com/etorreborre/specs2
  */
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.apache.logging.log4j" % "log4j-core" % "2.24.3" % "provided",
  "org.apache.logging.log4j" % "log4j-api" % "2.24.3" % "provided",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.24.3" % "provided",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "com.github.nscala-time" %% "nscala-time" % "3.0.0",
  "org.specs2" %% "specs2-core" % "4.23.0" % "test",
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "3.0.2" % "test"
)

// Compilation options
javacOptions ++= Seq("-source", "21")
scalacOptions ++= Seq("-deprecation", "-feature")

// Run main class
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

Compile / runMain := Defaults.runMainTask(
  Compile / fullClasspath,
  Compile / run / runner
).evaluated

/**
  * Java runtime options. These options are taken into account only when forking
  * a new JVM (we would need something like 'Compile / run / fork := true'),
  * which is not the case by default.
  * See https://github.com/sbt/sbt/issues/2041
  */
// Compile / run / fork := true
// Compile / run / javaOptions ++= Seq("-Xms2048M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")

// Tests
// lazy val enablingCoverageSettings = Seq(
//  Test / compile / coverageEnabled := true,
//  Compile / compile / coverageEnabled := false
// )
//Test / compile / coverageEnabled := true
Test / parallelExecution := false
Test / run / fork := true
Test / run / javaOptions ++= Seq("-Xms2048M", "-Xmx4096M", "-XX:+CMSClassUnloadingEnabled")

// Sonar
// import sbtsonar.SonarPlugin.autoImport.sonarUseExternalConfig

// sonarUseExternalConfig := true

// Assembly and packaging
ThisBuild / versionScheme := Some("early-semver")

pomIncludeRepository := { _ => false }

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

