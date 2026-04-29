resolvers += Classpaths.sbtPluginReleases

// https://scalameta.org/scalafmt/
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")

// https://scalacenter.github.io/scalafix/
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.6")

// https://github.com/sbt/sbt-maven-resolver
// addSbtPlugin("org.scala-sbt" % "sbt-maven-resolver" % "0.1.1")

// https://github.com/scoverage/sbt-scoverage
// addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")

// https://github.com/sonar-scala/sbt-sonar
// addSbtPlugin("com.sonar-scala" % "sbt-sonar" % "2.3.0")

// https://github.com/sbt/sbt-assembly
// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

// https://github.com/sbt/sbt-native-packager
// addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.11.17")

// https://github.com/sbt/sbt-pgp
// The following should be placed in ~/.sbt/1.0/plugins/gpg.sbt
// addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.3.1")

logLevel := Level.Warn
