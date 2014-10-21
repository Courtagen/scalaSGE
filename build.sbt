import AssemblyKeys._

assemblySettings

name := "scalaSGE"

version := "0.0.1"

scalaVersion := "2.10.4"

unmanagedBase <<= baseDirectory { base => base / "lib" }

mainClass in (Compile, run) := Some("com.github.mylons.sge.TestApp")

mainClass in (Compile, packageBin) := Some("com.github.mylons.sge.TestApp")

mainClass in assembly := Some("org.github.mylons.sge.TestApp")

jarName in assembly := "drmaaTest.jar"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Twitter repo" at "http://maven.twttr.com"

resolvers ++= Seq("snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                    "releases"  at "http://oss.sonatype.org/content/repositories/releases")

resolvers += Opts.resolver.sonatypeSnapshots

libraryDependencies ++= Seq(
    "com.typesafe" %% "scalalogging-log4j" % "1.1.0",
    "org.apache.logging.log4j" % "log4j-api" % "2.0-beta4",
    "org.apache.logging.log4j" % "log4j-core" % "2.0-beta4"
)

