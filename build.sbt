import AssemblyKeys._

assemblySettings

name := "scalaSGE"

version := "0.0.1"

scalaVersion := "2.10.0"

unmanagedBase <<= baseDirectory { base => base / "lib" }

mainClass in (Compile, run) := Some("com.github.mylons.sge.TestApp")

mainClass in (Compile, packageBin) := Some("com.github.mylons.sge.TestApp")

mainClass in assembly := Some("org.github.mylons.sge.TestApp")

jarName in assembly := "drmaaTest.jar"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.1.2"

