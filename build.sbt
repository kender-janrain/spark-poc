import sbt.ExclusionRule
import sbt.ExclusionRule

net.virtualvoid.sbt.graph.Plugin.graphSettings

organization := "janrain"

name := "jedi-logging-poc"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

val sparkExclusions = Seq(
  ExclusionRule(name = "minlog"),
  ExclusionRule(name = "slf4j-simple"),
  ExclusionRule(name = "minlog"),
  ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api"),
  ExclusionRule(name = "commons-beanutils-core"),
  ExclusionRule(name = "commons-collections")
)

libraryDependencies ++= Seq(
  "io.spray" %% "spray-json" % "1.2.5",
  ("org.apache.spark" %% "spark-core" % "0.9.0-incubating" % "provided").excludeAll(sparkExclusions: _*),
  ("org.apache.spark" %% "spark-streaming-kafka" % "0.9.0-incubating").excludeAll(sparkExclusions: _*),
  ("com.amazonaws" % "amazon-kinesis-client" % "1.0.0").excludeAll()
)