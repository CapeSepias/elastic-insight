import AssemblyKeys._

assemblySettings

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set(
    "spatial4j-0.4.1.jar", 
    "lucene-spatial-4.9.0.jar",
    "lucene-sandbox-4.9.0.jar",
    "lucene-misc-4.9.0.jar",
    "lucene-join-4.9.0.jar", 
    "lucene-suggest-4.9.0.jar", 
    "lucene-grouping-4.9.0.jar",     
    "lucene-codecs-4.9.0.jar",
    "lucene-memory-4.9.0.jar",
    "lucene-queries-4.9.0.jar",
    "lucene-highlighter-4.9.0.jar",
    "lucene-queryparser-4.9.0.jar",
    "lucene-core-4.9.0.jar",
    "lucene-analyzers-common-4.9.0.jar",
    "elasticsearch-1.3.2.jar",
    "antlr-runtime-3.5.jar", 
    "asm-4.1.jar",
    "asm-commons-4.1.jar",
    "sbt-assembly-0.11.2.jar"
  ) 
  cp filter { jar => excludes(jar.data.getName) }
}

name := "elastic-insight"

organization := "de.kp.elastic.insight"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
   "org.elasticsearch" % "elasticsearch" % "1.3.2",
   "com.typesafe.akka" % "akka-actor_2.10" % "2.2.3",
   "com.typesafe.akka" % "akka-contrib_2.10" % "2.2.3",
   "com.typesafe.akka" % "akka-remote_2.10" % "2.2.3",
   "org.json4s" % "json4s-native_2.10" % "3.2.10"
 )   