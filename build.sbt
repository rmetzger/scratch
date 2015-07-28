version := "1.0"

scalaVersion := "2.10.4"

val flinkVersion = "0.9.0"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq("org.apache.flink" % "flink-scala" % flinkVersion, "org.apache.flink" % "flink-clients" % flinkVersion)

libraryDependencies += "org.apache.flink" % "flink-streaming-scala" % flinkVersion

libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % flinkVersion exclude("org.apache.kafka", "kafka_${scala.binary.version}")

libraryDependencies += "com.101tec" % "zkclient" % "0.5"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.0"


lazy val commonSettings = Seq(
  organization := "com",
  version := "0.1.0",
  scalaVersion := "2.10.4"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(name := "flink-sbt-with-assembly")
  .settings(sbtPlugin := true)

// Run options
fork in run := true


assemblyMergeStrategy in assembly := { 
 case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
 case x => MergeStrategy.first
}
