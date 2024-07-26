// Project name
name := "KafkaToMongoDB"

// Project version
version := "1.0"

// Scala version to be used
scalaVersion := "2.12.10"

// Dependencies for the project
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-streaming" % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1"
)

// Adding sbt-assembly plugin for creating a fat JAR
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")

// Assembly configuration to handle duplicate files
import sbtassembly.MergeStrategy

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => 
    xs match {
      case "io.netty.versions.properties" :: Nil => MergeStrategy.first
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
