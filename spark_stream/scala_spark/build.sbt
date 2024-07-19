name := "KafkaToMongoDB"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-streaming" % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1"
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")

// Konfigurasi assembly untuk menangani file duplikat
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
