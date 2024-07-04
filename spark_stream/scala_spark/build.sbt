// Nama proyek
name := "KafkaToMongoDB"

// Versi proyek
version := "0.1.0-SNAPSHOT"

// Versi Scala
scalaVersion := "2.12.15"

// Dependensi proyek
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-streaming" % "3.5.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.3.0",
  "org.apache.kafka" % "kafka-clients" % "3.4.0"
)

// Repositori
resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/",
  "MongoDB" at "https://oss.sonatype.org/content/repositories/releases/"
)

// Plugin sbt-assembly untuk membuat jar yang bisa dijalankan
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