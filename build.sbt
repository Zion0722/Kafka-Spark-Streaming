name := "KafkaSparkStreaming"

version := "0.1"

scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" excludeAll(
  ExclusionRule(organization = "org.spark-project.spark", name = "unused"),
  ExclusionRule(organization = "org.apache.spark", name = "spark-streaming"),
  ExclusionRule(organization = "org.apache.hadoop")
)


