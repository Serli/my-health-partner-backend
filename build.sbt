name := "Spark-HandsOn"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "2.0.0-M3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

parallelExecution in test := false