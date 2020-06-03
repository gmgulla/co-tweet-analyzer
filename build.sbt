name := "co-tweet-analyzer"

version := "0.1"

scalaVersion := "2.12.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

// JUnit for Scala library
libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default"