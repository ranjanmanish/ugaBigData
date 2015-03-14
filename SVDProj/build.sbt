name := "ManishProject"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.1"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
