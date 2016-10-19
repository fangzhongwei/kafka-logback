name := "kafka-logback"

version := "1.0-SNAPSHOT"

organization := "com.lawsofnature.logback"

scalaVersion := "2.11.8"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"