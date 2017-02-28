name := "SimilarPubmed"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions := Seq("-optimise", "-Yinline-warnings")

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "com.h2database" % "h2" % "1.3.170",
  "com.typesafe.slick" %% "slick" % "3.1.1",
  "javax.servlet" % "javax.servlet-api" % "3.1.0",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.5")
//  "org.clapper" %% "grizzled-slf4j" % "1.0.1")  // For Scala 2.10 or later

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.16"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
