name := "SimilarPubmed"

version := "1.0"

scalaVersion := "2.11.8"

//scalacOptions := Seq("-optimise", "-Yinline-warnings")

libraryDependencies ++= Seq(
//  "org.slf4j" % "slf4j-api" % "1.7.5" ,
//  "org.slf4j" % "slf4j-simple" % "1.7.5",
//  "com.h2database" % "h2" % "1.3.170",
  "com.typesafe.slick" %% "slick" % "3.1.1"
//  "javax.servlet" % "javax.servlet-api" % "3.1.0", 
  , "org.apache.spark" % "spark-core_2.11" % "2.1.0"
  , "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
  , "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"

  //, "de.tudarmstadt.ukp.dkpro.core" % "de.tudarmstadt.ukp.dkpro.core.opennlp-asl" % "1.6.2"
  //, "de.tudarmstadt.ukp.dkpro.core" % "de.tudarmstadt.ukp.dkpro.core.languagetool-asl" % "1.6.2"
  //, "de.tudarmstadt.ukp.dkpro.core" % "de.tudarmstadt.ukp.dkpro.core.maltparser-asl" % "1.6.2"
  //, "de.tudarmstadt.ukp.dkpro.core" % "de.tudarmstadt.ukp.dkpro.core.io.text-asl" % "1.6.2"
  //, "de.tudarmstadt.ukp.dkpro.core" % "de.tudarmstadt.ukp.dkpro.core.io.conll-asl" % "1.6.2"
)
//  ,"org.scala-lang.modules" % "scala-xml_2.11" % "1.0.5")
//  "org.clapper" %% "grizzled-slf4j" % "1.0.1")  // For Scala 2.10 or later

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.16"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0"

//XitrumPackage.copy("dirToCopy", "fileToCopy")

// exclude
excludeFilter in unmanagedSources := "PubmedXmlParser.scala" || "PmidLength.scala" || "SparkTest.scala"

mainClass := Some("topic.Main")

val buildSettings = Defaults.defaultSettings ++ Seq(
   javaOptions += "-Xmx6G"
)
