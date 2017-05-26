name := "SimilarPubmed"

version := "1.0"

scalaVersion := "2.11.8"

//scalacOptions := Seq("-optimise", "-Yinline-warnings")

libraryDependencies ++= Seq(
  /*"com.typesafe.slick" %% "slick" % "3.1.1"*/
  "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
  , "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided" 
  , "org.apache.spark" % "spark-streaming_2.11" % "2.1.0" % "provided" 
  /*
  , "org.apache.lucene" % "lucene-core" % "4.6.0"
  , "org.apache.lucene" % "lucene-queries" % "4.6.0"
  , "org.apache.lucene" % "lucene-analyzers-common" % "4.6.0"
  , "org.apache.lucene" % "lucene-queryparser" % "4.6.0"
  , "org.apache.solr" % "solr-solrj" % "4.6.0"
  */
)

//libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.16"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0" % "provided" 
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.0" classifier "models"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0"

//XitrumPackage.copy("dirToCopy", "fileToCopy")

// exclude
excludeFilter in unmanagedSources := "PubmedXmlParser.scala" || "PmidLength.scala" || "SparkTest.scala" || "PorterStemmer.scala" || "UmlsTagger.scala" || "spark-shell.scala" || "FileReader.scala" || "Tables.scala"

mainClass := Some("topic.Main")

val buildSettings = Defaults.defaultSettings ++ Seq(
   javaOptions += "-Xmx6G"
)

//assemblyJarName in assembly := "something.jar"
