package sparkutil

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

class Streaming {
  val spark = SparkSession
    .builder
    .appName("SimilarPubmed")
    .config("spark.master", "local[*]")
    .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  //val conf = new SparkConf().setMaster("local[*]").setAppName("SimilarPubmed")
  val ssc = new StreamingContext(sc, Seconds(1))

  val meshFuzzyFile = "/home/shawn/git/PubmedWordGame/SimilarPubmed/data/expanded_mesh"
  val lines = ssc.textFileStream(meshFuzzyFile)
  //val kvs = lines.map(line => (line.split("\t")(0), ListBuffer(line.split("\t")(1))))
  //val fuzzySet = kvs.reduceByKey(_ ++ _)
  //fuzzySet.saveAsTextFiles("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/fuzzySetResult/result")
  //lines.saveAsTextFiles("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/fuzzySetResult/result")
  lines.print
  ssc.start
  ssc.awaitTermination
  /*
  val spark = SparkSession
    .builder
    .appName("FuzzyTopic")
    .config("spark.master", "local[*]")
    .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  val meshFuzzyRDD = sc.textFile(meshFuzzyFile)
  */
}
