package streaming;

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PubmedReceiver {
  val spark = SparkSession
    .builder
    .appName("c++ streaming")
    .config("spark.master", "spark://node19:7077")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  import spark.implicits._
  def main(args: Array[String]) {
    if (args.length <8) {
      System.err.println("Usage: PubmedReceiver <directoryToMonitor> <microbatchtime> <binFile> <matFile> <threadNum> <partitionNum> <idfsPath> <termIdsPath>")
      System.exit(1)
    }

    val directoryToMonitor = args(0)
    val microbatchtime = args(1).toInt
    val binFile = args(2)
    val matFile = args(3)
    val threadNum = args(4)
    val partitionNum = args(5)
    val idfsPath = args(6)
    val termIdsPath = args(7)

    //val sparkConf = new SparkConf().setAppName("PubmedReceiver")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(microbatchtime))

    val directoryStream = ssc.textFileStream(directoryToMonitor)
    directoryStream.foreachRDD { fileRdd =>
      if (fileRdd.count() != 0)
        processNewFile(fileRdd, binFile, matFile, threadNum, partitionNum, idfsPath, termIdsPath)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def processNewFile(
    fileRDD: RDD[String], 
    binFile:String, 
    matFile:String, 
    threadNum:String, 
    partitionNum:String, 
    idfsPath:String, 
    termIdsPath:String): Unit = {

    val sc = spark.sparkContext
    val bow = new algorithms.BagOfWords(spark)
    val outputPath = fileRDD.filter(_.split("\t").size == 1).take(1)(0)
    val textRDD = bow.load(fileRDD.filter(_.split("\t").size == 3))
    val normedDf = bow.preprocess(textRDD)
    val termsDf = bow.getAndCacheDocFreqs(normedDf)
    val tms = new algorithms.Terms(spark, termsDf)
    val tfByPmid = tms.tfByDoc()
    //val idfsPath = "file:///home/shawn/data/SimilarPubmed/idfsDf3"
    //val termIdsPath = "file:///home/shawn/data/SimilarPubmed/termIdsDf3"
    val idfs = spark.read.load(idfsPath).as[(String, Double)].rdd.collectAsMap
    val termIds = spark.read.load(termIdsPath).as[(String, Int)].rdd.collectAsMap
    val bIdfs = sc.broadcast(idfs)
    val bTermIds = sc.broadcast(termIds)

    val wt = new algorithms.Weights(spark)
    val mat = wt.scoreMatByDocs(tfByPmid, bIdfs.value, bTermIds.value)
    mat.map(attrs => (
      attrs.getInt(1), 
      Seq("%d,%f".format(attrs.getInt(0), attrs.getDouble(2))))).
    rdd.reduceByKey(_++_).
    map({ case (pmid, twSeq) => (
      "%d\t%s".format(pmid, twSeq.mkString("\t") ))
    }).repartition(partitionNum.toInt).pipe(
      Seq(binFile, 
        matFile, 
        threadNum)).saveAsTextFile(outputPath)

  }
}
