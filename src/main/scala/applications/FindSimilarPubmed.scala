package applications;

import java.io.{BufferedReader, InputStreamReader, File, PrintWriter}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FindSimilarPubmed {
  val spark = SparkSession
    .builder
    .appName("find similar pubmed")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  import spark.implicits._
  def main(args: Array[String]) {
    if (args.length <8) {
      System.err.println("Usage: PubmedReceiver <inputDocFile> <microbatchtime> <binFile> <matFile> <threadNum> <partitionNum> <idfsPath> <termIdsPath>")
      System.exit(1)
    }

    val inputDocFile = args(0)
    val microbatchtime = args(1).toInt
    val binFile = args(2)
    val matFile = args(3)
    val threadNum = args(4)
    val partitionNum = args(5)
    val idfsPath = args(6)
    val termIdsPath = args(7)
    val outputPath = args(8)

    val sc = spark.sparkContext
    val docRdd = sc.textFile(inputDocFile)
    processNewFile(docRdd, binFile, matFile, threadNum, partitionNum, idfsPath, termIdsPath, outputPath)

  }

  def processNewFile(
    fileRDD: RDD[String], 
    binFile:String, 
    matFile:String, 
    threadNum:String, 
    partitionNum:String, 
    idfsPath:String, 
    termIdsPath:String,
    outputPath:String
  ): Unit = {

    val sc = spark.sparkContext
    val bow = new algorithms.BagOfWords(spark)
    //val outputPath = "hdfs://soldier1:9000/result/pmidTw"
    val outputTag = "output"
    val callBackPathTag = "callback"
    //val outputPath = fileRDD.filter(row => row.split("\t").size == 2 && row.split("\t")(0) == outputTag).take(1)(0).split("\t")(1)
    val pw = new java.io.PrintWriter(new java.io.File(outputPath))
    //val callBackPath = fileRDD.filter(row => row.split("\t").size == 2 && row.split("\t")(0) == callBackPathTag).take(1)(0)
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
    val inputRdd = mat.map(attrs => (
      attrs.getInt(1), 
      Seq("%d,%f".format(attrs.getInt(0), attrs.getDouble(2))))).
    rdd.reduceByKey(_++_).
    map({ case (pmid, twSeq) => (
      "%d\t%s".format(pmid, twSeq.mkString("\t") ))
    })

    if (outputPath.startsWith("hdfs")) {

      System.err.println("output print to hdfs file")
      System.err.println("before print to %s".format(outputPath))
      inputRdd.repartition(partitionNum.toInt).pipe(
        Seq(binFile, 
          matFile, 
          threadNum)).saveAsTextFile(outputPath)
    }
    else {

      System.err.println("output print to local file")
      val localStringArray = inputRdd.repartition(partitionNum.toInt).pipe(
        Seq(binFile, 
          matFile, 
          threadNum)).sortBy(row=>row.split("\t")(0)).collect()
      //val pw = new java.io.PrintWriter(new java.io.File(outputPath))
      localStringArray.foreach(pw.println)
      pw.close()
    }
  }
}

