/**
  * Created by shawn on 2/20/17.
  */

package topic

//import org.slf4j.LoggerFactory
//import grizzled.slf4j.{Logger, Logging}

import com.typesafe.scalalogging.LazyLogging
import scala.collection._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.xml._
import java.io._
//import org.slf4j.LoggerFactory
//import org.slf4j.impl.SimpleLogger

//import scala.slick.driver.H2Driver.simple._

//import com.typesafe.slick

/**
  * Created by shawn on 2/13/17.
  */

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    // 1. compare words
    //val cw = new CompareWords()
    //cw.main
    //
    // 2. calculate idf
    //
    // 3. calculate pmid length
    //val pl = new PmidLength()
    //pl.main
    //logger.info("what the fuck!")

    // 4. test Connector
    //val conn = new Connector()


    // 5. compare words
    val analyzedOut = "./data/analyzed.txt"
    //val cw = new CompareWords(absFile, analyzedOut)
    //cw.main

    // 6. run topic match
    //runTopic()

    // (7). run topic tree
    //val testPmid = 24118693
    //val topicSet = testTopicTree(testPmid)
    //println(topicSet)

    // 8. posting list
    //testPostingList()
    val topicFile = "/home/shawn/git/SimilarPubmed/data/pmid_topic"
    val al = new Algorithm()
    //val postingList = al.createPostingList(topicFile, analyzedOut)

    // 9. save postingList
    val ds = new DataSaver()
    val postingListFile = "./data/posting_list"

    //ds.savePostingList(postingList, postingListFile)

    // 10. load postingList
    //testPostingList(postingList)


    val usedPmidFile1 = "./data/abs.txt"
    val usedPmidFile2 = "./data/abs_mar13.txt"
    //val usedPmidSet = al.loadPmidSet(List(usedPmidFile1, usedPmidFile2), 0)
    val absFile = "./data/abs_apr4.txt"
    val meshFile = "./data/mesh_apr4.txt"
    val conn = new Connector()
    //conn.runAbs(absFile)
    runTopic(absFile, meshFile)

    /*
    val inputAbsFile = args(0)
    println(inputAbsFile)
    val testSparkHdfs = "hdfs://localhost:9000/user/shawn/test_spark_result"
    val rstHdfs = "hdfs://localhost:9000/user/shawn/%s".format(args(1))
    //val rstHdfs = "hdfs://localhost:9000/user/shawn/abs_count_result"
    val sConf = new SparkConf()
                  .setMaster("local[*]")
                  .setAppName("SimilarPubmed")

    val testSparkFile = "file:///home/shawn/git/SimilarPubmed/data/test_spark.txt"

    // init spark
    val sc = new SparkContext(sConf)
    */
    //val absSc = sc.textFile(absLocal)
    //val testSparkSc = sc.textFile(testSparkFile)
    //val absSmall = "./data/abs_small.txt"
    //val analyzerSparkSc = sc.textFile(inputAbsFile, 100)
    // init analyzer
    //val analyzer = new ConceptAnalyzer()
    //analyzer.init
    // init read abs file
    /*
    val testSpark = testSparkSc.flatMap(
      line => {
        val k = line.split("\t")(0)
        val v = line.split("\t")(1)
        simulateExtractWordProcess(v).map(vv => (k, vv))
      }
    )
    */

   /*
    var lineNum = 0
    val analyzerSpark = analyzerSparkSc.flatMap(
      line => {
        val fq = new FileQueue("/tmp/portQueue", 8066 to 8071)
        lineNum += 1
        val k = line.split("\t")(0)
        val v = line.split("\t")(1)
        val p = fq.borrowOne
        while (p == 0) {

          println("ports are full, waiting for new")
          Thread sleep 1000
          val p = fq.borrowOne
        }
        val analyzer = new ConceptAnalyzer(p)
        val rst = analyzer.process(v).map(vv => (k, vv))
        fq.returnOne(p)
        println("processed %s".format(lineNum))
        rst
      }
    )
    //testSpark.saveAsTextFile(rstHdfs)
    analyzerSpark.saveAsTextFile(rstHdfs)
    sc.stop()
    */
    //testSpark.saveAsTextFile(testSparkHdfs)
    //println(simulateExtractWordProcess("aab"))

    //val counts = absSc.flatMap(line => line.split("\t"))
    //             .map(word => (word, 1))
    //             .reduceByKey(_ + _)
    //counts.saveAsTextFile(rstHdfs)

    //conn.runExcludeAbs(usedPmidSet, absFile)

    // preparation
    //val countMap = al.calcCountMap2(postingList, true)
    /*
    val postingList = ds.loadPostingList(postingListFile)
    val countMap = al.calcCountMap2(postingList, false)
    val lengthMap = al.calcArticleLength(absFile)
    val nDoc = al.calcDistinctPmid(postingListFile)
    val idfMap = al.calcIdf(postingList, nDoc)

    println("prepared")
    */

    // 11. calculate idf
    //val idfList = al.calcIdf(postingList)
    /*
    for (pmid <- pmidList) {
      val outputFile = "%s_not_accurate.txt".format(pmid)
      findSimilarPmids(pmid, conn, al, countMap, lengthMap, nDoc, idfMap, outputFile)
    }
    */
  }

  def simulateExtractWordProcess(line:String): 
    List[(String, String, String, String)] = {
      val rst = ListBuffer[(String, String, String, String)]()
      for(xx <- 1 to 5){
        val xline = line + xx
        val yl = (
          xline + 1,
          xline + 2,
          xline + 3,
          xline + 4
        )
        rst += yl
      }
      return rst.toList
  }

  def sortResult(al:Algorithm, conn:Connector):Unit = {
    val pmidList = List(27040497, 27783602, 25592537, 24742783, 20038531, 24460801, 22820290, 23716660, 26524530, 26317469)

    // sort result
    import java.nio.file.{Paths, Files}
    for (pmid <- pmidList) {
      //val onePmid = "24742783"
      //val fileWithTopic = "./data/raw_result/%s.txt".format(pmid)
      //val fileNoTopic = "./data/raw_result/%s_not_accurate.txt".format(pmid)
      val fileWithTopicSort = "./data/sorted/%s.txt".format(pmid)
      val fileNoTopicSort = "./data/sorted/%s_no_topic.txt".format(pmid)
      val fileResultWithTopic = "./data/result/%s.txt".format(pmid)
      val fileResultNoTopic = "./data/result/%s_no_topic.txt".format(pmid)
      if(Files.exists(Paths.get(fileWithTopicSort))) {
        al.addAbsAnnotation(
          pmid, 
          fileWithTopicSort, 
          fileResultWithTopic,
          10,
          conn
        )
      }
      if(Files.exists(Paths.get(fileNoTopicSort))) {
        al.addAbsAnnotation(
          pmid, 
          fileNoTopicSort, 
          fileResultNoTopic,
          10,
          conn
        )
      }
    }
  }

  def findSimilarPmids(
    pmid: Int, 
    conn: Connector, 
    al: Algorithm, 
    countMap: Map[String, Map[Int, Int]], 
    lengthMap: Map[Int, Int], 
    nDoc: Int, 
    idfMap: Map[String, Double], 
    outputFile: String): Unit = {

    val absTxt = conn.runOneAbs(pmid)
    if(absTxt == "") {
      println("%s no abstract".format(pmid))
      return
    }

    val smallTopicList = conn.runOneTopic(pmid)

    val ca = new ConceptAnalyzer()
    val smallMeshResult = ca.process(absTxt)

    val smallPostingList = al.createSmallPostingList(
        smallTopicList, 
        smallMeshResult
        )

    val m_length = al.getAbsLength(absTxt)
    val targetPostingList = smallPostingList
    //val targetPostingList = al.limitTopic(smallPostingList)

    println("calculate resultMap")
    val resultMap = al.calcSimilarity(
      m_length, 
      targetPostingList,
      countMap, 
      lengthMap, 
      idfMap, 
      nDoc
      )
    println(resultMap.size)
    val pw = new PrintWriter(new File(outputFile))
    for ((k, v) <- resultMap) {
      pw.write("%s: %s\n".format(k, v))
    }
    pw.close
  }

  def testPostingList(
      postingList: mutable.Map[String, ListBuffer[(Int, Int, Boolean)]]): 
      Unit = {
    val meshId = "C0039829"
    println(postingList(meshId))
  }

  def testTopicTree(pmid:Int): Boolean = {
    val al = new Algorithm()
    val topicFile = "./data/pmid_topic"
    val topicTree = al.createTopicTree(topicFile)
    val matched = "Risk Factors aa"
    return topicTree(pmid) contains matched
  }

  def runTopic(analyzedOut:String, pmidTopicFile:String): Unit = {
    val conn = new Connector()
    val pmidTable = "focus_pmid"
    /*
    val uniqPmidOut = "./data/uniq_pmid"
    conn.generatePmidFocus(analyzedOut, uniqPmidOut)
    logger.info("pmidFocus generated")

    //val pmidTopicFile = "./data/pmid_topic"
    conn.createPmidFocusTable(pmidTable)
    logger.info("focus_pmid created")
    conn.insertRecords(uniqPmidOut, pmidTable)
    logger.info("insert finished")
    */
    conn.runTopic(pmidTable, pmidTopicFile)
    logger.info("topic finished")
  }
}
