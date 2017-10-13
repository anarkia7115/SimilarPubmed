package topic
import util.control.Breaks._
import scala.io.Source
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.math.exp
import scala.math.log
import scala.math.sqrt
import scala.math.pow
import scala.collection.immutable.ListMap
import java.io._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import utils.Tabulator

class Algorithm {
  def calcIdf(postingList: Map[String, ListBuffer[(Int, Int, Boolean)]], nDoc:Int): 
      Map[String, Double] = {
    // calc final
    //
    // 1. load postingList
    //val ds = new DataSaver()
    //val postingListFile = "./data/posting_list"

    //val postingList = ds.loadPostingList(postingListFile)

    // 2. uniq pmid in postingList, count(distinct pmid)
    //val nDoc = calcDistinctPmid(postingListFile)

    // 3. calculate meshId, ListBuffer.size
    val idfMap = mutable.Map[String, Double]()
    for((meshId, lb) <- postingList) {
      idfMap += (meshId -> log(nDoc.toDouble / lb.size))
    }
    return idfMap.toMap
  }

  def getIdf(idfDict:Map[String, Double], term:String, nDoc:Int):Double = {
    if(idfDict contains term) {
      return idfDict(term)
    }
    else {
      //return log(nDoc)
      return 1
    }
  }

  /*
   * key1: pmid
   * key2: mesh_id
   * value: term_count, is_topic
   * */
  def calcCountMap(
      postingList:Map[String, ListBuffer[(Int, Int, Boolean)]], 
      onlyTopic:Boolean=false): 
      Map[Int, Map[String, Int]] = {

    val countMap = mutable.Map[Int, mutable.Map[String, Int]]()
    for((meshId, lb) <- postingList) {
      for((pmid, termCount, isTopic) <- lb) {

        val smallMap = countMap.getOrElseUpdate(pmid, 
          mutable.Map[String, Int]())
        if (!onlyTopic || isTopic) {
          smallMap += (meshId -> (termCount))
        }
      }
    }

    return countMap.toMap
  }

  def sortFile(inputFile:String, outputFile:String):Unit = {

    val scoreMap = mutable.Map[Int, Double]()
    for(line <- Source.fromFile(inputFile).getLines()) {
      val fields = line.split(": ")
      val pmid = fields(0).toInt
      val score = fields(1).toDouble
      scoreMap(pmid) = score
    }
    val pw = new PrintWriter(new File(outputFile))
    for((pmid, score) <- ListMap(scoreMap.toSeq.sortWith(_._2 > _._2):_*)) {
      pw.write("%s\t%s\n".format(pmid, score))
    }
    pw.close
  }

  def addAbsAnnotation(
      pmid: Int, 
      inputFile: String, 
      outputFile:String, 
      limitLine:Int, 
      conn:Connector):Unit = {

    val xtr = new XmlTagRemover("AbstractText")
    val pw = new PrintWriter(new File(outputFile))
    val (aTitle, absTxtData) = conn.runOneTitleAbs(pmid)
    val absTxt = xtr.trim(absTxtData)
    pw.write(
      """|Origin Artile PMID: %s
         |Title: %s
         |Abstract: %s
         |
         |""".stripMargin.format(pmid, aTitle, absTxt))

    var lineNum = 0
    breakable {
      for(line <- Source.fromFile(inputFile).getLines()) {
        val pmid = line.split("\t")(0).toInt
        val score = line.split("\t")(1).toDouble
        val (aTitle, absTxtData) = conn.runOneTitleAbs(pmid)
        val absTxt = xtr.trim(absTxtData)
        pw.write(
          """|Related Artile PMID: %s, Score: %s
             |Title: %s
             |Abstract: %s
             |
             |""".stripMargin.format(pmid, score, aTitle, absTxt))
        lineNum += 1
        if(lineNum >= limitLine) break
      }
    }
    pw.close
  }

  def addAbsAnnotation(
                        currpmid: Int,
                        inputArray: List[(Int, Double)],
                        spark:SparkSession, 
                        relatedMeshTable:DataFrame,
                        outputFile:String,
                        conn:Connector):Unit = {

    import spark.implicits._
    val xtr = new XmlTagRemover("AbstractText")
    val pw = new PrintWriter(new File(outputFile))
    val (aTitle, absTxtData) = conn.runOneTitleAbs(currpmid)
    val absTxt = xtr.trim(absTxtData)
    pw.write(
      """|Origin Artile PMID: %s
        |Title: %s
        |Abstract: %s
        |
        |""".stripMargin.format(currpmid, aTitle, absTxt))

    var lineNum = 0
    for(line <- inputArray) {
      val relpmid = line._1
      val score = line._2

      val meshTableToPrint = relatedMeshTable
        .filter($"curpmid" === currpmid and $"relpmid" === relpmid)
        .sort(desc("score"))
        .select("mesh", "curscore", "relscore", "score")
      val meshStringToPrint = Tabulator
        .format(
          Seq(meshTableToPrint.columns.toSeq) 
          ++ meshTableToPrint.collect.toSeq.map(s => s.toSeq)
        )

      val (aTitle, absTxtData) = conn.runOneTitleAbs(relpmid)
      val absTxt = xtr.trim(absTxtData)
      pw.write(
        """|Related Artile PMID: %s, Score: %s
          |Title: %s
          |Abstract: %s
          |
          |%s
          |
          |
          |""".stripMargin.format(relpmid, score, aTitle, absTxt, meshStringToPrint))
      lineNum += 1
    }
    pw.close
  }


  def addAbsAnnotation(
                        currpmid: Int,
                        inputArray: List[(Int, Double)],
                        outputFile:String,
                        conn:Connector):Unit = {

    val xtr = new XmlTagRemover("AbstractText")
    val pw = new PrintWriter(new File(outputFile))
    val (aTitle, absTxtData) = conn.runOneTitleAbs(currpmid)
    val absTxt = xtr.trim(absTxtData)
    pw.write(
      """|Origin Artile PMID: %s
        |Title: %s
        |Abstract: %s
        |
        |""".stripMargin.format(currpmid, aTitle, absTxt))

    var lineNum = 0
    for(line <- inputArray) {
      val relpmid = line._1
      val score = line._2
      val (aTitle, absTxtData) = conn.runOneTitleAbs(relpmid)
      val absTxt = xtr.trim(absTxtData)
      pw.write(
        """|Related Artile PMID: %s, Score: %s
          |Title: %s
          |Abstract: %s
          |
          |""".stripMargin.format(relpmid, score, aTitle, absTxt))
      lineNum += 1
    }
    pw.close
  }

  /*
   * key1: mesh_id
   * key2: pmid
   * value: term_count, is_topic
   * */
  def calcCountMap2(
      postingList:Map[String, ListBuffer[(Int, Int, Boolean)]],
      onlyTopic:Boolean=false): 
      Map[String, Map[Int, Int]] = {
    val countMap = mutable.Map[String, Map[Int, Int]]()
    for((meshId, lb) <- postingList) {
      val smallMap = mutable.Map[Int, Int]()
      for(v <- lb) {
        val isTopic = v._3
        // topic sensitive?
        if (!onlyTopic || isTopic) {
          smallMap += (v._1 -> v._2)
        }
      }
      countMap += (meshId -> smallMap.toMap)
    }
    return countMap.toMap
  }

  /*
   * mesh_id:
   *  1. iterm_count
   *  2. is_topic
   * */
  def calcSimilarity(
      m_length: Int, // current abstract length
      smallPostingList: Map[String, (Int, Boolean)], 
      countMap: Map[String, Map[Int, Int]], //postinglist
      lengthMap: Map[Int, Int], 
      idfMap: Map[String, Double], 
      nDoc: Int
    ): Map[Int, Double] = {
    val weightMap = mutable.Map[Int, mutable.Map[String, Double]]()
    val relatedArticle = ListBuffer[Int]()
    val meshList = smallPostingList.keys

    for ((m_term, (m_termCount, isTopic))  <- smallPostingList) {

      val m_idf = getIdf(idfMap, m_term, nDoc)
      val m_weight = singleWeight(m_termCount, m_length, m_idf)


      if (countMap contains m_term) {

        for ((pmid, termCount) <- countMap(m_term)) {

          relatedArticle += pmid
          val weight = singleWeight(termCount, lengthMap(pmid), m_idf)
          val smallMap = 
            weightMap.getOrElseUpdate(pmid, mutable.Map[String, Double]())
          smallMap += (m_term -> m_weight * weight)
        }
      }
    }

    val resultMap = mutable.Map[Int, Double]()
    for(pmid <- relatedArticle) {
      val currWeights = weightMap(pmid)
      var totWeight = 0.0
      for((meshId, w) <- currWeights) {
        totWeight += w
      }
      resultMap += (pmid -> totWeight)
    }

    return resultMap

  }

  def singleWeight(termCount:Int, absLength:Int, idf:Double): Double = {
    val eta = 1.0
    val km1 = termCount.toDouble - 1.0
    val mu = 0.013
    val lambda = 0.022
    val l = absLength

    val w = sqrt(idf) / 
            (
              1.0 + 
              pow( (mu / lambda), km1) * 
              exp(  -(mu - lambda) * l)
            )

    return w
  }

  /*
   * pmid -> length
   * */
  def calcArticleLength(absFile: String): Map[Int, Int] = {
    val lengthMap = mutable.Map[Int, Int]()
    for (l <- Source.fromFile(absFile).getLines()) {
      val fields = l.split("\t")
      val pmid = fields(0).toInt
      val absTxt = fields(1)
      val length = absTxt.split(" ").size
      lengthMap += (pmid -> length)
    }
    return lengthMap.toMap
  }

  def getAbsLength(absText: String):Int = {
    return absText.split(" ").size
  }

  def loadPmidSet(inputFiles:List[String], pmidIndex:Int=0): Set[Int] = {

    val pmidSet = mutable.Set[Int]()

    for (fn <- inputFiles) {
      for (l <- Source.fromFile(fn).getLines()) {

        val pmid = l.split("\t")(pmidIndex).toInt
        pmidSet += pmid
      }
    }
    return pmidSet.toSet
  }

  def calcDistinctPmid(inputFile:String): Int = {

    val pmidSet = mutable.Set[Int]()

    for (l <- Source.fromFile(inputFile).getLines()) {

      val pmid = l.split("\t")(1).toInt
      pmidSet += pmid
    }
    return pmidSet.size
  }

  // TODO: line.rstrip?
  def checkTopic(line:String, topicTree:mutable.Map[Int, mutable.Set[String]]):Boolean = {
    // input:   concept
    //          preferred
    // output:  isTopic boolean
    val fields = line.split("\t")
    val pmid = fields(0).toInt
    val matched = fields(1)
    val concept = fields(2)
    val preferred = fields(3)
    val topicSet = topicTree.getOrElse(pmid, mutable.Set[String]()) 
    if ((topicSet contains matched) ||
        (topicSet contains concept) ||
        (topicSet contains preferred)) {
      return true
    }
    else {
      return false
    }

  }

  def checkTopic(
      matched:String, 
      concept:String,
      preferred: String,
      topicSet:Set[String]):Boolean = {
    // input:   concept
    //          preferred
    // output:  isTopic boolean

    if ((topicSet contains matched) ||
        (topicSet contains concept) ||
        (topicSet contains preferred)) {
      return true
    }
    else {
      return false
    }
  }



  def createTopicTree(topicFile:String):mutable.Map[Int, mutable.Set[String]] = {
    val topicTree = mutable.Map[Int, mutable.Set[String]]()
    //val topicSet  = mutable.Set[String]()
    for (line <- Source.fromFile(topicFile).getLines()) {
      val fields = line.split("\t")
      val pmid = fields(0).toInt
      val topicName = fields(1)
      val topicSet = topicTree.getOrElseUpdate(pmid, mutable.Set[String]())
      topicSet += topicName
    }

    return topicTree
  }

  def appendToPostingList(currMesh: mutable.Map[String, (Int, Int, Boolean)],
    postingList: mutable.Map[String, ListBuffer[(Int, Int, Boolean)]]): Unit ={
      for ((k, v) <- currMesh) {
        val lb = postingList.getOrElseUpdate(k, ListBuffer[(Int, Int, Boolean)]())
        lb += v
      }
  }

  def createPostingList(topicFile: String, analyzedFile:String):
      mutable.Map[String, ListBuffer[(Int, Int, Boolean)]] = {
    val topicTree = createTopicTree(topicFile)
    var currPmid = 0
    var isTopic = false
    val postingList = mutable.Map[String, ListBuffer[(Int, Int, Boolean)]]()
    /*
      mesh_id:
        _1 pmid
        _2 count
        _3 is_topic
    */
    val currMesh = mutable.Map[String, (Int, Int, Boolean)]()

    for (line <- Source.fromFile(analyzedFile).getLines()) {
      val fields = line.split("\t")
      /*
      pw.write("%s\t%s\t%s\t%s\t%s\n".format(
        pmid, 
        matchedPhrase, 
        conceptName, 
        preferredName, 
        meshId))
      */
      val pmid = fields(0).toInt
      val matched = fields(1)
      val concept = fields(2)
      val preferred = fields(3)
      val meshId = fields(4)
      if(checkTopic(line, topicTree)){
        isTopic = true
      }
      else {
        isTopic = false
      }
      if (pmid != currPmid && currPmid !=0 ){
        // package current 
        appendToPostingList(currMesh, postingList)
        currMesh.clear()
        currPmid = pmid
      }
      else if (currPmid == 0) {
        currPmid = pmid
      }
      val cm = currMesh.getOrElse(meshId, (pmid, 0, isTopic))
      val freq = cm._2 + 1
      currMesh(meshId) = (pmid, freq, isTopic)
    }

    return postingList
  }

  def limitTopic(smallPostingList:mutable.Map[String, (Int, Boolean)]
      ):mutable.Map[String, (Int, Boolean)] = {
    val resultPostingList = mutable.Map[String, (Int, Boolean)]()
    for((mesh, (termCount, isTopic)) <- smallPostingList) {
      if(isTopic) {
        resultPostingList(mesh) = (termCount, isTopic)
      }
    }

    return resultPostingList
  }

  /*
   * mesh_id:
   *  1. term_count
   *  2. is_topic
   *
   * */
  def createSmallPostingList(topicList: List[String], 
      meshResult: List[(String, String, String, String)]):
      mutable.Map[String, (Int, Boolean)] = {
    val topicSet = topicList.toSet
    //var currPmid = 0
    //only one pmid

    val smallPostingList = mutable.Map[String, (Int, Boolean)]()
    /*
      mesh_id:
        _1 count
        _2 is_topic
    */
    for (tuple <- meshResult) {
      /*
      pw.write("%s\t%s\t%s\t%s\t%s\n".format(
        pmid, 
        matchedPhrase, 
        conceptName, 
        preferredName, 
        meshId))
      */
      val matched = tuple._1
      val concept = tuple._2
      val preferred = tuple._3
      val meshId = tuple._4

      var isTopic = false

      if(checkTopic(matched, concept, preferred, topicSet)){
        isTopic = true
      }
      else {
        isTopic = false
      }

      val cm = smallPostingList.getOrElse(meshId, (0, isTopic))
      val freq = cm._1 + 1
      smallPostingList(meshId) = (freq, isTopic)
    }

    return smallPostingList
  }
}
