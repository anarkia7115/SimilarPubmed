/**
  * Created by shawn on 2/20/17.
  */

package topic

import java.text.SimpleDateFormat
import java.io._

import gov.nih.nlm.nls.metamap.{MetaMapApi, MetaMapApiImpl, Result}
//import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
//import com.typesafe.scalalogging.Logger
import com.typesafe.scalalogging.LazyLogging
//import grizzled.slf4j.{Logger, LazyLogging}
//import slick.driver.H2Driver.api._
//import slick.jdbc.meta.MTable

class ConceptAnalyzer extends LazyLogging{
  val testFilePath =
    "/home/shawn/workspace/public_mm_main/public_mm/test.txt"
  val api = new MetaMapApiImpl()
  //var processResult: Result = _

  //val pubmeds = TableQuery[Pubmeds]
  //implicit var session: Session = _
  //var db: Database = _

  def init(): Unit = {

    // 1. init MetaMap Api
    // new option list
    val theOptions = new ListBuffer[String]
    //theOptions += "-K"
    //theOptions += "-C"
    //theOptions += "-y"
    //theOptions += "-z"
    if (theOptions.nonEmpty) {
      api.setOptions(theOptions.asJava)
    }
    println(api.getOptions)

    // 2. open file
    //val outputFile = "./data/pubmedDatas"
  }

  def process(line:String, pw:PrintWriter): Unit = {

    val pmid = line.split("\t")(0)
    val absTxt = line.split("\t")(1).replaceAll("\t", " ").replaceAll("[^\\x00-\\x7F]", "")
    //val db = Database.forURL("jdbc:h2:./data/pubmed", driver = "org.h2.Driver")
    try{
      val processResult = api.processCitationsFromString(absTxt).get(0)
      extractWords(pmid, processResult, pw)
    }
    catch {
      case ioe: java.lang.IndexOutOfBoundsException => println(api.processCitationsFromString(absTxt))
      case e: Throwable => {
        throw e
        println("Got some other kind of exception!")
      }
    }
    //val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //val logger = Logger
    logger.info("analyzed " + pmid.toString)
  }


  def main() {

    //api.setOptions("-yz")

    // start working
    //process()

    // get mesh list
    var meshMap = new mutable.HashMap[Int, List[MeshWord]]()
    //(currPmids, processResult).zipped.foreach(extractWords)

  }

  // extract matched words, concept words and preferred words, and
  // save them to pubmedArticle
  def extractWords(pmid: String, rst: Result, pw: PrintWriter): Unit = {

    // utterance
    rst.getUtteranceList.asScala.foreach(utterance => {
      // pcm
      utterance.getPCMList.asScala.foreach(pcm => {
        // map
        pcm.getMappingList.asScala.foreach(map => {
          // map ev
          map.getEvList.asScala.foreach(mapEv => {

            val matchedPhrase = mapEv.getMatchedWords.asScala.mkString(" ")

            val conceptName = mapEv.getConceptName
            val preferredName = mapEv.getPreferredName
            val meshId = mapEv.getConceptId
            pw.write("%s\t%s\t%s\t%s\t%s\n".format(
              pmid, 
              matchedPhrase, 
              conceptName, 
              preferredName, 
              meshId))

            //println(matchedPhrase + " => " + preferredName)
            //println(matchedPhrase + " => " + conceptName)

          })
        })
      })
    })
  }

  def after: Unit = {
    //session.close
  }

}
