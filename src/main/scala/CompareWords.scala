/**
  * Created by shawn on 2/20/17.
  */

package topic

import gov.nih.nlm.nls.metamap.Result

import scala.collection.mutable.ListBuffer
//import grizzled.slf4j.{Logger, Logging}
import com.typesafe.scalalogging.LazyLogging

import java.io._
import scala.io.Source

/**
  * Created by shawn on 1/19/17.
  */

class CompareWords extends LazyLogging{
  val parser = new PubmedXmlParser()
  val inputFile = "./data/abs.txt"
  val outputFile = "./data/analyzed.txt"

  def main: Unit = {
    //val parser = new PubmedXmlParser()
    //val pubmedArticleList = parser.main()
    //val absList = new ListBuffer[String]

    // multi small buf
    println("start multi small buf")
    val t0_b = System.nanoTime()
    var articleNumbers = 0
    //println("article size" + pubmedArticleList.size.toString)
    val analyzer = new ConceptAnalyzer()
    analyzer.init

    val pw = new PrintWriter(new File(outputFile))
    for (line <- Source.fromFile(inputFile).getLines()){
      analyzer.process(line, pw)
      articleNumbers = articleNumbers + 1
      logger.info("analyzed " + articleNumbers.toString + " articles.")
    }
    /*
    pubmedArticleList.foreach(pa => {
      analyzer.process(pa)
    })
    */
    analyzer.after
    val t1_b = System.nanoTime()
    println("Elapsed time: " + (t1_b - t0_b) * 1e-9 + " secs")

  }

}
