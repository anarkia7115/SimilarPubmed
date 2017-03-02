/**
  * Created by shawn on 2/20/17.
  */

package topic

//import org.slf4j.LoggerFactory
//import grizzled.slf4j.{Logger, Logging}

import com.typesafe.scalalogging.LazyLogging
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
    val absOut = "./data/abs_big.txt"
    //val connector = new ConnectDownload(absOut)
    //connector.runAbs

    // 1. compare words
    val analyzedOut = "./data/analyzed_big.txt"
    val cw = new CompareWords(absOut, analyzedOut)
    cw.main
  }
}
