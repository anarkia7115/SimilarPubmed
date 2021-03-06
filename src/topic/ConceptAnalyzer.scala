package topic
import gov.nih.nlm.nls.metamap._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class ConceptAnalyzer {
  val testFilePath =
    "/home/shawn/workspace/public_mm_main/public_mm/test.txt"
  val api = new MetaMapApiImpl()
  var processResult: List[Result] = _

  def process(buffer: List[String]): Unit = {

    val bigBuf = buffer.mkString("\n\n")
    processResult = api.processCitationsFromString(bigBuf).asScala.toList
  }

  def getProcessResult: List[Result] = {
    processResult
  }

  def main {

    // new metamap api
    val api = new MetaMapApiImpl
    // new option list
    val theOptions = new ListBuffer[String]
    theOptions += "-K"
    //theOptions += "-y"
    //theOptions += "-z"

    if (theOptions.nonEmpty) {
      api.setOptions(theOptions.asJava)
    }
    println(api.getOptions)
    //api.setOptions("-yz")

    val result = processResult(0)
    // utterance
    result.getUtteranceList.asScala.foreach(utterance => {
      // pcm
      utterance.getPCMList.asScala.foreach(pcm => {
        // map
        pcm.getMappingList.asScala.foreach(map => {
          // map ev
          map.getEvList.asScala.foreach(mapEv => {

            val matchedPhrase = mapEv.getMatchedWords.asScala.mkString(" ")

            val conceptName = mapEv.getConceptName
            val preferredName = mapEv.getPreferredName

            println(matchedPhrase + " => " + preferredName)
            println(matchedPhrase + " => " + conceptName)
          })
        })
      })
    })
  }

  def getWordsFromResult
}
