package topic

import gov.nih.nlm.nls.metamap.Result

import scala.collection.mutable.ListBuffer

/**
  * Created by shawn on 1/19/17.
  */

class CompareWords {
  val parser = new PubmedXmlParser()
  val analyzer = new ConceptAnalyzer()

  def main: List[Result] = {
    //val parser = new PubmedXmlParser()
    val pubmedArticleList = parser.main()
    val absList = new ListBuffer[String]

    pubmedArticleList.foreach(pa => {
      //absList.append(pa.getAbstractText())
      val absText = pa.getAbstractText()
      val meshSet = pa.getMeshMap()
    })

    //val analyzer = new ConceptAnalyzer()

    analyzer.process(absList.toList)
    val processResult = analyzer.getProcessResult
    analyzer.main
    processResult
  }

}

