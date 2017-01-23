import gov.nih.nlm.nls.metamap.Result

import scala.collection.mutable.ListBuffer

/**
  * Created by shawn on 1/19/17.
  */
package topic {
  class CompareWords {
    val a = new PubmedXmlParser()
    val b = new ConceptAnalyzer()
    def main(parser: PubmedXmlParser,
             analyzer: ConceptAnalyzer): List[Result] = {
      //val parser = new PubmedXmlParser()
      val pubmedArticleList = parser.main()
      val absList = new ListBuffer[String]

      pubmedArticleList.foreach(pa => {
        absList.append(pa.getAbstractText())
      })

      //val analyzer = new ConceptAnalyzer()

      analyzer.process(absList.toList)
      val processResult = analyzer.getProcessResult()
      return processResult
    }

  }
}
