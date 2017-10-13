/**
  * Created by shawn on 2/20/17.
  */

  package topic

  import scala.collection.mutable.ListBuffer
  import scala.io.Source
  import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}

  /**
    * Created by shawn on 1/17/17.
    */
  class PubmedXmlParser {
    val MAX_ARTICLE = 10000
    val xmlPath =
      "/home/shawn/data/2004_TREC/xml_medline/2004_TREC_XML_MEDLINE_A.xml"

    // current article parser
    var articleNum: Int = 0

    // article attributes
    var pmid: Int = 0
    var title: String = new String()
    var absText: String = new String()
    var wordList: ListBuffer[String] = new ListBuffer[String]

    var isStart: Boolean = true

    // article list
    var articleList: ListBuffer[PubmedArticle] = new ListBuffer[PubmedArticle]

    def main(): List[PubmedArticle] = {
      val xml = new XMLEventReader(Source.fromFile(xmlPath))
      parse(xml)
      return articleList.toList
    }

    def printText(text: String, nodeHead: String): Unit = {
      nodeHead match {
        case "PMID" => println("PMID: " + text)
        case "MedlineCitationSet" => {
          println("-----start/end-----")
          println("Article Number: " + this.articleNum)
          articleNum = articleNum + 1
        }
        case "ArticleTitle" => println("  ArticleTitle: " + text)
        case "AbstractText" => println("  Abstract: " + text)
        case "DescriptorName" => println("  MeshHeading: " + text)
        case _ => ()
      }
    }

    def renderText(text: String, nodeHead: String): Unit = {
      nodeHead match {
        case "PMID" => {
          try {
            pmid = text.toInt
          } catch {
            case e: Exception => {
              println("pmid: " + text + " Error!")
              return
            }
          }
        }
        case "MedlineCitationSet" => {
          if (isStart) {
            // begin article
            // do nothing
          } else {
            // end article
            val curArticle =
            buildArticle(pmid, title, absText, wordList.toList)
            articleList.append(curArticle)
            // clear word list
            wordList.clear()
          }
          isStart = !isStart

          println("Article Number: " + this.articleNum)
          articleNum = articleNum + 1
        }
        case "ArticleTitle" => title = text
        case "AbstractText" => absText = text
        case "DescriptorName" => wordList.append(text)
        case _ => ()
      }
    }

    def buildArticle(id: Int,
                     title: String,
                     text: String,
                     wordList: List[String]): PubmedArticle = {
      return new PubmedArticle(id, title, text, wordList)

    }

    def parse(xml: XMLEventReader): Unit = {
      def loop(currNode: List[String]): Unit = {
        //println("current node: " + currNode.toString)
        if (xml.hasNext) {
          xml.next match {
            case EvElemStart(_, label, _, _) =>
              //println("Start element: " + label)
              if (this.articleNum > MAX_ARTICLE) {
                return
              } else {
                loop(label :: currNode)
              }
            case EvElemEnd(_, label) =>
              //println("End element: " + label)
              if (this.articleNum > MAX_ARTICLE) {
                return
              } else {
                loop(currNode.tail)
              }
            case EvText(text) =>
              if (currNode.size != 0) {
                renderText(text, currNode.head)
              }
              if (this.articleNum > MAX_ARTICLE) {
                return
              } else {
                loop(currNode)
              }
            case _ => loop(currNode)
          }
        }

      }
      loop(List.empty)
    }

}
