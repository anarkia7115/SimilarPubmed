package topic

class PmidLength extends PubmedXmlParser{
  override val MAX_ARTICLE = 10
  var absLength: Int = 0
  override def renderText(text: String ,nodeHead: String): Unit = {
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
        case "AbstractText" => {
          absLength = text.split(" ").size
          println("absText: \n" + text)
          println("pmid: \n" + pmid)
          println("length: \n" + absLength.toString)
        }
        case "DescriptorName" => wordList.append(text)
        case _ => ()
      }
  }
}
