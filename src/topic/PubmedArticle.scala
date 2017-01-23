/**
  * Created by shawn on 1/18/17.
  */
package topic {
  class PubmedArticle(id: Int,
                      title: String,
                      text: String,
                      wordList: List[String]) {
    var pmid: Int = id
    var articleTile: String = title
    var abstractText: String = text
    var meshHeadingList: List[String] = wordList

    def getWordList(): List[String] = {
      return meshHeadingList
    }

    def getAbstractText(): String = {
      return abstractText
    }
  }
}
