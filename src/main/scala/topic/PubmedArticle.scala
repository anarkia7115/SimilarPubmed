/**
  * Created by shawn on 2/20/17.
  */

package topic

/**
  * Created by shawn on 1/18/17.
  */
class PubmedArticle(id: Int,
                    title: String,
                    text: String,
                    wordList: List[String]
                   ) {
  var pmid: Int = id
  var articleTile: String = title
  var abstractText: String = text
  //var meshHeadingList: List[String] = wordList
  val meshSet = wordList.toSet
  //var matchedList = List[String]()
  //var conceptList = List[String]()
  //var preferredList = List[String]()
}