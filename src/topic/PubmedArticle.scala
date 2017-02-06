package topic

/**
  * Created by shawn on 1/18/17.
  */
class PubmedArticle(id: Int,
                    title: String,
                    text: String,
                    wordList: List[String]) {
  var pmid: Int = id
  var articleTile: String = title
  var abstractText: String = text
  //var meshHeadingList: List[String] = wordList
  val meshMap : collection.mutable.Map[String, Int] = _
  // initiate empty map
  wordList.foreach( word => {
    meshMap(word) = 1
  })

  def getMeshMap(): collection.mutable.Map[String, Int] = {
    return meshMap
  }

  def getAbstractText(): String = {
    return abstractText
  }
}
