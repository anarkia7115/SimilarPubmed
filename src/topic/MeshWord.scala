package topic

/**
  * Created by shawn on 2/4/17.
  */
class MeshWord(phrase: String) {

  val mentionPhrase:String = phrase
  var conceptWord: String = _
  var preferredWord: String = _

  def getMentionWords: String ={
    mentionPhrase
  }

  def setPreferredWord(word:String): Unit ={
    preferredWord = word
  }

  def setConceptWord(word:String): Unit ={
    conceptWord = word
  }

}
