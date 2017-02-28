/**
  * Created by shawn on 2/20/17.
  */

package topic

/**
  * Created by shawn on 2/4/17.
  */
class MeshWord(phrase: String, concept: String, preferred: String, id: String) {

  var mentionPhrase: String = phrase
  var conceptName: String = concept
  var preferredName: String = preferred
  var meshId: String = id

  def getMentionWords: String = {
    mentionPhrase
  }

}