package trie
import scala.collection._

class Node(id:Int= -1) {
  var PhraseId = id

  val Children = mutable.Map[String, Node]()

}
