package trie
import scala.collection._
class Trie {

  def addPhrase (root:Node, phrase:String, phraseId:Int) {

    var node = root

    val words = phrase.split(' ');

    var i = 0
    words.foreach(word => {

      if(node.Children.contains(word) == false) {
        node.Children(word) = new Node()
      }

      node = node.Children(word)

      if(i == words.size - 1) {
        node.PhraseId = phraseId
      }
      i = i + 1
    })
  }

  def treeSearch(root:Node, node:Node, foundPhrases:mutable.ListBuffer[Int], word:String) :Node = {
    var node2 = node

    if (node.Children.contains(word)) {
      node2 = node.Children(word)
    }
    else {
      if (node.PhraseId != -1) {
        foundPhrases += node.PhraseId
      }

      if (node == root) {
        // pass
      }
      else {
        node2 = root
        treeSearch(root, node2, foundPhrases, word)
      }
    }

    return node2
  }

  def findPhrases(root:Node, textBody:String):List[Int] ={

    var node = root
    val foundPhrases = mutable.ListBuffer[Int]()

    val words = textBody.split(' ')
    var i = 0

    words.foreach(word => {
      node = treeSearch(root, node, foundPhrases, word)
    })

    if (node.PhraseId != -1) {
      foundPhrases += node.PhraseId
    }

    return foundPhrases.toList
  }
}
