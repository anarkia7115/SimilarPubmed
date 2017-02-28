package topic

class XmlTagRemover(tagToRemove:String){
  // <AbstractText Label="null" NlmCategory="UNLABELLED">
  // </AbstractText>
  def trim(xmlText: String): String = {
    // front trim
    val headWordIdx = xmlText.indexOfSlice(tagToRemove)
    val headIdx = xmlText.indexOf(">", headWordIdx) + 1
    val tailIdx = xmlText.indexOfSlice("</AbstractText>")
    //println(xmlText.size - tailIdx + ": " + headIdx + "\t" + tailIdx)
    return xmlText.slice(headIdx, tailIdx)
    // back trim
  }
}
