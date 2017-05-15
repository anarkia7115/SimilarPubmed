package topic

class XmlTagRemover(tagToRemove:String) extends java.io.Serializable {
  // <AbstractText Label="null" NlmCategory="UNLABELLED">
  // </AbstractText>
  def trim(xmlText: String): String = {
    // front trim
    val headWordIdx = xmlText.indexOfSlice(tagToRemove)
    val headIdx = xmlText.indexOf(">", headWordIdx) + 1
    val tailIdx = xmlText.indexOfSlice("</%s>".format(tagToRemove))
    //println(xmlText.size - tailIdx + ": " + headIdx + "\t" + tailIdx)
    return xmlText.slice(headIdx, tailIdx)
    // back trim
  }
}
