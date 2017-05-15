class Weights{

  def scoreVec(termFreqs:Map[String, Int], idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[(Int, Double)] = {
    val eta = 1.0
    val mu = 0.013
    val lambda = 0.022
    val absTotalTerms = termFreqs.values.sum
    val l = absTotalTerms

    val termScores = termFreqs.filter{
	case (term, freq) => termIds.contains(term)
      }.map { case (term, freq) => {
      val km1 = termFreqs(term).toDouble - eta
      val idf = idfs(term)

      val w = math.sqrt(idf) /
	  (
	    1.0 +
	    math.pow( (mu / lambda), km1) *
	    math.exp(  -(mu - lambda) * l)
	  )
      (termIds(term), w)
    }}.toSeq
    termScores
  }

  def scoreMat(termFreqs:Map[String, Int], rowIdx:Int, idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[(Int, Int, Double)] = {

    val eta = 1.0
    val mu = 0.013
    val lambda = 0.022
    val absTotalTerms = termFreqs.values.sum
    val l = absTotalTerms
    val termSize = termIds.size

    val termScores = termFreqs.filter{
	case (term, freq) => termIds.contains(term)
      }.map { case (term, freq) => {
      val km1 = termFreqs(term).toDouble - eta
      val idf = idfs(term)

      val w = math.sqrt(idf) /
	  (
	    1.0 +
	    math.pow( (mu / lambda), km1) *
	    math.exp(  -(mu - lambda) * l)
	  )
      (termIds(term), rowIdx, w)
    }}.toSeq

    termScores
  }

  /*
   * Input: [Int, Map[String, Int]]
   * */
  def scoreMatByDocs(
    tfByDocs:DataFrame
    , idfs:Map[String, Double]
    , termIds:immutable.Map[String, Int]
  ): DataFrame = {
    val mat = tfByDocs.flatMap{case (pmid, termFreqs) => {
      scoreMat(termFreqs, pmid, idfs, termIds)
    }}
  }
}
