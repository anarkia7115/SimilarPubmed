package algorithms
import org.apache.spark.sql.DataFrame

// Row(pmid, Seq[terms])
class Terms(df:DataFrame) {

  /*
   * Term Frequency Map for every doc.
   *
   * */
  def tfByDoc(): DataFrame = {

    df.map{ case (pmid, terms) => {
      // init an empty TF map
      val tf = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      // tf by doc
      (pmid, tf.toMap)
    }}
  }

  /*
   * Every term count once per docs, 
   * even if the term occurs multiple 
   * times in the same doc.
   *
   * */
  def docFreqs(): DataFrame = {
    // calc from origin df
    df.foldLeft(new HashMap[String, Int]()) {
      (map, (pmid, terms)) => {
        // distinct occurences
        terms.toSet.foreach( term => {
          map += term -> map.getOrElse(term, 0) + 1
        })
      }
    }
  }

  def docFreqs(tfbd:DataFrame): DataFrame = {
    // calc from tfByDoc Df
    tfbd.flatMap(_._2.keySet).map((_, 1)).reduceByKey(_ + _)
  }

  /*
   * Inverse Document Frequency
   * Need to calculate Document Frequency(docFreqs) 
   * first.
   * Before doing this, docFreqs should be limited 
   * by freqs, freqs too low should be discard
   *
   * */
  def idf(docf:DataFrame): Map[String, Long] = {
    // number of all documents
    val numDocs = df.count
    docf.map {
      case (term, count) => (term, math.log(numDocs.toDouble / count))
    }
  }
}
