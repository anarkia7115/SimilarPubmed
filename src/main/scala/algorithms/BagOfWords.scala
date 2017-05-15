package algorithms

class BagOfWords {

  /*
   * From local/hdfs to rdd
   * */
  def load(inputPath:String, sc:SparkContext): RDD = {
    val cd = new render.CleanDocs(inputPath)
    val cleanRdd = cd.getRdd(sc)
    return cleanRdd
  }

  /*
   * Lemmatizing and stopwords removing
   * */
  def preprocess(textRdd:RDD): RDD = {
    // lemmatizer
    val ler = new algorithms.Lemmatizer()
    val lemmatized = ler.lemmatize(textRdd).toDF("pmid", "title_words", "abs_words")

    // stopwords remover
    import org.apache.spark.ml.feature.StopWordsRemover
    val remover_title = new StopWordsRemover().
      setInputCol("title_words").
      setOutputCol("cleaned_title_words")
    val remover_abs   = new StopWordsRemover().
      setInputCol("abs_words").
      setOutputCol("cleaned_abs_words")

    val normedRdd = remover_abs.transform(remover_title.transform(lemmatized))
    return normedRdd
  }
}
