package algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection._

class BagOfWords(spark:SparkSession) {
    import spark.implicits._

  /*
   * From local/hdfs to rdd
   * */
  def load(inputRDD:RDD[String]): RDD[(Int, String, String)] = {
    val cd = new render.CleanDocs(spark:SparkSession)
    val cleanRdd = cd.getRdd(inputRDD)
    return cleanRdd
  }

  def load(inputPath:String): RDD[(Int, String, String)] = {
    val cd = new render.CleanDocs(spark:SparkSession)
    val cleanRdd = cd.getRdd(inputPath)
    return cleanRdd
  }

  /*
   * Lemmatizing and stopwords removing
   * */
  def preprocess(textRdd:RDD[(Int, String, String)]): DataFrame = {
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

    val normedDf = remover_abs.transform(remover_title.transform(lemmatized))
    return normedDf
  }

  /* 
   * Get and cache docFreqs 
   * */
  def getAndCacheDocFreqs(normedDf: DataFrame): DataFrame = {
    // merge title and abs
    val termsDf = normedDf.
      select($"pmid", $"cleaned_title_words",  $"cleaned_abs_words").
      map(attrs => {
        val pmid = attrs.getInt(0)
        val tw = attrs.getSeq[String](1)
        val aw = attrs.getSeq[String](2)
        (pmid, tw ++ aw)
      }).toDF("pmid", "terms")

    return termsDf.cache()
  }

  def getTopIdfs(termsDf: DataFrame, tfbd:DataFrame, tms:Terms, topSize:Int): Map[String, Double] = {
    // docFreqs
    val docf = tms.docFreqs(tfbd)

    // idf
    val idfs = tms.topIdf(docf, topSize)
    return idfs
  }

  def getIdfs(termsDf: DataFrame, tfbd:DataFrame, tms:Terms, freqThreshold:Int): Map[String, Double] = {
    // docFreqs
    val docf = tms.docFreqs(tfbd)

    // idf
    val idfs = tms.idf(docf, freqThreshold)
    return idfs
  }

  def getTermIds(idfs: Map[String, Double]): immutable.Map[String, Int] = {
    return idfs.keys.zipWithIndex.toMap
  }
}
