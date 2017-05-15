package algorithms

import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

class Lemmatizer {

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def plainTextToLemmas(text: String, ppl: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    ppl.annotate(doc)

    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences;
	 token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 1) {
	lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  /*
   * Input:  RDD[pmid, title, abs_text]
   * Output: RDD[pmid, Seq[String], Seq[String]]
   * */
  def lemmatize(textRdd: RDD):RDD = {

    val lemmatized = absRdd.mapPartitions{ it =>
      val ppl = createNLPPipeline()
      it.map { case (pmid, title, abs_text) =>
	(   pmid
	  , plainTextToLemmas(title, ppl)
	  , plainTextToLemmas(abs_text, ppl)
	)
      }
    }
    return lemmatized
  }
}
