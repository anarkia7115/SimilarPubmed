def addDoc(w:IndexWriter, title:String, isbn:String):Unit = {
  val doc = new Document()
  doc.add(new TextField("title", title, Field.Store.YES));
  doc.add(new StringField("isbn", isbn, Field.Store.YES))
  w.addDocument(doc)
}

import java.io.File
import java.io.StringReader
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.Field.Index
import org.apache.lucene.document.Field.Store
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.util.Version

val idfsRanked = idfsById.withColumn("rank", rank.over(w))
mat.createOrReplaceTempView("mat")

val query = """
select term_id, count(1) as doc_count
from mat
group by term_id
"""

val docTerm = spark.sql(query)
