
val qualifierPath = "hdfs://hpc2:9000/raw/mesh_heading_qualifier.txt"
val qualifierData = spark.read.textFile(qualifierPath)
val header = qualifierData.first
val qualifierData_noHeader = qualifierData.filter(row => row != header)
qualifierData_noHeader.show
qualifierData.show
case class Qualifier(pmid:Int, descriptor_name:String, qualifier_name:String, qualifier_name_major_yn:Boolean)
val qualifierDf = qualifierData_noHeader .map(_.split("\t")) .map(attributes => Qualifier( attributes(0).toInt , attributes(1) , attributes(2) , attributes(3).trim match{ case "Y" => true case "N" => false })).toDF
qualifierDf.show
qualifierDf.write.save("hdfs://hpc2:9000/data/qualifierDf12.parquet")
:history
:history 100

hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf2.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf3.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf4.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf5.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf6.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf7.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf8.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf9.parquet
hdfs dfs -rmdir hdfs://hpc2:9000/data/qualifierDf_repartitioned.parquet

hdfs dfs -rm -r hdfs://hpc2:9000/data/qualifierDf
hdfs dfs -rm -r hdfs://hpc2:9000/data/qualifierDf.parquet
hdfs dfs -rm -r hdfs://hpc2:9000/data/qualifierDf10.parquet
hdfs dfs -rm -r hdfs://hpc2:9000/data/qualifierDf11.parquet
hdfs dfs -rm -r hdfs://hpc2:9000/data/qualifierDf12.parquet
hdfs dfs -rm -r hdfs://hpc2:9000/data/qualifierDf2

val selfJoinQuery = "select a.pmid, b.pmid, a.mesh, a.major, b.major from mesh a join mesh b on a.mesh = b.mesh"
val selfJoinDf = spark.sql(selfJoinQuery)
meshDf.createOrReplaceTempView("mesh")
val selfJoinDf = spark.sql(selfJoinQuery)
selfJoinDf.show
val selfJoinDf = spark.sql(selfJoinQuery).toDF("source_pmid", "related_pmid", "mesh", "source_major", "related_major")
selfJoinDf.show
selfJoinDf.map({case (p1, p2, m, m1, m2) => ((p1, p2, source_major, related_major), m)})
selfJoinDf.map({case (p1, p2, m, m1, m2) => ((p1, p2, m1, m2), 1)})
selfJoinDf.map(attrs => ((attrs.get(0), attrs.get(1), attrs.get(3), attrs.get(4)), 1))
selfJoinDf.map(attrs => ((attrs.getInt(0), attrs.getInt(1), attrs.getBoolean(3), attrs.getBoolean(4)), 1))
val relationDf = selfJoinDf.map(attrs => ((attrs.getInt(0), attrs.getInt(1), attrs.getBoolean(3), attrs.getBoolean(4)), 1)).rdd.reduceByKey(_+_).toDF("source_pmid", "related_pmid", "source_major", "related_major", "mesh_count")
val relationDf = selfJoinDf.map(attrs => ((attrs.getInt(0), attrs.getInt(1), attrs.getBoolean(3), attrs.getBoolean(4)), 1)).rdd.reduceByKey(_+_).map({case ((f1, f2, f3, f4), f5) => (f1, f2, f3, f4, f5)}).toDF("source_pmid", "related_pmid", "source_major", "related_major", "mesh_count")
relationDf.write.save(hdfsRoot + "/data/relationDf")

val hdfsRoot = "hdfs://hpc2:9000"
val meshUrl = hdfsRoot + "/data/meshDf"
val meshDf = spark.read.load(meshUrl)
val qualifierUrl = hdfsRoot + "/data/qualifierDf"
val qualifierDf = spark.read.load(qualifierUrl)
qualifierDf.createOrReplaceTempView("qualifier")
meshDf.createOrReplaceTempView("mesh")
val currPmidList = List(
  27040497,
  27783602,
  25592537,
  24742783,
  20038531,
  24460801,
  22820290,
  23716660,
  26524530,
  26317469
)
val smallMeshDf = meshDf.as("a").join(currPmidList.toDF("pmid").as("b"), $"a.pmid" === $"b.pmid").select("a.pmid", "a.mesh", "a.major").toDF
smallMeshDf.createOrReplaceTempView("smallmesh")
val query = "select a.pmid, a.mesh, b.pmid, a.major, b.major from smallmesh a join mesh b on a.mesh = b.mesh"
val smallMeshRelationDf = spark.sql(query).toDF("source_pmid", "source_mesh", "related_pmid", "source_major", "related_major")
smallMeshRelationDf.write.save(hdfsRoot + "/data/smallMeshRelationDf")

val smallMeshRelationDf = spark.read.load(hdfsRoot + "/data/smallMeshRelationDf")
smallMeshRelationDf.createOrReplaceTempView("small_mesh_relation")
val smallRelationCountDf = spark.sql("select source_pmid, related_pmid, source_major, related_major, count(1) from small_mesh_relation group by source_pmid, related_pmid, source_major, related_major").toDF("pmid", "related_pmid", "major", "related_major", "count")
smallRelationCountDf.write.save(hdfsRoot + "/data/smallRelationCountDf")
val smallRelationCountDf = spark.read.load(hdfsRoot + "/data/smallRelationCountDf")
smallRelationCountDf.orderBy(desc("count")).show


val smallQualifierDf = qualifierDf.as("a").join(currPmidList.toDF("pmid").as("b"), $"a.pmid" === $"b.pmid").select("a.pmid", "a.descriptor_name", "a.qualifier_name", "a.qualifier_name_major_yn").toDF("pmid", "mesh", "qualifier", "major")
smallQualifierDf.createOrReplaceTempView("small_qualifier")
val query = "select a.pmid, a.mesh, a.qualifier, b.pmid, a.major, b.qualifier_name_major_yn from small_qualifier a join qualifier b on a.mesh = b.descriptor_name and a.qualifier = b.qualifier_name"
val smallQualifierRelationDf = spark.sql(query).toDF("source_pmid", "mesh", "qualifier", "related_pmid", "source_major", "related_major")
smallQualifierRelationDf.write.save(hdfsRoot + "/data/smallQualifierRelationDf")

val smallQualifierRelationDf = spark.read.load(hdfsRoot + "/data/smallQualifierRelationDf")
smallQualifierRelationDf.createOrReplaceTempView("small_qualifier_relation")
val smallQualifierRelationCountDf = spark.sql("select source_pmid, related_pmid, source_major, related_major, count(1) from small_qualifier_relation group by source_pmid, related_pmid, source_major, related_major").toDF("pmid", "related_pmid", "major", "related_major", "count")
smallQualifierRelationCountDf.write.save(hdfsRoot + "/data/smallQualifierRelationCountDf")
//val smallTotalRelationCountDf = smallRelationCountDf .as("a") .join( smallQualifierRelationCountDf.as("b") , Seq("pmid", "related_pmid", "major", "related_major") , "left_outer").select($"a.pmid", $"a.related_pmid", $"a.major", $"a.related_major", $"a.count", $"b.count").toDF("pmid", "related_pmid", "major", "related_major", "count1", "count2").na.fill(0).select($"pmid", $"related_pmid", $"major", $"related_major", $"count1" + $"count2").toDF("pmid", "related_pmid", "major", "related_major", "total_count")

// count00, count01, count10, count11, total_count
val smallRCDf = smallRelationCountDf.map(
  attrs =>
    (
      (attrs.getAs[Int]("pmid"), attrs.getAs[Int]("related_pmid")),
      ( if (attrs.getAs[Boolean]("major") == false && attrs.getAs[Boolean]("related_major") == false) attrs.getAs[Long]("count") else 0
      , if (attrs.getAs[Boolean]("major") == false && attrs.getAs[Boolean]("related_major") == true ) attrs.getAs[Long]("count") else 0
      , if (attrs.getAs[Boolean]("major") == true  && attrs.getAs[Boolean]("related_major") == false) attrs.getAs[Long]("count") else 0
      , if (attrs.getAs[Boolean]("major") == true  && attrs.getAs[Boolean]("related_major") == true ) attrs.getAs[Long]("count") else 0
      )
     )
  ).rdd.reduceByKey(
  { case ((a1, b1, c1, d1), (a2, b2, c2, d2)) => 
    (a1 + a2, b1 + b2, c1 + c2, d1 + d2)
  }
  ).map(
  { case ((pmid, related_pmid), (count00, count01, count10, count11)) =>
    (pmid, related_pmid, count00, count01, count10, count11, 
     count00 + count01 + count10 + count11)
  }
  ).toDF("pmid", "related_pmid", "count00", "count01", "count10", "count11", "naive_total_count")

smallRCDf.write.save(hdfsRoot + "/data/smallRCDf")
val smallRCDf = spark.read.load(hdfsRoot + "/data/smallRCDf")

val smallQRCDf = smallQualifierRelationCountDf.map(
  attrs =>
    (
      (attrs.getAs[Int]("pmid"), attrs.getAs[Int]("related_pmid")),
      ( if (attrs.getAs[Boolean]("major") == false && attrs.getAs[Boolean]("related_major") == false) attrs.getAs[Long]("count") else 0
      , if (attrs.getAs[Boolean]("major") == false && attrs.getAs[Boolean]("related_major") == true ) attrs.getAs[Long]("count") else 0
      , if (attrs.getAs[Boolean]("major") == true  && attrs.getAs[Boolean]("related_major") == false) attrs.getAs[Long]("count") else 0
      , if (attrs.getAs[Boolean]("major") == true  && attrs.getAs[Boolean]("related_major") == true ) attrs.getAs[Long]("count") else 0
      )
     )
  ).rdd.reduceByKey(
  { case ((a1, b1, c1, d1), (a2, b2, c2, d2)) => 
    (a1 + a2, b1 + b2, c1 + c2, d1 + d2)
  }
  ).map(
  { case ((pmid, related_pmid), (count00, count01, count10, count11)) =>
    (pmid, related_pmid, count00, count01, count10, count11, 
     count00 + count01 + count10 + count11)
  }
  ).toDF("pmid", "related_pmid", "count00", "count01", "count10", "count11", "naive_total_count")

smallQRCDf.write.save(hdfsRoot + "/data/smallQRCDf")
val smallQRCDf = spark.read.load(hdfsRoot + "/data/smallQRCDf")

val meshCountDf = meshDf.map(
  attrs =>
  (
    attrs.getAs[Int]("pmid"), 
    ( if (attrs.getAs[Boolean]("major") == false) 1 else 0
    , if (attrs.getAs[Boolean]("major") == true ) 1 else 0
    )
  )).rdd.reduceByKey(
  { case ((a1, b1), (a2, b2)) =>
    (a1 + a2, b1 + b2)
  }).map(
  { case (pmid, (count0, count1)) =>
    (pmid, count0, count1, count0 + count1)
  }).toDF("pmid", "count0", "count1", "naive_total_count")

meshCountDf.write.save(hdfsRoot + "/data/meshCountDf")
val meshCountDf = spark.read.load(hdfsRoot + "/data/meshCountDf")

val qualifierCountDf = qualifierDf.map(
  attrs =>
  (
    attrs.getAs[Int]("pmid"), 
    ( if (attrs.getAs[Boolean]("qualifier_name_major_yn") == false ) 1 else 0
    , if (attrs.getAs[Boolean]("qualifier_name_major_yn") == true ) 1 else 0
    )
  )).rdd.reduceByKey(
  { case ((a1, b1), (a2, b2)) =>
    (a1 + a2, b1 + b2)
  }).map(
  { case (pmid, (count0, count1)) =>
    (pmid, count0, count1, count0 + count1)
  }).toDF("pmid", "count0", "count1", "naive_total_count")

qualifierCountDf.write.save(hdfsRoot + "/data/qualifierCountDf")
val qualifierCountDf = spark.read.load(hdfsRoot + "/data/qualifierCountDf")

smallRCDf.createOrReplaceTempView("src")
meshCountDf.createOrReplaceTempView("meshc")
val scoreQuery = """
select
    a.pmid, a.related_pmid
  , IF((b.count0 + c.count0 - a.count00) = 0, 0, a.count00 / (b.count0 + c.count0 - a.count00)) as score00
  , IF((b.count0 + c.count1 - a.count01) = 0, 0, a.count01 / (b.count0 + c.count1 - a.count01)) as score01
  , IF((b.count1 + c.count0 - a.count10) = 0, 0, a.count10 / (b.count1 + c.count0 - a.count10)) as score10
  , IF((b.count1 + c.count1 - a.count11) = 0, 0, a.count11 / (b.count1 + c.count1 - a.count11)) as score11
  , IF((b.naive_total_count + c.naive_total_count - a.naive_total_count) = 0, 0, 
        a.naive_total_count / (b.naive_total_count + c.naive_total_count - a.naive_total_count)
    ) as naive_score
from src a
  join meshc b
  on a.pmid = b.pmid
  join meshc c
  on a.related_pmid = c.pmid
"""
val smallRSDf = spark.sql(scoreQuery)

smallRSDf.write.save(hdfsRoot + "/data/smallRSDf")
val smallRSDf = spark.read.load(hdfsRoot + "/data/smallRSDf")

smallQRCDf.createOrReplaceTempView("sqrc")
qualifierCountDf.createOrReplaceTempView("qualifierc")
val scoreQuery = """
select
    a.pmid, a.related_pmid
  , IF((b.count0 + c.count0 - a.count00) = 0, 0, a.count00 / (b.count0 + c.count0 - a.count00)) as score00
  , IF((b.count0 + c.count1 - a.count01) = 0, 0, a.count01 / (b.count0 + c.count1 - a.count01)) as score01
  , IF((b.count1 + c.count0 - a.count10) = 0, 0, a.count10 / (b.count1 + c.count0 - a.count10)) as score10
  , IF((b.count1 + c.count1 - a.count11) = 0, 0, a.count11 / (b.count1 + c.count1 - a.count11)) as score11
  , IF((b.naive_total_count + c.naive_total_count - a.naive_total_count) = 0, 0, 
        a.naive_total_count / (b.naive_total_count + c.naive_total_count - a.naive_total_count)
    ) as naive_score
from sqrc a
  join qualifierc b
  on a.pmid = b.pmid
  join qualifierc c
  on a.related_pmid = c.pmid
"""
val smallQRSDf = spark.sql(scoreQuery)

smallQRSDf.write.save(hdfsRoot + "/data/smallQRSDf")
val smallQRSDf = spark.read.load(hdfsRoot + "/data/smallQRSDf")

def findScore(pmid1:Int, pmid2:Int):Unit = {
smallRSDf.filter($"pmid" === pmid1 and $"related_pmid" === pmid2).show
}
def sortScore(pmid1:Int):Unit = {
smallRSDf.filter($"pmid" === pmid1).orderBy(desc("naive_score")).show
}
def findWords(pmid1:Int, pmid2:Int):Unit = {
smallMeshRelationDf.filter($"source_pmid" === pmid1 and $"related_pmid" === pmid2).show
}


def findScore2(pmid1:Int, pmid2:Int):Unit = {
smallQRSDf.filter($"pmid" === pmid1 and $"related_pmid" === pmid2).show
}
def sortScore2(pmid1:Int):Unit = {
smallQRSDf.filter($"pmid" === pmid1).orderBy(desc("naive_score")).show
}
def findWords2(pmid1:Int, pmid2:Int):Unit = {
smallQualifierRelationDf.filter($"source_pmid" === pmid1 and $"related_pmid" === pmid2).show
}

val sumCountQuery = """
select IF(a.pmid is null, b.pmid, a.pmid) as pmid
  , IF(a.related_pmid is null, b.related_pmid, a.related_pmid) as related_pmid
  , IF(a.count00 is null, 0, a.count00) + IF(b.count00 is null, 0, b.count00) as count00
  , IF(a.count01 is null, 0, a.count01) + IF(b.count01 is null, 0, b.count01) as count01
  , IF(a.count10 is null, 0, a.count10) + IF(b.count10 is null, 0, b.count10) as count10
  , IF(a.count11 is null, 0, a.count11) + IF(b.count11 is null, 0, b.count11) as count11
  , IF(a.naive_total_count is null, 0, a.naive_total_count) + IF(b.naive_total_count is null, 0, b.naive_total_count) as naive_total_count
from src a
full outer join sqrc b
  on a.pmid = b.pmid
  and a.related_pmid = b.related_pmid
"""
val smallSRCDf = spark.sql(sumCountQuery)
smallSRCDf.write.save(hdfsRoot + "/data/smallSRCDf")
val smallSRCDf = spark.read.load(hdfsRoot + "/data/smallSRCDf")

// values in meshc and qualifierc both can be null
val mpqCountQuery = """
select IF(a.pmid is null, b.pmid, a.pmid) as pmid
  , IF(a.count0 is null, 0, a.count0) + IF(b.count0 is null, 0, b.count0) as count0
  , IF(a.count1 is null, 0, a.count1) + IF(b.count1 is null, 0, b.count1) as count1
  , IF(a.naive_total_count is null, 0, a.naive_total_count) + IF(b.naive_total_count is null, 0, b.naive_total_count) as naive_total_count
from meshc a
full outer join qualifierc b
  on a.pmid = b.pmid
"""
val mpqCountDf = spark.sql(mpqCountQuery)
mpqCountDf.write.save(hdfsRoot + "/data/mpqCountDf")
val mpqCountDf = spark.read.load(hdfsRoot + "/data/mpqCountDf")

smallSRCDf.createOrReplaceTempView("join_table")
mpqCountDf.createOrReplaceTempView("count_table")
val scoreQuery = """
select
    a.pmid, a.related_pmid
  , IF((b.count0 + c.count0 - a.count00) = 0, 0, a.count00 / (b.count0 + c.count0 - a.count00)) as score00
  , IF((b.count0 + c.count1 - a.count01) = 0, 0, a.count01 / (b.count0 + c.count1 - a.count01)) as score01
  , IF((b.count1 + c.count0 - a.count10) = 0, 0, a.count10 / (b.count1 + c.count0 - a.count10)) as score10
  , IF((b.count1 + c.count1 - a.count11) = 0, 0, a.count11 / (b.count1 + c.count1 - a.count11)) as score11
  , IF((b.naive_total_count + c.naive_total_count - a.naive_total_count) = 0, 0, 
        a.naive_total_count / (b.naive_total_count + c.naive_total_count - a.naive_total_count)
    ) as naive_score
from join_table a
  join count_table b
  on a.pmid = b.pmid
  join count_table c
  on a.related_pmid = c.pmid
"""
val smallSRSDf = spark.sql(scoreQuery)
smallSRSDf.write.save(hdfsRoot + "/data/smallSRSDf")
val smallSRSDf = spark.read.load(hdfsRoot + "/data/smallSRSDf")

val testSql1 = """
select count(1)
from src a
full outer join sqrc b
  on a.pmid = b.pmid
  and a.related_pmid = b.related_pmid
"""
val testSql2 = """
select count(1)
from src a
"""
val testSql3 = """
select count(1)
from join_table a
  join count_table b
  on a.pmid = b.pmid
  join count_table c
  on a.related_pmid = c.pmid
"""

import org.apache.spark.sql.expressions.Window
val w = Window.partitionBy($"pmid").orderBy(desc("naive_score"))
val ranked = smallSRSDf.withColumn("rank", rank.over(w)).where($"rank" <= 1000)
ranked.write.save(hdfsRoot + "/data/rankedSSRSDf")
val ranked = spark.read.load(hdfsRoot + "/data/rankedSSRSDf")

val rankFind = (p1:Int, p2:Int) => ranked.filter($"pmid" === p1 and $"related_pmid" === p2).show

def wordFind(p1:Int, p2:Int):Unit = {
smallMeshRelationDf.filter($"source_pmid" === p1 and $"related_pmid" === p2).show
smallQualifierRelationDf.filter($"source_pmid" === p1 and $"related_pmid" === p2).show
}

spark.read.csv(localAbsData).map(attrs => (attrs.getInt(0), attrs.getString(1), xtr.trim(attrs.getString(2)) )).show


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

val xtr = new XmlTagRemover("AbstractText")

val localAbsData = "/tmp/abs_data3.txt"
val hdfsRoot = "hdfs://hpc2:9000"

val absRawData = sc.textFile(localAbsData)

val absRdd = absRawData.flatMap { line =>
  val fields = line.trim.split('\t')
  if (fields.size != 3) {
    None
  } else {
    val (pmid, title, absText) = (fields(0).toInt, fields(1), xtr.trim(fields(2)))
    Some((pmid, title, absText))
  }
}

// lemmatization
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

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

val pipeline = createNLPPipeline()

val doc = new Annotation(text)

val props = new Properties()
props.put("annotators", "tokenize, ssplit, pos, lemma")
val pipeline = new StanfordCoreNLP(props)

pipeline.annotate(doc)

val lemmas = new ArrayBuffer[String]()
val sentences = doc.get(classOf[SentencesAnnotation])
for (sentence <- sentences;
     token <- sentence.get(classOf[TokensAnnotation])) {
  val lemma = token.get(classOf[LemmaAnnotation])
  if (lemma.length > 1 && !stopWords.contains(lemma)) {

    println("%s -> %s".format(token, lemma))
    //lemmas += lemma.toLowerCase
  }
}

val lemmatized = absRdd.mapPartitions{ it => 
  val ppl = createNLPPipeline()
  it.map { case (pmid, title, abs_text) =>
    (   pmid
      , plainTextToLemmas(title, ppl)
      , plainTextToLemmas(abs_text, ppl)
    )
  }
}

//val wordsDf = lemmatized.toDF("pmid", "title_words", "abs_words")

val remover_title = new StopWordsRemover().setInputCol("title_words").setOutputCol("filtered_title_words")
val remover_abs   = new StopWordsRemover().setInputCol("abs_words").setOutputCol("filtered_abs_words")

//val filteredWordsDf = remover_title.transform(remover_abs.transform(wordsDf))

//val wordsDf = spark.read.load(hdfsRoot + "/data/filteredWordsDf")
val sampleWordsDf = wordsDf.sample(false, 0.2)
val wordsDf = spark.read.load(hdfsRoot + "/data/sampleWordsDf")


val absTF = wordsDf.select("abs_words", "").as[Seq[String]].rdd.flatMap( terms => {
    val termFreqs = terms.foldLeft(new HashMap[String, Int]())  {
      (map, term) => {
        map += term -> (map.getOrElse(term, 0) + 1)
        map
      }
    }
    termFreqs
  }
).reduceByKey(_+_)

val titleTF = wordsDf.select("title_words").as[Seq[String]].rdd.flatMap( terms => {
    val termFreqs = terms.foldLeft(new HashMap[String, Int]())  {
      (map, term) => {
        map += term -> (map.getOrElse(term, 0) + 1)
        map
      }
    }
    termFreqs.toMap
  }
).reduceByKey(_+_)

val absWordsDf = filtered.select("pmif", "filtered_abs_words").as[(Int, Seq[String])].flatMap{
  case (pmid, absWords) =>
   absWords.map((pmid, _))
}.toDF("pmid", "word")

val absWordsDf = spark.read.load(hdfsRoot + "/data/absWordsDf")
val titleWordsDf = spark.read.load(hdfsRoot + "/data/titleWordsDf")

val titleWordsDf = filtered.select("pmif", "filtered_title_words").as[(Int, Seq[String])].flatMap{
  case (pmid, titleWords) =>
   titleWords.map((pmid, _))
}.toDF("pmid", "word")

val docTermFreqs = wordsDf.as[(Int, String)].map{
  case (pmid, word) =>
    (pmid, Map(word -> 1))
}.rdd.reduceByKey{
  case (a, b) =>
  a ++ b.map{ case (k,v) => k -> (v + a.getOrElse(k,0)) }
}

a ++ b.map{ case (k,v) => k -> (v + a.getOrElse(k,0)) }

val hdfsRoot = "hdfs://hpc2:9000"

val filtered = spark.read.load(hdfsRoot + "/data/filteredWordsDf")

import scala.collection.mutable.HashMap
val absTF = filtered.select("pmif", "filtered_title_words", "filtered_abs_words").as[(Int, Seq[String], Seq[String])].rdd.map{ case (pmid, terms1, terms2) => {
    val terms = terms1 ++ terms2
    val termFreqs = terms.foldLeft(new HashMap[String, Int]())  {
      (map, term) => {
        map += term -> (map.getOrElse(term, 0) + 1)
        map
      }
    }
    (pmid, termFreqs.toMap)
  }
}

/*
val titleTF = filtered.select("pmif", "filtered_title_words").as[(Int, Seq[String])].rdd.map{ case (pmid, terms) => {
    val termFreqs = terms.foldLeft(new HashMap[String, Int]())  {
      (map, term) => {
        map += term -> (map.getOrElse(term, 0) + 1)
        map
      }
    }
    (pmid, termFreqs.toMap)
  }
}
*/

val absFreqs = absTF.flatMap{case (pmid, wordMap) =>
  wordMap.keySet
}.map((_, 1)).reduceByKey(_+_)

//val numTerms = 50000

//val ordering = Ordering.by[(String, Int), Int](_._2)

//val topAbsFreqs = absFreqs.top(numTerms)(ordering)

val numDocs = filtered.count

val afs = absFreqs.filter(t => t._2 >10)

//val absIdfs = absFreqs.map {
val absIdfs = afs.map {
  case (term, count) => (term, math.log(numDocs.toDouble / count))
}.collectAsMap
//}

val absTermIds = absIdfs.keys.zipWithIndex.toMap

val bAbsTermIds = sc.broadcast(absTermIds)
val bAbsIdfs = sc.broadcast(absIdfs)


import org.apache.spark.mllib.linalg.Vectors
//import scala.collection.JavaConversions._
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Matrices

import scala.collection._

def scoreVectors(termFreqs:Map[String, Int], idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[(Int, Double)] = {
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

val mat = absTF.flatMap{case (pmid, termFreqs) => {

  // List((termId, weight))
  val scores = scoreVectors(termFreqs, bAbsIdfs.value, bAbsTermIds.value)
  scores.map(attrs => (pmid, attrs._1, attrs._2))
  // (pmid, termId, weight)
}}


/*
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix,
  MatrixEntry, BlockMatrix}

def oneSVMatrix(termFreqsArray:Array[immutable.Map[String, Int]], idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Array[org.apache.spark.mllib.linalg.distributed.BlockMatrix] = {
  val eta = 1.0
  val mu = 0.013
  val lambda = 0.022
  val termSize = termIds.size

  val blockMatArray = termFreqsArray.grouped(1000).map(

    tfChunk => {
      var colNum = 0
      val termScores = tfChunk.flatMap(
        termFreqs => 
        {
          val absTotalTerms = termFreqs.values.sum
          val l = absTotalTerms
          val meSeq = termFreqs.filter{ 
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
            val me = MatrixEntry(termIds(term), colNum, w)
            me
          }}
          colNum += 1
          meSeq
      }).toSeq
      val coordMat = new CoordinateMatrix(sc.parallelize(termScores), termSize, termFreqsArray.size)
      val blockMat = coordMat.toBlockMatrix
      blockMat
    }).toArray
  blockMatArray
}

def scoreMatrixEntry(termFreqs:Map[String, Int], rowIdx:Int, idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[MatrixEntry] = {
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
    MatrixEntry(termIds(term), rowIdx, w)
  }}.toSeq

  termScores
}
*/

def scoreCooSeq(termFreqs:Map[String, Int], rowIdx:Int, idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[(Int, Int, Double)] = {
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


val sampleTF = absTF.sample(false, 10000.0/absTF.count).collect()
//val samplePmids = sampleTF.map(_._1).zipWithIndex.toMap

val cooDf = sampleTF.flatMap ( attrs =>{

  val pmid = attrs.getInt(0)
  val tf =   attrs.getMap[String, Int](1)
  scoreCooSeq(tf, pmid, bAbsIdfs.value, bAbsTermIds.value)

  }).toDF
// (termId, pmid, value)

val sparseMat = SparseMatrix.fromCOO(bAbsTermIds.value.size, sampleTF.size, cooIter)
val coordMat = new CoordinateMatrix(mes, bAbsTermIds.value.size, sampleTF.count)
val blockMat = coordMat.toBlockMatrix
blockMat.validate
val localMat = blockMat.toLocalMatrix

val mat = new IndexedRowMatrix(vecs)

val result = mat.multiply(localMat)

def oneSVMatrix(termFreqs:Map[String, Int], idfs:Map[String, Double], termIds:immutable.Map[String, Int]):org.apache.spark.mllib.linalg.distributed.BlockMatrix = {
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
    MatrixEntry(termIds(term), 0, w)
  }}.toSeq
  val coordMat = new CoordinateMatrix(sc.parallelize(termScores), termSize, 1)
  val blockMat = coordMat.toBlockMatrix.cache
  return blockMat
}

//val vecs = absTF.map{case (pmid, termFreqs) => {
//
//  IndexedRow(pmid, scoreVectors(termFreqs, bAbsIdfs.value, bAbsTermIds.value))
//
//}}

//val mat = new IndexedRowMatrix(vecs)
val result = mat.multiply(localMat).rows.map(row => (row.index, row.vector(0)))
result.toDF("pmid", "score").orderBy(desc("score")).show

val resultArray = sampleSVBMatArray.flatMap(
  bm => {
    val localMat = bm.toLocalMatrix
    val resultRows = mat.multiply(localMat).rows
    val rowIndexes = resultRows.map(r => r.index).collect
    val rowValues =  resultRows.map(r => r.vector.toArray).collect.transpose
    val topPmids = trResultRows.rows.map(
      scores => {
        val topScoreIndices = scores.toArray.zipWithIndex.sortBy(-_._1).map(_._2).take(20)
        val filteredPmid = topScoreIndices.map(rowIndexes(_))
        filteredPmid
      }
    )

    // final
    //resultRows.unpersist()
    topPmids
  }
)

import topic.Algorithm
val al = new Algorithm()
val folderPath = "data/result_may05"
localResult.map{case (pmid, relpmids) => 
  al.addAbsAnnotation(
    pmid
    , relpmids.map(a => (a.toInt, 0.0)).toList.slice(1, 6)
    , folderPath + "/" + pmid.toString + ".txt"
    , conn
  )
}

def coordinateMatrixMultiply(leftMatrix: org.apache.spark.rdd.RDD[(Int, Int, Double)], rightMatrix: Seq[(Int, Int, Double)]): org.apache.spark.rdd.RDD[((Int, Int), Double)] = {
  val L_ = leftMatrix.map{  case (i, j, v) => (j, (i, v)) }
  val R_ = sc.broadcast(
    rightMatrix.map{ case (j, k, w) => (j, (k, w)) }.toMap
  )

  val productEntries = L_
    .flatMap{ case (j, (i, v)) => {
      if (R_.value.contains(j)) {
        Some((i, v), R_.value(j))
      }
      else {
        None
      }
    }}
    .map{case ((i, v), (k, w)) => ((i, k), (v * w))}
    .reduceByKey(_ + _)

  productEntries
}

def coordinateMatrixMultiply(
    leftMatrix:  org.apache.spark.rdd.RDD[(Int, Int, Double)]
  , rightMatrix: org.apache.spark.rdd.RDD[(Int, Int, Double)]): org.apache.spark.rdd.RDD[((Int, Int), Double)] = {

  val L_ = leftMatrix.map{  case (i, j, v) => (j, (i, v)) }
  val R_ = rightMatrix.map{ case (j, k, w) => (j, (k, w)) }

  val productEntries = L_
    .join(R_)
    .map{case (_, ((i, v), (k, w))) => ((i, k), (v * w))}
    .reduceByKey(_ + _)

  productEntries
}

def coordinateMatrixMultiply(
    leftMatrix:  org.apache.spark.sql.DataFrame
  , rightMatrix: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {

  val L_ = leftMatrix.toDF("pmid", "term_id", "weight")
  // (pmid, termId, weight)
  val R_ = rightMatrix.toDF("term_id", "pmid", "weight")
  // (termId, pmid, value)

  val productEntries = L_.as("l")
    .join(R_.as("r"), "term_id")
    .select("l.pmid", "l.weight", "r.pmid", "r.weight")
    .map(attrs => (
      (attrs.getInt(0), attrs.getInt(2)), // key
      (attrs.getDouble(1) * attrs.getDouble(3))
      ))
    .rdd.reduceByKey(_ + _)
    .map{case ((p1, p2), v) => (p1, p2, v)}.toDF("pmid1", "pmid2", "weight")

  productEntries
}

val hdfsRoot = "hdfs://hpc2:9000"
val mat =   spark.read.load(hdfsRoot + "/data/matDf")
val cooDf = spark.read.load(hdfsRoot + "/data/cooDf") 
mat.cache
cooDf.cache
val result = coordinateMatrixMultiply(mat, cooDf)

val neighborFile = "/home/gcbi/data/SimilarPubmed/pubmed_neighbor_all.txt"
val neighborFile = "/home/gcbi/data/SimilarPubmed/pubmed_neighbor_10.txt"

val nbDf = sc.textFile("file://" + neighborFile).map(
  _.split("\t")).map(attrs => 
  (attrs(0).toInt, attrs(1).toInt, attrs(2).toInt, attrs(3).trim)).toDF(
  "src_pmid", "rel_pmid", "score", "link_name")

import org.apache.spark.sql.expressions.Window
val w = Window.partitionBy($"src_pmid").orderBy(desc("score"))
val w = Window.partitionBy($"pmid2").orderBy(desc("weight"))
val rankedNbDf = nbDf.filter($"link_name" === "pubmed_pubmed").withColumn("rank", rank.over(w))

val resultDf = spark.read.load(hdfsRoot + "/result/may09")

val spDf = resultDf.map(rows => (
    rows.getStruct(0).getInt(1)
  , rows.getStruct(0).getInt(0)
  , rows.getDouble(1)
)).toDF("src_pmid", "rel_pmid", "score")
val rankedSpDf = spDf.withColumn("rank", rank.over(w))

rankedNbDf.filter($"rank" <= 5).show
rankedSpDf.filter($"rank" <= 5).show
rankedNbDf.write.save(hdfsRoot + "/data/rankedNbDf")
rankedSpDf.write.save(hdfsRoot + "/data/rankedSpDf")

val nbRank = 100
val spRank = 10

nb.unpersist()
sp.unpersist()

val hdfsRoot = "hdfs://hpc2:9000"
val rankedNbDf = spark.read.load(hdfsRoot + "/data/rankedNbDf")
val ppRankedScores = spark.read.load(hdfsRoot + "/result/ppRankedScores").
  filter($"src_pmid" =!= $"rel_pmid")
import org.apache.spark.sql.expressions.Window
val w = Window.partitionBy($"src_pmid").orderBy(desc("score"))
val ranked = ppRankedScores.withColumn("rank", rank.over(w))

val nb = rankedNbDf.filter($"rank" <= nbRank)
nb.cache
val sp = ranked.filter($"rank" <= spRank)
sp.cache

nb.createOrReplaceTempView("nb")
sp.createOrReplaceTempView("sp")

val query = """
select sp.src_pmid, sum(if(nb.rel_pmid is null, 0, 1)) as comm_num
from nb right outer join sp
on nb.src_pmid = sp.src_pmid
  and nb.rel_pmid = sp.rel_pmid
group by 1
order by 2 desc
"""

val comparedResult = spark.sql(query)
comparedResult.createOrReplaceTempView("cp")

val query = """
select comm_num, count(1) as c
from cp
group by 1
order by 2 desc
"""

val commDist = spark.sql(query)

/***************************************************************************************************/
import algorithms.BagOfWords
val bow = new BagOfWords()
val textRdd = bow.load("file:///home/gcbi/data/raw/small_abs_data.txt", sc)
val normedDf = bow.preprocess(textRdd, spark)

/***************************************************************************************************/
val absPath = "/home/gcbi/data/raw/small_abs_data.txt"
import topic.Main
val mat = Main.bagOfWordsProcess(sc, spark, absPath)

def searchTopWords(pmid:Int, mat:DataFrame, termIds:scala.collection.Map[Int, String]): DataFrame = {
  mat.filter($"pmid" === pmid).map(attrs => {
    val term_id = attrs.getInt(0)
    val weight = attrs.getDouble(2)
    (termIds(term_id), weight)
  }).toDF("term", "weight")
}

def showTopWords(pmid:Int, mat:DataFrame, termIds:scala.collection.Map[Int, String]): Unit = {
  val rst = mat.filter($"pmid" === pmid).map(attrs => {
    val term_id = attrs.getInt(0)
    val weight = attrs.getDouble(2)
    (termIds(term_id), weight)
  }).toDF("term", "weight")

  rst.orderBy(desc("weight")).show
}

def sct(pmid1:Int, pmid2:Int, mat:DataFrame, termIds:scala.collection.Map[Int, String]): Unit = {

  val tw1 = mat.filter($"pmid" === pmid1).map(attrs => {
    val term_id = attrs.getInt(0)
    val weight = attrs.getDouble(2)
    (termIds(term_id), weight)
  }).toDF("term", "weight")

  val tw2 = mat.filter($"pmid" === pmid2).map(attrs => {
    val term_id = attrs.getInt(0)
    val weight = attrs.getDouble(2)
    (termIds(term_id), weight)
  }).toDF("term", "weight")

  val rst = tw1.as("a").join(tw2.as("b"), "term").select($"term", $"a.weight" * $"b.weight").toDF("term", "weight")

  rst.orderBy(desc("weight")).show
}

/***************************************************************************************************/
a.map {case (j, k, w) => (j, (k, w))}.foldLeft(Map.empty[Int, Seq[(Int, Double)]]){ case (m, (j, kw)) => 
  m + (j -> (
    m.getOrElse(j, Seq.empty[(Int, Double)]) :+ kw
  ))}

    val termIdsDf = sc.parallelize(termIdsList).toDF("term_id")
    val lMatEntries = termIdsDf.as("a") .join(smallMat.as("b"), $"a.term_id" === $"b.term_id", "left_outer") .select("a.term_id", "pmid", "weight") .toDF("term_id", "pmid", "weight") .na.fill(0.0, Seq("weight", pmid)) .map(attrs => {
        val termId = attrs.getInt(0)
        val pmid = attrs.getInt(1)
        val weight = attrs.getDouble(2)
        if (pmid != null) {
          MatrixEntry(termId, smallPmidMap(pmid), weight)
        }
      }).rdd
    val lMat = new CoordinateMatrix(lMatEntries)
    return lMat

spark.conf.set("redis.host", "192.168.2.10")
spark.conf.set("redis.port", "6379")
spark.conf.set("redis.db", "db6")

val kvDf = pwByTermIdDf.map(attrs => {
  val termId = attrs.getInt(0)
  val smallKv = attrs.getMap(1)
  val pmid = smallKv
})

val absUnionInRange = spark.sql("""
  select 
  src_pmid as pmid, sa.title, sa.abstract
  from nbir join abs sa on src_pmid = sa.pmid
  union
  select
  rel_pmid as pmid, ra.title, ra.abstract
  from nbir join abs ra on rel_pmid = ra.pmid
  union
  select 
  src_pmid as pmid, sa.title, sa.abstract
  from spir join abs sa on src_pmid = sa.pmid
  union
  select
  rel_pmid as pmid, ra.title, ra.abstract
  from spir join abs ra on rel_pmid = ra.pmid
  """
  ).cache

val spShowRaw = spark.sql("""
  select
    src_pmid, sa.title as src_title, sa.abstract as src_abstract
  , rel_pmid, ra.title as rel_title, ra.abstract as rel_abstract, spir.rank
  from spir join absir sa on src_pmid = sa.pmid
            join absir ra on rel_pmid = ra.pmid
  
  """
  )

val nbShowRaw = spark.sql("""
  select
    src_pmid, sa.title as src_title, sa.abstract as src_abstract
  , rel_pmid
  , if(ra.title is not null, ra.title, "-No Title-") as rel_title
  , if(ra.abstract is not null, ra.abstract, "-No Abstract-") as rel_abstract, nbir.rank
  from nbir            join absir sa on src_pmid = sa.pmid
            left outer join absir ra on rel_pmid = ra.pmid
  
  """
  )

val spGroupedShowRaw = spShowRaw.map(attrs => {
  val src_pmid = attrs.getInt(0)
  val src_title = attrs.getString(1)
  val src_abstract = attrs.getString(2)
  val rel_pmid = attrs.getInt(3)
  val rel_title = attrs.getString(4)
  val rel_abstract = attrs.getString(5)
  val rank = attrs.getInt(6)
  ((src_pmid, src_title, src_abstract), List((rel_pmid, rel_title, rel_abstract, rank)))
}).rdd.reduceByKey(_ ++ _)

val spShowLines = spGroupedShowRaw.map{ case ((srcPmid, srcTitle, srcAbs), relList) => {
  val sortedRelList = relList.sortBy(_._4)
  val relatedStringList = sortedRelList.map{ case (relPmid, relTitle, relAbs, rank) => {
    "%s\nTitle: %s\nrank:%s\nAbstract: \n%s\n\n".format(relPmid, relTitle, rank, relAbs)
  }}.mkString
  val outLine = "Source Article: \n\n%s\nTitle: %s\nAbstract: \n%s\n\nRelated Articles: \n\n%s".format(srcPmid, srcTitle, srcAbs, relatedStringList)
  (srcPmid, outLine)
}}
spShowLines.toDF.write.save("/data/spShowLinesDf2")

val nbGroupedShowRaw = nbShowRaw.map(attrs => {
  val src_pmid = attrs.getInt(0)
  val src_title = attrs.getString(1)
  val src_abstract = attrs.getString(2)
  val rel_pmid = attrs.getInt(3)
  val rel_title = attrs.getString(4)
  val rel_abstract = attrs.getString(5)
  val rank = attrs.getInt(6)
  ((src_pmid, src_title, src_abstract), List((rel_pmid, rel_title, rel_abstract, rank)))
}).rdd.reduceByKey(_ ++ _)

val nbShowLines = nbGroupedShowRaw.map{ case ((srcPmid, srcTitle, srcAbs), relList) => {
  val sortedRelList = relList.sortBy(_._4)
  val relatedStringList = sortedRelList.map{ case (relPmid, relTitle, relAbs, rank) => {
  """%s
    |Title: %s
    |rank:%s
    |Abstract: 
    |%s
    |
    |""".format(relPmid, relTitle, rank, relAbs)
  }}.mkString
  val outLine = 
  """Source Article: 
    |
    |%s
    |Title: %s
    |Abstract: 
    |%s
    |
    |Related Articles: 
    |
    |%s""".format(srcPmid, srcTitle, srcAbs, relatedStringList)
  (srcPmid, outLine)
}}
nbShowLines.toDF.write.save("/data/nbShowLinesDf2")

val spLocal = spShowLines.collectAsMap
val nbLocal = nbShowLines.collectAsMap

spLocal.foreach{case (pmid, lines) => {
  val pw = new PrintWriter(
    new File("/home/gcbi/data/SimilarPubmed/spShowResult/%s.txt".format(pmid)))
  pw.write(lines)
  pw.close
}}

nbLocal.foreach{case (pmid, lines) => {
  val pw = new PrintWriter(
    new File("/home/gcbi/data/SimilarPubmed/nbShowResult/%s.txt".format(pmid)))
  pw.write(lines)
  pw.close
}}

val detailsWithTermDf = details.map(line=>{
  val fields = line.split("\t")
  val src_pmid = fields(0).toInt
  val rel_pmid = fields(1).toInt
  val term = bTids.value(fields(2).toInt)
  val src_weight = fields(3).toDouble
  val rel_weight = fields(4).toDouble
  val weight = fields(5).toDouble
  ((src_pmid, rel_pmid), Map(term -> weight))
  }).reduceByKey(_ ++ _).map{ 
    case ((src_pmid, rel_pmid), term_weight) => {
    (src_pmid, rel_pmid, term_weight)
  }}.toDF(
  "src_pmid", "rel_pmid", "term_weight")

detailsWithTermDf.cache.createOrReplaceTempView("details")

val spShowRaw = spark.sql("""
  select
    spir.src_pmid, sa.title as src_title, sa.abstract as src_abstract
  , spir.rel_pmid, ra.title as rel_title, ra.abstract as rel_abstract, spir.rank
  , d.term_weight
  from spir join absir sa  on spir.src_pmid = sa.pmid
            join absir ra  on spir.rel_pmid = ra.pmid
            join details d on spir.src_pmid = d.src_pmid 
                          and spir.rel_pmid = d.rel_pmid
  
  """
  )

val spGroupedShowRaw = spShowRaw.map(attrs => {
  val src_pmid = attrs.getInt(0)
  val src_title = attrs.getString(1)
  val src_abstract = attrs.getString(2)
  val rel_pmid = attrs.getInt(3)
  val rel_title = attrs.getString(4)
  val rel_abstract = attrs.getString(5)
  val rank = attrs.getInt(6)
  val term_weight = attrs.getStruct(7)
  (   (src_pmid, src_title, src_abstract)
    , List((rel_pmid, rel_title, rel_abstract, rank, term_weight)))
}).rdd.reduceByKey(_ ++ _)

val spShowLines = spGroupedShowRaw.map{ case ((srcPmid, srcTitle, srcAbs), relList) => {
  val sortedRelList = relList.sortBy(_._4)
  val relatedStringList = sortedRelList.map{ 
    case (relPmid, relTitle, relAbs, rank, term_weight) => {
      val sorted_term_weight = term_weight.sortBy(-_._2)
      val str_related_terms = sorted_term_weight.map{
        case (term, weight) => "%s\t\t%s\n".format(term, weight)
      }
      """%s
        |Title: %s
        |rank:%s
        |Abstract: 
        |%s
        |
        |related terms:
        |%s
        |
        |""".format(relPmid, relTitle, rank, relAbs, str_related_terms)
  }}.mkString
  val outLine = 
  """Source Article: 
    |
    |%s
    |Title: %s
    |Abstract: 
    |%s
    |
    |Related Articles: 
    |
    |%s""".format(srcPmid, srcTitle, srcAbs, relatedStringList)
  (srcPmid, outLine)
}}
//spShowLines.toDF.write.save("/data/spShowLinesDf3")

val spLocal = spShowLines.collectAsMap

spLocal.foreach{case (pmid, lines) => {
  val pw = new PrintWriter(
    new File("/home/gcbi/data/SimilarPubmed/spShowResult2/%s.txt".format(pmid)))
  pw.write(lines)
  pw.close
}}

val filtered = spark.read.load("/data/filteredWordsDf")
val absWordsDf = filtered.select("pmif", "filtered_abs_words").toDF("pmid", "abs_words")

val nb = spark.read.load("/data/rankedNbDf")
nb.createOrReplaceTempView("nb")
val pmidRangeDf = spark.sql(
  """
  select distinct rel_pmid as pmid 
  from nb
  union
  select distinct src_pmid as pmid
  from nb
  """
)
val pmidRange = pmidRangeDf.as[Int].collect
val pmidSet = pmidRange.toSet
val bPmidSet = sc.broadcast(pmidSet)

val absLinesDf = absWordsDf.flatMap( attrs => {
  val pmid = attrs.getInt(0)
  val abs_words = attrs.getSeq[String](1)
  if(bPmidSet.value.contains(pmid)) {
    Some("%s\t%s".format(pmid, abs_words.mkString(" ")))
  }
  else {
    None
  }
})

docTermDf.map(attrs => {
  "%s\t%s\t%s".format(attrs.getInt(0), attrs.getLong(1), attrs.getInt(2))
})

spark.sql("select doc_count, count(1) as c from dt group by 1 having c <= 5000 order by 1 asc").agg(sum("c")).show

spark.sql("select doc_count, count(1) as c from dt group by 1 order by 1 asc")

spark.sql("select count(1) as c from dt group by doc_count").show

// doc count limit, so that value in one key won't be too big
// largest unlimited: 12421884
// now limited to:        1000
val tidRangeDf = docTermDf.filter($"doc_count" <= 1000).select("term_id")

val tidRangeDf = spark.read.load("/data/tidRangeDf")
val tidRange = tidRangeDf.as[Int].collect().toSet()
val tidSet = tidRange.toSet
val bTidSet = sc.broadcast(tidSet)

val matLimitedTids = mat.flatMap( attrs => {
  val term_id = attrs.getInt(0)
  val pmid = attrs.getInt(1)
  val weight = attrs.getDouble(2)
  if (bTidSet.value.contains(term_id)) {
    Some((term_id, pmid, weight))
  } else {
    None
  }
})

//val redisHost = "192.168.2.10"
//val r = new RedisClient(redisHost, 6379, 6)

val mat = spark.read.load("/data/matLimitedTidsDf")

mat.foreachPartition( attrs_iter => {
  val redisHost = "192.168.2.10"
  val rr = new RedisClient(redisHost, 6379, 6)
  println(attrs_iter.size)

  attrs_iter.foreach(attrs => {
    val term_id = attrs.getInt(0)
    val pmid = attrs.getInt(1)
    val weight = attrs.getDouble(2)
    rr.zadd("term_id:%s".format(term_id), weight, pmid)
  })
})

val oldMat = spark.read.load("/data/bigMatDf3")
val docFreqs = spark.read.load("/data/docTermDf")
val tmapDf = spark.read.load("/data/termIdsDf3")
val numTerms = 50000

val ordering = Ordering.by[(Int, Long, Int), Long](_._2)
val topDocFreqs = docFreqs.as[(Int, Long, Int)].rdd.top(numTerms)(ordering)
val numDocs = oldMat.select("pmid").distinct.count
val tmap = tmapDf.select("value", "key").as[(Int, String)].rdd.collectAsMap
val bTmap = sc.broadcast(tmap).value

val idfsDirty = topDocFreqs.map{
  case (termId, count, rank) => {
    val term = bTmap(termId)
    (term, math.log(numDocs.toDouble / count))
  }
}

val idfs = idfsDirty.filterNot{ case (term, idf) => {
  term.matches("-.*-")
}}.toMap

val termIds = idfs.keys.zipWithIndex.toMap

val bTermIds = sc.broadcast(termIds).value
val bIdfs = sc.broadcast(idfs).value

val vecs = docTermFreqs.map(termFreqs => {
  val docTotalTerms = termFreqs.values().sum
  val termScores = termFreqs.filter {
    case (term, freq) => bTermIds.containsKey(term)
  }.map{
    case (term, freq) => (bTermIds(term), 
      //TODO: change weight algorithm
      bIdfs(term) * termFreqs(term) / docTotalTerms)
  }.toSeq
  Vectors.sparse(bTermIds.size, termScores)
})

val newMat = oldMat.flatMap(attrs => {
  val term_id = attrs.getInt(0)
  val pmid = attrs.getInt(1)
  val weight = attrs.getDouble(2)
  val term = bTmap(term_id)
  if(bTermIds.contains(term)) {
    val tid = bTermIds(term)
    Some((tid, pmid, weight))
  } else {
    None
  }
})

val mat = spark.read.load("/data/svd/matDf").toDF("term_id", "pmid", "weight")

val termIds = spark.read.load("/data/svd/termIdsDf")
val bTermIds = sc.broadcast(termIds).value
//sc.parallelize(termIds.toSeq).toDF("term", "id").write.save("/data/svd/termIdsDf")
//sc.parallelize(idfs.toSeq).toDF("term", "idf").write.save("/data/svd/idfsDf")
val pmids = mat.select("pmid").as[Int].distinct.collect
val pmidMap = pmids.zipWithIndex.toMap
//val pmid_idsDf = sc.parallelize(pmid_ids).toDF("pmid", "id")
//pmid_idsDf.write.save("/data/svd/pmidIdsDf")
//val bDocIds = sc.broadcast(pmid_ids).value
val bPmidMap = sc.broadcast(pmidMap).value
val mat_xy = mat.repartition(1000000).map(attrs => {
  val term_id = attrs.getInt(0)
  val pmid = attrs.getInt(1)
  val weight = attrs.getDouble(2)
  (term_id, pmidMap(pmid), weight)
}).toDF("term_id", "pmid_id", "weight")

sc.parallelize(pmids.zipWithIndex).toDF("pmid", "id").write.save("/data/svd/pmidMapDf")
mat_xy.write.save("/data/svd/matXyDf")

val mat = spark.read.load("/data/svd/matDf").toDF("term_id", "pmid", "weight")
val termIds = spark.read.load("/data/svd/termIdsDf")

val docTws = mat.map(attrs => {
  val term_id = attrs.getInt(0)
  val pmid = attrs.getInt(1)
  val weight = attrs.getDouble(2)
  (pmid, Seq((term_id, weight)))
}).rdd.reduceByKey(_ ++ _).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)
//}).rdd.reduceByKey(_ ++ _).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

val termSize = termIds.count.toInt


val docIds = docTws.
  map(_._1).
  zipWithUniqueId().
  collect().toMap

// write doc ids to file
val docIdsFile = "file:///home/gcbi/data/docIds.txt"
import java.io._
val pw = new PrintWriter(new File(docIdsFile))
docIds.foreach{ case (id, pmid) => {
  pw.write("%s\t%s\n".format(id, pmid))
}}
pw.close

//import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Vectors
val vecs = docTws.map {
  case (pmid, twList) => {
    Vectors.sparse(termSize, twList)
  }
}

vecs.cache
docTws.unpersist
//vecs.toDF("index", "vec").write.save("/data/svd/vecs")
//val vecs = spark.read.load("/data/svd/vecs")
val vecs = sc.objectFile[org.apache.spark.mllib.linalg.Vector]("/data/svd/vecs.bin").cache()
import org.apache.spark.mllib.linalg.distributed.RowMatrix
val cmat = new RowMatrix(vecs)

val k = 1000
val svd = cmat.computeSVD(k, computeU=true)

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrices

def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
  val sArr = diag.toArray
  new RowMatrix(mat.rows.map { vec =>
    val vecArr = vec.toArray
    val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
    Vectors.dense(newArr)
  })
}

def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
  new RowMatrix(mat.rows.map { vec =>
    val array = vec.toArray
    val length = math.sqrt(array.map(x => x * x).sum)
    Vectors.dense(array.map(_ / length))
  })
}

val normalizedUS = sc.objectFile[org.apache.spark.mllib.linalg.Vector]("/data/svd/normalizedUS.bin").cache
val vecs = sc.objectFile[org.apache.spark.mllib.linalg.Vector]("/data/svd/vecs.bin").cache()
val usDict = normalizedUS.zipWithUniqueId.map(_.swap)

//def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
def tdfd(docId: Long): Seq[(Double, Long)] = {
  // Look up the row in US corresponding to the given doc ID.
  //val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
  val docRowArr = usDict.lookup(docId).head.toArray
  val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

  // Compute scores against every doc
  val normMat = new RowMatrix(normalizedUS)
  val docScores = normMat.multiply(docRowVec)

  // Find the docs with the highest scores
  val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

  // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
  allDocWeights.filter(!_._1.isNaN).top(10)
}

val nb = spark.read.load("/data/rankedNbDf")
val pmidRangeDf = nb.select("src_pmid").distinct.withColumnRenamed("src_pmid", "pmid").as[Int].cache()
val pmidRangeSet = pmidRangeDf.collect.toSet
:history

import scala.io._
val docIdsFile = "/home/gcbi/data/docIds.txt"
val idPmids = Source.fromFile(docIdsFile).getLines().flatMap(line =>{
  val attrs = line.trim().split("\t")
  val pmid = attrs(0).toInt
  val id = attrs(1).toInt
  if(pmidRangeSet.contains(pmid)) {
    Some((id, pmid))
  } else {
    None
  }
}).toMap

val oneDocId = idPmids.keys.toList(1)
idPmids(oneDocId)
val topDocs = tdfd(oneDocId)

topDocs.foreach{case (score, docId) => {
  val pmid = idPmids(docId.toInt)
  println("%s\t%s".format(pmid, score))
}}

val normalizedUS = sc.objectFile[org.apache.spark.mllib.linalg.Vector]("/data/svd/normalizedUS.bin").cache
val multiIndex = normalizedUS.zipWithUniqueId.zipWithIndex
val multiIndex2 = multiIndex.zipWithUniqueId
val ids = multiIndex.map{case ((vec, uid), oid) => {
  (uid, oid)
}}
ids.filter{case (u1, u2) => {
  u1 != u2
}}.count
[((org.apache.spark.mllib.linalg.Vector, Long), Long)]

val ids = iRdd.map{case ((vec, uid), oid) => {
  (uid, oid)
}}

val rddWith2Uid = iRdd.zipWithUniqueId.map{ case (((vec, uid1), idx1), uid2) => {
  (uid1, uid2)
}}

val uMat = sc.objectFile[org.apache.spark.mllib.linalg.Vector]("file:///home/gcbi/data/svd/uMat.bin")

val uMatWithPmid = uMat.zipWithUniqueId.map{case (vec, uid) => {
  val pmid = idPmids(uid.toInt)
  (vec, pmid)
}}

val fileList = Range(0, 172).map(i => {
  val fileName = "part-%5d".format(i).replaceAll(" ", "0")
  "file:///home/gcbi/data/svd/uMat.bin/%s".format(fileName)
})

val rdds = fileList.map(fileName => {
  sc.objectFile[org.apache.spark.mllib.linalg.Vector](fileName, 1).collect
})
