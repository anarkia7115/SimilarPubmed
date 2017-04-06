package sparkutil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import algorithms.SerializableClass

case class Abstract(pmid:Int, abs:String)
case class MeshHeading(pmid:Int, mesh:String, major:Boolean)
case class MeshFuzzy(mesh:String, fuzzy:String)
case class WideTable(pmid:Int, abs:String, mesh:String, major:Boolean, fuzzy:String)
//case class PostingList(mesh:String, pmid:Int, count:Int, major:Boolean)

class Dataframe {
  /*
  val sConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SimilarPubmed")
  */

  val spark = SparkSession
    .builder
    .appName("SimilarPubmed")
    .config("spark.master", "local[*]")
    .config("spark.executor.memory", "6g")
    .config("spark.driver.memory", "6g")
    .config("spark.driver.extraJavaOptions", "-Xmx6g")
    .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  val absDf = spark.read
    .textFile("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/abs_apr4.txt")
    .map(_.split("\t"))
    .map(attributes => Abstract(attributes(0).toInt, attributes(1).trim))
    .toDF()

  val meshDf = spark.read
    .textFile("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/mesh_apr4.txt")
    .map(_.split("\t"))
    .map(attributes => MeshHeading(attributes(0).toInt, 
      attributes(1), 
      attributes(2).trim match {
        case "Y" => true
        case "N" => false
      }))
    .toDF()

  val fuzzyDf = spark.read
    .textFile("/home/shawn/fast_data/umls/expanded_mesh.txt")
    .map(_.split("\t"))
    .map(attributes => MeshFuzzy(attributes(0), attributes(1).trim))
    .toDF()

  def joinTables(spark:SparkSession, absDf:DataFrame, meshDf:DataFrame, fuzzyDf:DataFrame):DataFrame = {
    absDf.createOrReplaceTempView("abs")
    meshDf.createOrReplaceTempView("mesh")
    fuzzyDf.createOrReplaceTempView("fuzzy")

    val sqlDf = spark.sql(
      """
      select a.pmid, a.abs, m.mesh, m.major, f.fuzzy
      from abs a
      join mesh m
      on a.pmid = m.pmid
      join fuzzy f
      on m.mesh = f.mesh
      """ ).as[WideTable]
      .toDF
    //map( attrs => WideTable( attrs.getInt(0), attrs.getString(1), attrs.getString(2), attrs.getBoolean(3), attrs.getString(4)))

    return sqlDf    /*
+--------+--------------------+------------+-----+--------------------+
|    pmid|                 abs|        mesh|major|               fuzzy|
+--------+--------------------+------------+-----+--------------------+
     * */
  }

  def fuzzyCount(sqlDf:DataFrame):DataFrame = {

    val resultDf = sqlDf.as[WideTable].map(attrs => (
      attrs.mesh, // mesh word
      attrs.pmid, // pmid
      SerializableClass.countSubstring(attrs.abs, attrs.fuzzy),
      //attrs.fuzzy.r.findAllIn(attrs.abs).length, // fuzzy in abs
      attrs.major  // major
    ))
    .toDF("mesh", "pmid", "count", "major")
    .as[PostingList]
    .toDF
    //map( (attrs => PostingList(attrs(0), attrs(1), attrs(2), attrs(3))))
    return resultDf
  }

  def reduceFuzzyCount(fuzzyCountDf:DataFrame):DataFrame = {
    val resultDf = fuzzyCountDf.as[PostingList]
      .map(attrs => (
        (attrs.mesh, attrs.pmid, attrs.major), // key
        attrs.count
      ))
      .rdd
      .reduceByKey(_ + _)
      .map({
        case ((mesh, pmid, major), count) =>
          (mesh, pmid, count, major)
      }).toDF("mesh", "pmid", "count", "major")
      .as[PostingList]
      .toDF

    return resultDf
  }

  def main():DataFrame = {
    val wideTableDf = joinTables(spark, absDf, meshDf, fuzzyDf)
    val fuzzyCountDf = fuzzyCount(wideTableDf)
    val postingListDf = reduceFuzzyCount(fuzzyCountDf)
    return postingListDf
  }

  def loadPostingListDf():DataFrame = {
    val dfPath = "/home/shawn/fast_data/umls/postinglist.parquet"
    return spark.read.load(dfPath)
  }
}
/*
def matchYN(x:String): Boolean = x match {
  case "Y" => true
  case "N" => false
}
*/
