package sparkutil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import algorithms.SerializableClass

case class Abstract(pmid:Int, abs:String)
case class MeshHeading(pmid:Int, mesh:String, major:Boolean)
case class MeshFuzzy(mesh:String, fuzzy:String)
case class WideTable(pmid:Int, abs:String, mesh:String, major:Boolean, fuzzy:String)
case class Qualifier(pmid:Int, descriptor_name:String, qualifier_name:String, qualifier_name_major_yn:Boolean)
//case class PostingList(mesh:String, pmid:Int, count:Int, major:Boolean)

class Dataframe {
  /*
  val sConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SimilarPubmed")
  */

  //val sparkUrl = "spark://hpc2:7077"
  val sparkUrl = "local[*]"

  val hdfsUrl = "hdfs://hpc2:9000"

  val spark = SparkSession
    .builder
    .appName("SimilarPubmed")
    .config("spark.master",sparkUrl)
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .config("spark.submit.deployMode","client")
    .config("spark.executor.memory", "10g")
    .config("spark.executor.cores", "40")
    .config("spark.driver.memory", "5g")
    .config("spark.driver.core", "10")
    .getOrCreate()

//    .config("spark.jars", "hdfs://hpc2:9000/jars/mysql-connector-java-5.1.38.jar")
//    .config("spark.executor.memory", "100g")
//    .config("spark.executor.cores", "40")
//    .config("spark.driver.memory", "100g")
//    .config("spark.driver.extraJavaOptions", "-Xmx100g")
//    .config("spark.driver.extraClassPath", 
//      "hdfs://hpc2:9000/sbt-classes/")
//    .config("spark.local.dir", "/home/shawn/data/spark-tmp")
  import spark.implicits._

  val sc = spark.sparkContext

  /*
  val clearedDf = absDf2.alias("a")
    .join(filterDf
    .alias("b"), $"a.id" === $"b.id", "leftouter")
    .filter($"b.id".isNull)
    .select($"a.value")
    .map(attrs => attrs.getString(0))
    .map(_.split("\t"))
    .map(attrs=>(attrs(0).toInt, attrs(1)))
    .toDF("pmid", "abs")
  */

  /*
  val absDf = spark.read
    .textFile("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/abs_all.txt")
    .map(_.split("\t"))
    .map(attributes => Abstract(attributes(0).toInt, attributes(1).trim))
    .toDF()
  */
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

  def loadAbsDf(): DataFrame = {
    val absDf = spark.read.load(hdfsUrl + "/data/absDf")
    return absDf
  }

  def loadAllDfs(): (DataFrame, DataFrame, DataFrame) = {

    val absDf = spark.read.load(hdfsUrl + "/data/absDf")

    val meshDf = spark.read.load(hdfsUrl + "/data/meshDf")

    val fuzzyDf = spark.read.load(hdfsUrl + "/data/fuzzyDf")

    (absDf, meshDf, fuzzyDf)

  }

  def loadQualifierDf(): DataFrame = {
    val qualifierPath = hdfsUrl + "/raw/mesh_heading_qualifier.txt"
    val qualifierData = spark.read.textFile(qualifierPath)
    val header = qualifierData.first
    val qualifierData_noHeader = qualifierData.filter(row => row != header)
    val qualifierDf = qualifierData_noHeader
      .map(_.split("\t"))
      .map(attributes => Qualifier(
        attributes(0).toInt
        , attributes(1)
        , attributes(2)
        , attributes(3).trim match{
          case "Y" => true
          case "N" => false
        }
        )).toDF

    return qualifierDf
  }

  def genPostingList():DataFrame = {
    val (absDf, meshDf, fuzzyDf) = loadAllDfs()
    val wideTableDf = joinTables(spark, absDf, meshDf, fuzzyDf)
    val fuzzyCountDf = fuzzyCount(wideTableDf)
    val postingListDf = reduceFuzzyCount(fuzzyCountDf)
    //val postingListLocalPath = "/home/shawn/fast_data/umls/postinglist.parquet"
    //postingListDf.write.save(postingListLocalPath)
    return postingListDf
  }

  def loadPostingListDf():DataFrame = {
    val dfPath = "hdfs://hpc2:9000/data/postingListDf"
    return spark.read.load(dfPath)
  }
}
/*
def matchYN(x:String): Boolean = x match {
  case "Y" => true
  case "N" => false
}
*/
