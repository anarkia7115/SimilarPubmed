package sparkutil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

case class Abstract(pmid:Int, abs:String)
case class MeshHeading(pmid:Int, mesh:String, major:Boolean)
case class MeshFuzzy(mesh:String, fuzzy:String)
case class WideTable(pmid:Int, abs:String, mesh:String, major:Boolean, fuzzy:String)
case class PostingList


class DataFrame {
  /*
  val sConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SimilarPubmed")
  */

  val spark = SparkSession
    .builder
    .appName("FuzzyTopic")
    .config("spark.master", "local[*]")
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
      """ )
    return sqlDf.map(
      attrs => WideTable(attrs.getInt(0), attrs.getString(1), attrs.getString(2), attrs.getBoolean(3), attrs.getString(4))).toDF
    /*
+--------+--------------------+------------+-----+--------------------+
|    pmid|                 abs|        mesh|major|               fuzzy|
+--------+--------------------+------------+-----+--------------------+
     * */
  }

  def fuzzyCount(sqlDf:DataFrame):DataFrame = {
    val resultDf = sqlDf.map(attrs => (
      attrs.getAs[String]("mesh"), // mesh word
      attrs.getAs[Int]("pmid"), // pmid
      attrs.getAs[String]("fuzzy").r.findAllIn(attrs.getAs[String]("abs")).length, // fuzzy in abs
      attrs.getAs[Boolean]("major")  // major
    ))toDF

    return resultDf
  }
}
/*
def matchYN(x:String): Boolean = x match {
  case "Y" => true
  case "N" => false
}
*/
