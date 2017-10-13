package algorithms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
//import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

class Neighbors(inputFilePath:String, sc:SparkContext, spark:SparkSession) {
  import spark.implicits._
  val nbDf = sc.textFile(inputFilePath).map(
    _.split("\t")).map(attrs =>
    (attrs(0).toInt, attrs(1).toInt, attrs(2).toInt, attrs(3).trim)).toDF(
    "src_pmid", "rel_pmid", "score", "link_name")

  def rank():DataFrame = {
    val rk = new algorithms.Rank[Row]()
    val rankedNbDf = rk.rank(
      nbDf.filter($"link_name" === "pubmed_pubmed"), "src_pmid", "score")

    return rankedNbDf
  }

  def compare(rankedSp:DataFrame, rankedNb:DataFrame, spRank:Int, nbRank:Int):DataFrame = {
    val sp = rankedSp.filter($"rank" <= spRank)
    val nb = rankedNb.filter($"rank" <= nbRank)
    sp.cache
    nb.cache
    sp.createOrReplaceTempView("sp")
    nb.createOrReplaceTempView("nb")

    val query = """
    select sp.src_pmid, count(1) as comm_num
    from nb join sp
    on nb.src_pmid = sp.src_pmid
      and nb.rel_pmid = sp.rel_pmid
    group by 1
    order by 2 desc
    """

    val compared = spark.sql(query)

    return compared
  }
}
