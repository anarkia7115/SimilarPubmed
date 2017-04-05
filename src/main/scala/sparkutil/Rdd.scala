package sparkutil

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.Row

//case class Abstract(pmid:Int, abs:String)
//case class MeshHeading(pmid:Int, mesh:String, major:Boolean)
//case class MeshFuzzy(mesh:String, fuzzy:String)
case class PostingList(mesh:String, pmid:Int, count:Int, major:Boolean)

class Rdd {
  val spark = SparkSession
    .builder
    .appName("FuzzyTopic")
    .config("spark.yarn.executor.memoryOverhead", "4096")
    .config("spark.master", "local[*]")
    .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  val absFile = "/home/shawn/git/PubmedWordGame/SimilarPubmed/data/abs_apr4.txt"
  val meshFile = "/home/shawn/git/PubmedWordGame/SimilarPubmed/data/mesh_apr4.txt"
  val fuzzyFile = "/home/shawn/fast_data/umls/expanded_mesh.txt"

  val absRdd = sc
    .textFile("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/abs_apr4.txt")
    .map(_.split("\t"))
    .map(attributes => Abstract(attributes(0).toInt, attributes(1).trim))

  val meshRdd = sc
    .textFile("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/mesh_apr4.txt")
    .map(_.split("\t"))
    .map(attributes => MeshHeading(attributes(0).toInt, 
      attributes(1), 
      attributes(2).trim match {
        case "Y" => true
        case "N" => false
      }))

  val fuzzyRdd = sc
    .textFile("/home/shawn/fast_data/umls/expanded_mesh.txt")
    .map(_.split("\t"))
    .map(attributes => MeshFuzzy(attributes(0), attributes(1).trim))

  def reduceFirst(absRdd:RDD[Abstract], meshRdd:RDD[MeshHeading], fuzzyRdd:RDD[MeshFuzzy]):RDD[(String, Int, Int, Boolean)] = {

    // join mesh fuzzy
    val meshByMesh  = meshRdd.map( mh => (mh.mesh, (mh.pmid, mh.major)))
    val fuzzyByMesh = fuzzyRdd.map(mf => (mf.mesh, Set(mf.fuzzy)))
      .reduceByKey(_ ++ _)

      //  0      10     11       12           10     0     11      12
      // (mesh, (pmid, major, fuzzySet)) -> (pmid, (mesh, major, fuzzySet))
    val fuzzyByPmid = meshByMesh.join(fuzzyByMesh)
      .map({case (mesh, ((pmid, major), fuzzySet)) => (pmid, (mesh, major, fuzzySet))})
    val absByPmid = absRdd.map(abs => (abs.pmid, abs.abs))

    // join abs, fuzzy
    // try to count word frequency in reduce
    // abs, (mesh, major, fuzzySet)

    //(pmid, (abs, mesh, major, fuzzySet)) -> (mesh, pmid, fuzzySet.map, major)
    val result = absByPmid.join(fuzzyByPmid)
      .map({case (pmid, (abs, (mesh, major, fuzzySet))) => (mesh, pmid, fuzzySet.map(fw => {fw.r.findAllIn(abs).length}).sum, major)})

    return result
    //val resultPath = "/home/shawn/fast_data/umls/posting_list.txt"
    //val result.saveAsTextFile(resultPath)
  }

  /*
  def reduceLater(absRdd:RDD[Abstract], meshRdd:RDD[MeshHeading], fuzzyRdd:RDD[MeshFuzzy]):RDD[(String, Int, Int, Boolean)] = {
  }
  */
}
