/**
  * Created by shawn on 2/20/17.
  */
package topic

//import org.slf4j.LoggerFactory
//import grizzled.slf4j.{Logger, Logging}

import java.io._

import com.typesafe.scalalogging.LazyLogging

import scala.collection._
import scala.collection.mutable.ListBuffer

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.SingularValueDecomposition

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


//import org.slf4j.LoggerFactory
//import org.slf4j.impl.SimpleLogger

//import scala.slick.driver.H2Driver.simple._

//import com.typesafe.slick

/**
  * Created by shawn on 2/13/17.
  */

case class HdfsAlreadyExistsException(fp:String) extends Exception(
  fp + " already exists!")

case class TermDocWeight(term_id:Int, pmid:Int, weight:Double)
case class PpScore(src_pmid:Int, rel_pmid:Int, score:Double)

object Main extends LazyLogging {
  val spark = SparkSession
    .builder
    .appName("Matrix Multiply")
    .config("spark.master", "spark://hpc2:7077")
    .getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val absPath =  "/raw/abs_data3.txt"

    val matOutPath =  "/data/bigMatDf3" 
    val idfsOutPath = "/data/idfsDf3"
    val termIdsOutPath = "/data/termIdsDf3"

    //val mat = bigMat(absPath, matOutPath, idfsOutPath, termIdsOutPath)
    val mat = spark.read.load(matOutPath)

    val twByPmid = mat.as[TermDocWeight].map(attrs => 
      (attrs.pmid, Map(attrs.term_id -> attrs.weight))
    ).rdd.reduceByKey(_ ++ _).toDF("pmid", "tw")

    chunkAnalyzeProcess(args, twByPmid)
  }

  def calcSvd(args: Array[String]): Unit = {
    val matPath = "/data/bigMatDf2"
    val mat = spark.read.load(matPath).sample(false, 0.00001)
    mat.cache
    val svdAl = new algorithms.Svd(spark)
    val termDocMat = svdAl.genRowMatrix(mat)
    termDocMat.rows.cache
    val k = 1000
    val svd = termDocMat.computeSVD(k, computeU=true)
  }

  def chunkAnalyzeProcess(args: Array[String], twByPmid:DataFrame): Unit = {
    val sc = spark.sparkContext

    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val leftRange = 0
    val numCandidates = 101
    val numOfPartitions = args(0).toInt
    val srcLimit        = args(1).toInt
    val chunkSize       = args(2).toInt
    val pmidRangeDf = spark.read.textFile(pmidRangePath).
      map(_.toInt).
      toDF("pmid").limit(srcLimit)

    val pmidRange = pmidRangeDf.as[Int].collect
    val pmidChunks = pmidRange.grouped(chunkSize).toList

    twByPmid.cache

    var i = 0
    pmidChunks.foreach(chunk => {
      val ppScoreChunk = analyzeAndSort(
        twByPmid, 
        chunk, 
        numOfPartitions, 
        numCandidates)
      ppScoreChunk.write.save(hdfsRoot + 
        "/data/ppScoreChunks/chunk_%s".format(i.toString))
      i += 1
    })
  }

  def unionAll(chunksPath:String, hdfsOutPath:String): Dataset[PpScore] = {
    val sc = spark.sparkContext
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    val hfArray = FileSystem.get(sc.hadoopConfiguration).
      listStatus(new Path(chunksPath))

    val dsArray = hfArray.filter(_.isDirectory).map(hf => {

      val hfPath = hf.getPath
      spark.read.load(hfPath.toString).as[PpScore]
    })

    val unionedDs = sc.union(dsArray.map(_.rdd)).toDS
    //unionedDs.write.save(hdfsOutPath)
    unionedDs
  }

  def analyzeAndSort(
    twByPmid:DataFrame, 
    chunk: Array[Int], 
    numOfPartitions:Int, 
    numCandidates:Int): Dataset[PpScore] = {

    val sc = spark.sparkContext
    val pmidRangeDf = sc.parallelize(chunk).toDF("pmid")
    val smallTwByPmid = pmidRangeDf.
      join(twByPmid.as("b"), "pmid").
      select("pmid", "tw").
      as[(Int, Map[Int, Double])].collect
    val bSmallTw = sc.broadcast(smallTwByPmid)

    // Calculate score between every pair of pmids
    val ppScores = twByPmid.repartition(numOfPartitions).flatMap( attrs => {
      val pmid = attrs.getInt(0)
      val tw = attrs.getMap[Int, Double](1)

      // Loop in Focused pmid_tw
      val smallPpScores = bSmallTw.value.map{ case (pmid2, tw2) => {
        // Init pmid-pmid score
        var ppScore = 0.0
        tw.foreach{ case (t1, w1) => {
          // check if term_id in range
          val w2 = tw2.getOrElse(t1, 0.0)
          // accumulate ppScore
          ppScore += w1 * w2
        }}
        (pmid2, ppScore)
      }}
      smallPpScores.map{ case (pmid2, ppScore) => {
        (pmid2, pmid, ppScore)
      }}
    }).toDF("src_pmid", "rel_pmid", "score").as[PpScore]

    //val numCandidates = 101

    val groupedPpScore = ppScores.mapPartitions(part => {
      part.toSeq.groupBy(_.src_pmid).map{case (k, v) => {
        v.sortBy(-_.score).take(numCandidates)
      }}.flatten.toIterator
    }).as[PpScore]

    val rk = new algorithms.Rank[PpScore]()
    val ranked = rk.rank(groupedPpScore, "src_pmid", "score").as[PpScore]
    return ranked.filter($"rank" <= numCandidates)
    //ranked.filter($"rank" <= 101).write.save(hdfsRoot + "/data/ppRankedScores")
    //ppScores.write.save(hdfsRoot + "/data/ppRankedScores")

    /*
    val smallTwByPmid = smallMat.map(attrs => 
      (attrs.pmid, Map(attrs.term_id, attrs.weight))
    ).rdd.reduceByKey(_ ++ _).collectAsMap
    */

    /*
    val twByPmid = mat.as[TermDocWeight].map(attrs => 
      (attrs.pmid, Map(attrs.term_id, attrs.weight))
    ).rdd.reduceByKey(_ ++ _)

    twByPmid.toDF("pmid", "tw").write.save(hdfsRoot + "/data/twByPmid")
    */
    
  }

  def multiplyTest(args: Array[String]): Unit = {
    val sc = spark.sparkContext
    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).toDF("pmid").limit(10)

    val mat = spark.read.load("hdfs://hpc2:9000/data/bigMatDf").as[TermDocWeight]
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").select("term_id", "pmid", "weight").as[TermDocWeight]
    mat.cache
    smallMat.cache

    val smallPmidMap = indexSmallMat(smallMat)
    val numSrc = smallPmidMap.size
    val bSmallPmidMap = sc.broadcast(smallPmidMap)

    val result = multiplyResultNoIndex(mat, smallMat, bSmallPmidMap)
    result.rows.map(_.toArray).toDF.write.save(hdfsRoot + "/result/multiply_may22")
    //println(result.numRows)
  }

  /*
  def test2(args: Array[String]): Unit = {
    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).limit(10).toDF("pmid")

    val mat = spark.read.load("hdfs://hpc2:9000/data/bigMatDf").as[TermDocWeight]
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").as[TermDocWeight]
    mat.cache
    smallMat.cache
    val pwByTermIdDf = mat.map (attrs => {
      (attrs.term_id, List((attrs.pmid, attrs.weight)))
    }).rdd.reduceByKey(_ ++ _).toDF("term_id", "weight_by_pmid")
    val pwByTermIdDf = mat.map (attrs => {
      (attrs.term_id, (attrs.pmid, attrs.weight))
    }).aggregateByKey(ListBuffer)
    //pMat = mat.repartition("pmid")
    //pMat.fold(ListBuffer.empty[])
    val pwByTermId = mat.map( attrs => 
      (attrs.term_id, Map(attrs.pmid -> attrs.weight))
    ).rdd.reduceByKey(
      _ ++ _
    )

    val matByTermId = mat.groupByKey(attrs => {
      attrs.term_id
    })
    
    val pwByTermId = matByTermId.flatMapGroups{case (k, tdwIter) => {
      val pwList = tdwIter.map( attrs => {
        (attrs.pmid, attrs.weight)
      })
      Map(k -> pwList.toList)
    }}

    val kvRdd = pwByTermIdDf.as[(Int, Map[Int, Double])].rdd
    kvRdd.cache

    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).limit(10).toDF("pmid")

    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").as[TermDocWeight]
    val scores = smallMat.flatMap(attrs => {
      val termId = attrs.term_id
      val pmid = attrs.pmid
      val weight = attrs.weight

      val pwMap = kvRdd.lookup(termId)
      val scoreList = pwMap.map{case (pmid2, weight2) => 
      (pmid, pmid2, weight * weight2)}
      scoreList.sortBy(-_._3).slice(0, 100)
    })

    val pwByTermIdDf = spark.read.load(hdfsRoot + "/data/pwByTermIdDf")

    val result = smallMat.as("a").join(
      pwByTermIdDf.as("b"), "term_id").
      toDF("term_id", "pmid", "weight", "pmid_weight").cache.
      map(attrs => { 
        val term_id = attrs.getInt(0) 
        val pmid = attrs.getInt(1) 
        val weight = attrs.getDouble(2) 
        val pmid_weight = attrs.getMap[Int, Double](3) 

        val pmid_weight2 = pmid_weight.map{ case (pmid2, weight2) =>
          (pmid2, weight2 * weight)
        }
        (pmid, pmid_weight2)
      }).rdd.reduceByKey{case (m1, m2) => {
        m1 ++ m2.map{case (k, v) =>
          k -> (v + m1.getOrElse(k, 0.0))
        }
      }}.flatMap{ case (src_pmid, rel_pmid_weight) => {
        val top100Scores = rel_pmid_weight.toSeq.sortBy(-_._2).slice(0, 100)
        top100Scores.map{ case (rel_pmid, score) =>
          (src_pmid, rel_pmid, score)
        }
      }}

    val result2 = smallMat.as("a").join(
      mat.as("b"), "term_id").
      select("a.pmid", "b.pmid", "a.weight", "b.weight")
      toDF("term_id", "pmid", "weight", "pmid_weight").cache.
      map(attrs => { 
        val term_id = attrs.getInt(0) 
        val pmid = attrs.getInt(1) 
        val weight = attrs.getDouble(2) 
        val pmid_weight = attrs.getMap[Int, Double](3) 

        val pmid_weight2 = pmid_weight.map{ case (pmid2, weight2) =>
          (pmid2, weight2 * weight)
        }
        (pmid, pmid_weight2)
      }).rdd.reduceByKey{case (m1, m2) => {
        m1 ++ m2.map{case (k, v) =>
          k -> (v + m1.getOrElse(k, 0.0))
        }
      }}.flatMap{ case (src_pmid, rel_pmid_weight) => {
        val top100Scores = rel_pmid_weight.toSeq.sortBy(-_._2).slice(0, 100)
        top100Scores.map{ case (rel_pmid, score) =>
          (src_pmid, rel_pmid, score)
        }
      }}
  }
  */

  /*
  def test1(args: Array[String]): Unit = {
    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).limit(10).toDF("pmid")

    val mat = spark.read.load("hdfs://hpc2:9000/data/bigMatDf").as[TermDocWeight]
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").as[TermDocWeight]
    //.select("term_id", "pmid", "weight")
    val partitionedSmallMat = smallMat.repartition($"pmid")

    val pr3 = multi.coordinateMatrixMultiply3(mat, partitionedSmallMat)
    pr3.write.save(hdfsRoot + "/result/pr3")
    
    val twByPmid = mat.as[TermDocWeight].map(
      attrs => (attrs.pmid, Map(attrs.term_id -> attrs.weight))
    ).rdd.reduceByKey(_ ++ _)

    val pwByTermId = mat.as[TermDocWeight].map(
      attrs => (attrs.term_id, List((attrs.pmid, attrs.weight)))
    ).rdd.reduceByKey(_ ++ _)

    val smallPwByTermId = smallMat.as[TermDocWeight].map(
      attrs => (attrs.term_id, List((attrs.pmid, attrs.weight)))
    ).rdd.reduceByKey(_ ++ _)

    pwByTermIdDf = pwByTermId.toDF("term_id", "weightByPmid")

    pwByTermId.cache
    smallPwByTermId.cache

    val numTerms = mat.select("term_id").distinct.count

    val lCoo = lMat2Coo(mat)
    val lBm = lCoo.toBlockMatrix.cache
    lBm.validate

    val pmidId = smallMat.select("pmid").as[Int].distinct.collect.zipWithIndex.toMap
    val bPmidId = sc.broadcast(pmidId)

    val smallVecs = smallMat.map(attrs => 
      MatrixEntry(attrs.term_id, bPmidId.value(attrs.pmid), attrs.weight)
    )

    val smallVecs = smallMat.map(attrs => 
      MatrixEntry(attrs.term_id, bPmidId.value(attrs.pmid), attrs.weight)
    )
  }
  */

  def rankMultiplyResult(): Unit = {
    val sc = spark.sparkContext
    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).toDF("pmid").limit(10)

    val mat = spark.read.load("hdfs://hpc2:9000/data/bigMatDf").limit(100).as[TermDocWeight]
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").select("term_id", "pmid", "weight").as[TermDocWeight]
    mat.cache
    smallMat.cache

    val smallPmidMap = indexSmallMat(smallMat)
    val numSrc = smallPmidMap.size
    val bSmallPmidMap = sc.broadcast(smallPmidMap)
    val lCoo = lMat2Coo(mat)

    val numTerms = mat.select("term_id").distinct.as[Int].collect.size
    val rCoo = rMat2Coo(smallMat, bSmallPmidMap.value, numTerms, spark)
    val blkMat = rCoo.toBlockMatrix.cache
    blkMat.validate
    val localMat = blkMat.toLocalMatrix()
    val result = lCoo.toIndexedRowMatrix.multiply(localMat)

    // inverse pmid map
    val imap = smallPmidMap.map(_.swap)

    val numCandidates = 101

  }

  def multiplyResultNoIndex(mat:Dataset[TermDocWeight], smallMat:Dataset[TermDocWeight], bSmallPmidMap:Broadcast[Map[Int, Int]]): RowMatrix = {

    val sc = spark.sparkContext
    val lCoo = lMat2Coo(mat)

    val numTerms = mat.select("term_id").distinct.as[Int].collect.max + 1
    val rCoo = rMat2Coo(smallMat, bSmallPmidMap.value, numTerms, spark)
    val blkMat = rCoo.toBlockMatrix.cache
    blkMat.validate
    val localMat = blkMat.toLocalMatrix()
    val result = lCoo.toRowMatrix.multiply(localMat)

    result
  }

  def multiplyResult(mat:Dataset[TermDocWeight], smallMat:Dataset[TermDocWeight], bSmallPmidMap:Broadcast[Map[Int, Int]]): IndexedRowMatrix = {

    val sc = spark.sparkContext
    val lCoo = lMat2Coo(mat)

    val numTerms = mat.select("term_id").distinct.as[Int].collect.max + 1
    val rCoo = rMat2Coo(smallMat, bSmallPmidMap.value, numTerms, spark)
    val blkMat = rCoo.toBlockMatrix.cache
    blkMat.validate
    val localMat = blkMat.toLocalMatrix()
    val result = lCoo.toIndexedRowMatrix.multiply(localMat)

    result
  }

  def sortScores(): Unit = {

    val sc = spark.sparkContext
    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).toDF("pmid").limit(10)

    val mat = spark.read.load("hdfs://hpc2:9000/data/bigMatDf").as[TermDocWeight]
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").select("term_id", "pmid", "weight").as[TermDocWeight]
    mat.cache
    smallMat.cache

    val smallPmidMap = indexSmallMat(smallMat)
    val numSrc = smallPmidMap.size
    val bSmallPmidMap = sc.broadcast(smallPmidMap)

    val result = multiplyResult(mat, smallMat, bSmallPmidMap)
    // inverse pmid map
    val imap = smallPmidMap.map(_.swap)
    val bImap = sc.broadcast(imap)

    val numCandidates = 101

    // rows: index, vector
    //val testList = testResult.map(indexed_row => {
    val resultList1 = result.rows.map(indexed_row => {
      val rel_pmid = indexed_row.index.toInt
      val scores = indexed_row.vector.toArray
      (rel_pmid, scores)
      val (scoreList, minList) = (
        List.fill(numSrc)(mutable.Map.empty[Int, Double]) // list of result
       ,List.fill(numSrc)((0, 0.0)))
      for (i <- 0 to scoreList.size - 1){
        var sm = scoreList(i)
        sm += (rel_pmid -> scores(i))
      }
      (scoreList, minList)
    })
    val resultList = resultList1.reduce({case ((sl1, ml1), (sl2, ml2)) => {
      val resultSl = ListBuffer.empty[mutable.Map[Int, Double]]
      val resultMl = ListBuffer.empty[(Int, Double)]
        for (i <- 0 to (numSrc - 1)) {
          val sm1 = sl1(i)
          val sm2 = sl2(i)
          val mt1 = ml1(i)
          val mt2 = ml2(i)
          val resultSm = mergeScoreMap(sm1, sm2, numCandidates)
          var resultMt = (0, 0.0)
          if (resultSm.size >0){
            resultMt = resultSm.toSeq.sortBy(_._2).apply(0) // minimum
          }
          resultSl.append(resultSm)
          resultMl.append(resultMt)
        }
      (resultSl.toList, resultMl.toList)
    }})

    println("=======")
    println(resultList._1.size)
    println("=======")


    //result.rows.toDF.write.save(hdfsRoot + "/result/limited10_may18")
    /*
    val formattedResult = ListBuffer.empty[(Int, Int, Double)]
    for (i <- 0 to resultList._1.size - 1) {
      val srcPmid = bImap.value(i).toInt
      val curMap  = resultList._1(i)
      curMap.foreach{ case (relPmid, weight) => {
        formattedResult.append((srcPmid, relPmid, weight))
      }}
    }
    */
    //sc.parallelize(formattedResult.toList).toDF.show
//write.save(hdfsRoot + "/result/small_may19")
    spark.close
  }

  def denseMultiply(): Unit = {

    val sc = spark.sparkContext
    val hdfsRoot = "hdfs://hpc2:9000"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).toDF("pmid").limit(1000)

    val mat = spark.read.load("hdfs://hpc2:9000/data/bigMatDf").as[TermDocWeight]
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").select("term_id", "pmid", "weight").as[TermDocWeight]
    mat.cache
    smallMat.cache

    val smallPmidMap = indexSmallMat(smallMat)
    val numSrc = smallPmidMap.size
    val bSmallPmidMap = sc.broadcast(smallPmidMap)
    val lCoo = lMat2Coo(mat)

    val numTerms = mat.select("term_id").distinct.as[Int].collect.size
    val rCoo = rMat2Coo(smallMat, bSmallPmidMap.value, numTerms, spark)
    val blkMat = rCoo.toBlockMatrix.cache
    blkMat.validate
    val localMat = blkMat.toLocalMatrix()
    val result = lCoo.toIndexedRowMatrix.multiply(localMat)

    // inverse pmid map
    val imap = smallPmidMap.map(_.swap)

    val numCandidates = 101

    // rows: index, vector
    //val testList = testResult.map(indexed_row => {
    val resultList = result.rows.map(indexed_row => {
      val rel_pmid = indexed_row.index.toInt
      val scores = indexed_row.vector.toArray
      (rel_pmid, scores)
    }).aggregate(
      (List.fill(numSrc)(mutable.Map.empty[Int, Double]) // list of result
      ,List.fill(numSrc)((0, 0.0))) // list of min value (id, score)
    )(
    {case ((scoreList, minList), (rel_pmid, scores)) => {
      fixLengthScoreCompare(scoreList, minList, rel_pmid, scores, numSrc, numCandidates)
    }},
    {case ((sl1, ml1), (sl2, ml2)) => {
      val resultSl = ListBuffer.empty[mutable.Map[Int, Double]]
      val resultMl = ListBuffer.empty[(Int, Double)]
        for (i <- 0 to (numSrc - 1)) {
          val sm1 = sl1(i)
          val sm2 = sl2(i)
          val mt1 = ml1(i)
          val mt2 = ml2(i)
          val resultSm = mergeScoreMap(sm1, sm2, numCandidates)
          var resultMt = (0, 0.0)
          if (resultSm.size >0){
            resultMt = resultSm.toSeq.sortBy(_._2).apply(0) // minimum
          }
          resultSl.append(resultSm)
          resultMl.append(resultMt)
        }
      (resultSl.toList, resultMl.toList)
    }})

    /*
    for (i <- 0 to testList._1.size - 1) {
      val relPmid = imap(i)
      println(testList._1(i)(relPmid))
    }
    */

    //result.rows.toDF.write.save(hdfsRoot + "/result/limited10_may18")
    val formattedResult = ListBuffer.empty[(Int, Int, Double)]
    for (i <- 0 to resultList._1.size - 1) {
      val srcPmid = imap(i).toInt
      val curMap  = resultList._1(i)
      curMap.foreach{ case (relPmid, weight) => {
        formattedResult.append((srcPmid, relPmid, weight))
      }}
    }
    sc.parallelize(formattedResult.toList).toDF.write.save(hdfsRoot + "/result/small_may19")
  }

  def mergeScoreMap(
    sm1:mutable.Map[Int, Double]
  , sm2:mutable.Map[Int, Double]
  , numCandidates:Int
  ):mutable.Map[Int, Double] = {

    val resultScoreMap = mutable.Map.empty[Int, Double]

    val seq1 = sm1.toSeq.sortBy(-_._2)
    val seq2 = sm2.toSeq.sortBy(-_._2)
    val l1 = seq1.size
    val l2 = seq2.size

    var i1 = 0
    var i2 = 0

    // map not full, and maps has value
    for (dummyIdx <- 0 to numCandidates - 1
         if (i1 < l1 || i2 < l2)) {

      if (i1 >= l1 || i2 >= l2) {
        // if i1 exceed, use seq2
        if (i1 >= l1){
          // use seq2 k, v
          resultScoreMap += seq2(i2)
          i2 = i2 + 1

          // if i2 exceed, use seq1
        } else{
          // use seq1 k, v
          resultScoreMap += seq1(i1)
          i1 = i1 + 1
        }
      } else {
        // if value1 larger
        if (seq1(i1)._2 > seq2(i2)._2) {
          // use seq1 k, v
          resultScoreMap += seq1(i1)
          i1 = i1 + 1

          // value1 no larger
        } else {
          // use seq2 k, v
          resultScoreMap += seq2(i2)
          i2 = i2 + 1
        }
      } 
    }
    return resultScoreMap
  }

  def fixLengthScoreCompare(
    scoreList:List[mutable.Map[Int, Double]]
  , minList:List[(Int, Double)]
  , rel_pmid:Int
  , scores:Array[Double]
  , numSrc:Int
  , numCandidates:Int
  ): (List[mutable.Map[Int, Double]], List[(Int, Double)]) = {
    // init
    val resultMinList = ListBuffer.empty[(Int, Double)]
    // loop in score vector for each src_pmid
    for(i <- 0 to (numSrc - 1)) {
      val curScoreMap = scoreList(i)
      var curMinId = minList(i)._1
      var curMinScore = minList(i)._2
      val curScore = scores(i)

      // if map is full
      if (curScoreMap.size > numCandidates - 1) {

        // if smaller than min, ignore
        // if larger than min, replace in Map, and replace min
        if(curScore > curMinScore) {
          curScoreMap.remove(curMinId)
          curScoreMap += (rel_pmid -> curScore)

          curMinId = rel_pmid
          curMinScore = curScore
        }

        // if smaller than the min, and map is free
      } else if(curScore < curMinScore) {
        // replace min and append
        curMinId = rel_pmid
        curMinScore = curScore

        curScoreMap += (rel_pmid -> curScore)

        // if not the min, and map is free
      } else {
        // just append
        curScoreMap += (rel_pmid -> curScore)
      }
      // update min in list
      resultMinList.append((curMinId, curMinScore))
    }
    (scoreList, resultMinList.toList)
  }

  def indexSmallMat(sm:Dataset[TermDocWeight]): Map[Int, Int] = {
    val pmidArr = sm.select("pmid").as[Int].distinct.collect
    return pmidArr.zipWithIndex.toMap
  }

  def lMat2RowMatrix(mat:Dataset[TermDocWeight]):CoordinateMatrix = {
    val lMatEntries = mat.map(attrs => {
      MatrixEntry(attrs.pmid, attrs.term_id, attrs.weight)
    }).rdd
    val lMat = new CoordinateMatrix(lMatEntries)
    return lMat
  }

  /*(termId, docId, weight)*/
  def lRdd2Vecs(mat:RDD[(Int, Long, Double)]):RDD[MatrixEntry] = {
    val lMatEntries = mat.map(attrs => {
      MatrixEntry(attrs._1, attrs._2, attrs._3)
    })
    return lMatEntries
  }

  def lMat2Vecs(mat:Dataset[TermDocWeight], docIds:Map[Int, Int]):RDD[MatrixEntry] = {
    val lMatEntries = mat.map(attrs => {
      MatrixEntry(docIds(attrs.pmid), attrs.term_id, attrs.weight)
    }).rdd
    return lMatEntries
  }

  def lMat2Coo(mat:Dataset[TermDocWeight]):CoordinateMatrix = {
    val lMatEntries = mat.map(attrs => {
      MatrixEntry(attrs.pmid, attrs.term_id, attrs.weight)
    }).rdd
    val lMat = new CoordinateMatrix(lMatEntries)
    return lMat
  }

  def rMat2Coo(
    mat:Dataset[TermDocWeight], 
    pmidMap:Map[Int, Int], 
    numTerms:Int, 
    spark:SparkSession):CoordinateMatrix = {
    val sc = spark.sparkContext
    val lMatEntries = mat.map(attrs => {
        val termId = attrs.term_id
        val pmid = attrs.pmid
        val weight = attrs.weight
        MatrixEntry(termId, pmidMap(pmid), weight)
      }).rdd
    val lMat = new CoordinateMatrix(lMatEntries, numTerms, pmidMap.size)
    return lMat
  }


  def scoreDf(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Mat Multiply")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val hdfsRoot = "hdfs://hpc2:9000"

    // out path
    val scoreOutPath = hdfsRoot + "/data/scoreDf"

    // check exists
    val hdfs = new utils.HdfsFiles(spark, hdfsRoot)
    for(p <- List(
         scoreOutPath
        )) {
      if(hdfs.exists(p)) {
        throw new HdfsAlreadyExistsException(p)
      }
    }


    // load big mat
    val bigMatPath = hdfsRoot + "/data/bigMatDf"
    val mat = spark.read.load(bigMatPath).as[TermDocWeight]
    mat.cache

    // load small mat
    //val pmidRangePath = hdfsRoot + "/raw/pmid_range_10.txt"
    val pmidRangePath = hdfsRoot + "/raw/pmid_range_may10.txt"
    //val pmidRangeDf = sc.parallelize(pmidRangePath).toDF("pmid")
    val pmidRangeDf = spark.read.textFile(pmidRangePath).map(_.toInt).toDF("pmid")
    val smallMat = pmidRangeDf.join(mat.as("b"), "pmid").select("term_id", "pmid", "weight").as[TermDocWeight]
    smallMat.cache()
    val localMat = smallMat.as[(Int, Int, Double)].collect.toSeq
    val ml = new algorithms.matrix.Multiply(spark)
    val scoreMat = ml.coordinateMatrixMultiply3(mat, smallMat)

    // write data
    scoreMat.write.save(scoreOutPath)
    spark.close
  }

  def bigMatWithTopTerms(absPath:String, matOutPath:String, idfsOutPath:String, termIdsOutPath:String): Unit = {
    val spark = SparkSession
      .builder
      .appName("Bag Of Words")
      .getOrCreate()

    val sc = spark.sparkContext

    //val absPath =  "/raw/abs_data3.txt"
    //val absPath = "/raw/small_abs_data.txt"

    //val matOutPath =  "/data/bigMatDf2" 
    //val matOutPath = "/data/smallMatDf"
    //val idfsOutPath = "/data/idfsDf2"
    //val termIdsOutPath = "/data/termIdsDf2"

    // check exists
    val hdfsRoot = "hdfs://hpc2:9000"
    val hdfs = new utils.HdfsFiles(spark, hdfsRoot)
    for(p <- List(
        matOutPath
        , idfsOutPath
        , termIdsOutPath
        )) {
      if(hdfs.exists(p)) {
        throw new HdfsAlreadyExistsException(p)
      }
    }

    val mat = bagOfWordsProcessWithTopTerms(spark, absPath, idfsOutPath, termIdsOutPath)
    mat.write.save(matOutPath)

    //val df = new sparkutil.Dataframe()
    //df.genPostingList()
    spark.close
  }

  def bigMat(
    absPath:String, 
    matOutPath:String, 
    idfsOutPath:String, 
    termIdsOutPath:String): DataFrame = {

    // check exists
    val hdfsRoot = "hdfs://hpc2:9000"
    val hdfs = new utils.HdfsFiles(spark, hdfsRoot)
    for(p <- List(
        matOutPath
        , idfsOutPath
        , termIdsOutPath
        )) {
      if(hdfs.exists(p)) {
        throw new HdfsAlreadyExistsException(p)
      }
    }

    val mat = bagOfWordsProcess(spark, absPath, idfsOutPath, termIdsOutPath)
    mat.write.save(matOutPath)

    return mat
  }

  def bagOfWordsProcessWithTopTerms(spark:SparkSession, absPath:String, idfsOutPath:String, termIdsOutPath:String): DataFrame = {
    val sc = spark.sparkContext
    val topSize = 50000

    val bow = new algorithms.BagOfWords(spark)
    val textRdd = bow.load(absPath)
    val normedDf = bow.preprocess(textRdd)
    val termsDf = bow.getAndCacheDocFreqs(normedDf)
    val tms = new algorithms.Terms(spark, termsDf)
    val tfByPmid = tms.tfByDoc()
    val idfs = bow.getTopIdfs(termsDf, tfByPmid, tms, topSize)
    val termIds = bow.getTermIds(idfs)

    // write idfs and termId to hdfs
    val dw = new utils.DataWriter(spark)
    dw.write(idfs, idfsOutPath)
    dw.write(termIds, termIdsOutPath)

    // broadcast
    val bIdfs = sc.broadcast(idfs)
    val bTermIds = sc.broadcast(termIds)

    // big mat
    val wt = new algorithms.Weights(spark)
    val mat = wt.scoreMatByDocs(tfByPmid, bIdfs.value, bTermIds.value)
    mat
  }

  def bagOfWordsProcess(spark:SparkSession, absPath:String, idfsOutPath:String, termIdsOutPath:String): DataFrame = {
    val sc = spark.sparkContext
    val freqThreshold = 10

    val bow = new algorithms.BagOfWords(spark)
    val textRdd = bow.load(absPath)
    val normedDf = bow.preprocess(textRdd)
    val termsDf = bow.getAndCacheDocFreqs(normedDf)
    val tms = new algorithms.Terms(spark, termsDf)
    val tfByPmid = tms.tfByDoc()
    val idfs = bow.getIdfs(termsDf, tfByPmid, tms, freqThreshold)
    val termIds = bow.getTermIds(idfs)

    // write idfs and termId to hdfs
    val dw = new utils.DataWriter(spark)
    dw.write(idfs, idfsOutPath)
    dw.write(termIds, termIdsOutPath)

    // broadcast
    val bIdfs = sc.broadcast(idfs)
    val bTermIds = sc.broadcast(termIds)

    // big mat
    val wt = new algorithms.Weights(spark)
    val mat = wt.scoreMatByDocs(tfByPmid, bIdfs.value, bTermIds.value)
    mat
  }

  def downloadAllFromMysql():Unit = {
    val absFile = "./data/abs_all.txt"
    val meshFile = "./data/mesh_all.txt"
    val conn = new Connector()
    conn.runAbs(absFile)
    conn.runTopicAll(meshFile)
  }

  def runRankedSimilarPubmed():Unit = {
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
    val df = new sparkutil.Dataframe()
    val ha = new algorithms.AdvancedAnalysis(df.spark, df.loadPostingListDf)
    val nDoc = ha.docNumber
    //val similarityDf = ha.main(currPmidList, df.absDf, df.meshDf, df.fuzzyDf, nDoc)
    val similarityDf = df.spark.read.load("./data/similarityDf")
    val sumSimilarityDf = ha.sumSimilarity(similarityDf)
    val rankedSimilarResult = ha.rankAndCollect(sumSimilarityDf)


    val al = new Algorithm()
    val conn = new Connector()
    val resultFolder = "./data/result_all_abs"
    //sortDfResult(al, conn, rankedSimilarResult, resultFolder)
    val relatedMeshTable = ha.loadRelatedMeshTable()
    //sortDfResult(al, conn, df.spark, relatedMeshTable, rankedSimilarResult, resultFolder)
  }

  def localWorks(): Unit = {
    // 1. compare words
    //val cw = new CompareWords()
    //cw.main
    //
    // 2. calculate idf
    //
    // 3. calculate pmid length
    //val pl = new PmidLength()
    //pl.main

    // 4. test Connector
    //val conn = new Connector()


    // 5. compare words
    val analyzedOut = "./data/analyzed.txt"
    //val cw = new CompareWords(absFile, analyzedOut)
    //cw.main

    // 6. run topic match
    //runTopic()

    // (7). run topic tree
    //val testPmid = 24118693
    //val topicSet = testTopicTree(testPmid)
    //println(topicSet)

    // 8. posting list
    //testPostingList()
    val topicFile = "/home/shawn/git/SimilarPubmed/data/pmid_topic"
    val al = new Algorithm()
    //val postingList = al.createPostingList(topicFile, analyzedOut)

    // 9. save postingList
    val ds = new DataSaver()
    val postingListFile = "./data/posting_list"

    //ds.savePostingList(postingList, postingListFile)

    // 10. load postingList
    //testPostingList(postingList)


    val usedPmidFile1 = "./data/abs.txt"
    val usedPmidFile2 = "./data/abs_mar13.txt"
    //val usedPmidSet = al.loadPmidSet(List(usedPmidFile1, usedPmidFile2), 0)
    val absFile = "./data/abs_apr4.txt"
    val meshFile = "./data/mesh_apr4.txt"
    val conn = new Connector()
    //conn.runAbs(absFile)
    //runTopic(absFile, meshFile)

    /*
    val inputAbsFile = args(0)
    println(inputAbsFile)
    val testSparkHdfs = "hdfs://localhost:9000/user/shawn/test_spark_result"
    val rstHdfs = "hdfs://localhost:9000/user/shawn/%s".format(args(1))
    //val rstHdfs = "hdfs://localhost:9000/user/shawn/abs_count_result"
    val sConf = new SparkConf()
                  .setMaster("local[*]")
                  .setAppName("SimilarPubmed")

    val testSparkFile = "file:///home/shawn/git/SimilarPubmed/data/test_spark.txt"

    // init spark
    val sc = new SparkContext(sConf)
    */
    //val absSc = sc.textFile(absLocal)
    //val testSparkSc = sc.textFile(testSparkFile)
    //val absSmall = "./data/abs_small.txt"
    //val analyzerSparkSc = sc.textFile(inputAbsFile, 100)
    // init analyzer
    //val analyzer = new ConceptAnalyzer()
    //analyzer.init
    // init read abs file
    /*
    val testSpark = testSparkSc.flatMap(
      line => {
        val k = line.split("\t")(0)
        val v = line.split("\t")(1)
        simulateExtractWordProcess(v).map(vv => (k, vv))
      }
    )
    */

    /*
     var lineNum = 0
     val analyzerSpark = analyzerSparkSc.flatMap(
       line => {
         val fq = new FileQueue("/tmp/portQueue", 8066 to 8071)
         lineNum += 1
         val k = line.split("\t")(0)
         val v = line.split("\t")(1)
         val p = fq.borrowOne
         while (p == 0) {

           println("ports are full, waiting for new")
           Thread sleep 1000
           val p = fq.borrowOne
         }
         val analyzer = new ConceptAnalyzer(p)
         val rst = analyzer.process(v).map(vv => (k, vv))
         fq.returnOne(p)
         println("processed %s".format(lineNum))
         rst
       }
     )
     //testSpark.saveAsTextFile(rstHdfs)
     analyzerSpark.saveAsTextFile(rstHdfs)
     sc.stop()
     */
    //testSpark.saveAsTextFile(testSparkHdfs)
    //println(simulateExtractWordProcess("aab"))

    //val counts = absSc.flatMap(line => line.split("\t"))
    //             .map(word => (word, 1))
    //             .reduceByKey(_ + _)
    //counts.saveAsTextFile(rstHdfs)

    //conn.runExcludeAbs(usedPmidSet, absFile)

    // preparation
    //val countMap = al.calcCountMap2(postingList, true)
    /*
    val postingList = ds.loadPostingList(postingListFile)
    val countMap = al.calcCountMap2(postingList, false)
    val lengthMap = al.calcArticleLength(absFile)
    val nDoc = al.calcDistinctPmid(postingListFile)
    val idfMap = al.calcIdf(postingList, nDoc)

    println("prepared")
    */

    // 11. calculate idf
    //val idfList = al.calcIdf(postingList)
    /*
    for (pmid <- pmidList) {
      val outputFile = "%s_not_accurate.txt".format(pmid)
      findSimilarPmids(pmid, conn, al, countMap, lengthMap, nDoc, idfMap, outputFile)
    }
    */

  }

  def simulateExtractWordProcess(line: String):
  List[(String, String, String, String)] = {
    val rst = ListBuffer[(String, String, String, String)]()
    for (xx <- 1 to 5) {
      val xline = line + xx
      val yl = (
        xline + 1,
        xline + 2,
        xline + 3,
        xline + 4
        )
      rst += yl
    }
    return rst.toList
  }

  def sortResult(al: Algorithm, conn: Connector): Unit = {
    val pmidList = List(27040497, 27783602, 25592537, 24742783, 20038531, 24460801, 22820290, 23716660, 26524530, 26317469)

    // sort result
    import java.nio.file.{Files, Paths}
    for (pmid <- pmidList) {
      //val onePmid = "24742783"
      //val fileWithTopic = "./data/raw_result/%s.txt".format(pmid)
      //val fileNoTopic = "./data/raw_result/%s_not_accurate.txt".format(pmid)
      val fileWithTopicSort = "./data/sorted/%s.txt".format(pmid)
      val fileNoTopicSort = "./data/sorted/%s_no_topic.txt".format(pmid)
      val fileResultWithTopic = "./data/result/%s.txt".format(pmid)
      val fileResultNoTopic = "./data/result/%s_no_topic.txt".format(pmid)
      if (Files.exists(Paths.get(fileWithTopicSort))) {
        al.addAbsAnnotation(
          pmid,
          fileWithTopicSort,
          fileResultWithTopic,
          10,
          conn
        )
      }
      if (Files.exists(Paths.get(fileNoTopicSort))) {
        al.addAbsAnnotation(
          pmid,
          fileNoTopicSort,
          fileResultNoTopic,
          10,
          conn
        )
      }
    }
  }

  def sortDfResult(al: Algorithm,
                   conn: Connector,
                   spark: SparkSession,
                   relatedMeshTable: DataFrame,
                   rankedSimilarResult: Array[(Int, List[(Int, Double)])],
                   resultFolder:String
                  ): Unit = {
    // sort result
    import java.nio.file.{Files, Paths}

    for ((pmid, relatedList) <- rankedSimilarResult) {

      val fileResultWithTopic = "%s/%s.txt".format(resultFolder, pmid)

      al.addAbsAnnotation(
        pmid,
        relatedList,
        spark,
        relatedMeshTable,
        fileResultWithTopic,
        conn
      )
    }
  }

  def sortDfResult(al: Algorithm,
                   conn: Connector,
                   rankedSimilarResult: Array[(Int, List[(Int, Double)])],
                   resultFolder:String
                  ): Unit = {
    // sort result
    import java.nio.file.{Files, Paths}
    for ((pmid, relatedList) <- rankedSimilarResult) {

      val fileResultWithTopic = "%s/%s.txt".format(resultFolder, pmid)

      al.addAbsAnnotation(
        pmid,
        relatedList,
        fileResultWithTopic,
        conn
      )
    }
  }

  def findSimilarPmids(
                        pmid: Int,
                        conn: Connector,
                        al: Algorithm,
                        countMap: Map[String, Map[Int, Int]],
                        lengthMap: Map[Int, Int],
                        nDoc: Int,
                        idfMap: Map[String, Double],
                        outputFile: String): Unit = {

    val absTxt = conn.runOneAbs(pmid)
    if (absTxt == "") {
      println("%s no abstract".format(pmid))
      return
    }

    val smallTopicList = conn.runOneTopic(pmid)

    val ca = new ConceptAnalyzer()
    val smallMeshResult = ca.process(absTxt)

    val smallPostingList = al.createSmallPostingList(
      smallTopicList,
      smallMeshResult
    )

    val m_length = al.getAbsLength(absTxt)
    val targetPostingList = smallPostingList
    //val targetPostingList = al.limitTopic(smallPostingList)

    println("calculate resultMap")
    val resultMap = al.calcSimilarity(
      m_length,
      targetPostingList,
      countMap,
      lengthMap,
      idfMap,
      nDoc
    )
    println(resultMap.size)
    val pw = new PrintWriter(new File(outputFile))
    for ((k, v) <- resultMap) {
      pw.write("%s: %s\n".format(k, v))
    }
    pw.close
  }

  def testPostingList(
                       postingList: mutable.Map[String, ListBuffer[(Int, Int, Boolean)]]):
  Unit = {
    val meshId = "C0039829"
    println(postingList(meshId))
  }

  def testTopicTree(pmid: Int): Boolean = {
    val al = new Algorithm()
    val topicFile = "./data/pmid_topic"
    val topicTree = al.createTopicTree(topicFile)
    val matched = "Risk Factors aa"
    return topicTree(pmid) contains matched
  }

  def runTopic(analyzedOut: String, pmidTopicFile: String): Unit = {
    val conn = new Connector()
    val pmidTable = "focus_pmid"
    /*
    val uniqPmidOut = "./data/uniq_pmid"
    conn.generatePmidFocus(analyzedOut, uniqPmidOut)
    logger.info("pmidFocus generated")

    //val pmidTopicFile = "./data/pmid_topic"
    conn.createPmidFocusTable(pmidTable)
    logger.info("focus_pmid created")
    conn.insertRecords(uniqPmidOut, pmidTable)
    logger.info("insert finished")
    */
    conn.runTopic(pmidTable, pmidTopicFile)
    logger.info("topic finished")
  }
}
