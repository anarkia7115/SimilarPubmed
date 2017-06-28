package algorithms

import scala.math
import scala.collection.SortedSet
import org.apache.spark.mllib.linalg.distributed.IndexedRow

object SerializableClass extends java.io.Serializable {


  def aggSeqOp(
    topSize:Int
    , sortedSetOrdering:Ordering[(Long, Double)]
  )(
    sortedSetList:List[SortedSet[(Long, Double)]]
  , ir:IndexedRow
  ):List[SortedSet[(Long, Double)]] = {
    val rel_pmid = ir.index
    val src_scoreVec = ir.vector
    sortedSetList.zip(src_scoreVec.toArray).map{ 
      case (sortedSet, src_score) => {
      var newSortedSet = SortedSet[(Long, Double)]()(sortedSetOrdering)
      if (sortedSet.size < topSize) {
        // if not full, just add
        newSortedSet = sortedSet + ((rel_pmid, src_score))
      } else {
        val (min_rel_pmid, min_score) = sortedSet.take(1).toList(0)
        if (src_score > min_score) {
          // drop and add
          newSortedSet = sortedSet.drop(1) + ((rel_pmid, src_score))
        } else{
          // not change
          newSortedSet = sortedSet
        }
      }
      newSortedSet
    }}
  }

  def aggCombOp(topSize:Int)(
    ssl1:List[SortedSet[(Long, Double)]]
  , ssl2:List[SortedSet[(Long, Double)]]
  ):List[SortedSet[(Long, Double)]] = {

    ssl1.zip(ssl2).map{ case (ss1, ss2) => {
      val mergedSs = ss1 ++ ss2
      val topSs = mergedSs.takeRight(topSize)
      topSs
    }}
  }

    def countSubstring(str1:String, str2:String):Int={
      def count(pos:Int, c:Int):Int={
         val idx=str1 indexOf(str2, pos)
         if(idx == -1) c else count(idx+str2.size, c+1)
      }
      count(0,0)
    }

    def singleWeight(termCount:Int, absLength:Int, idf:Double): Double = {
      val eta = 1.0
      val km1 = termCount.toDouble - 1.0
      val mu = 0.013
      val lambda = 0.022
      val l = absLength

      val w = math.sqrt(idf) /
              (
                1.0 +
                math.pow( (mu / lambda), km1) *
                math.exp(  -(mu - lambda) * l)
              )

      return w
    }

    /*
    def wordCount(content:String):List[Map[String, Int]] = {
    }
    */

}
