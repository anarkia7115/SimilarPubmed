package topic

import java.io._
import scala.io.Source
import scala.collection._
import scala.collection.mutable.ListBuffer

class DataSaver {
  def savePostingList(
    postingList: mutable.Map[String, ListBuffer[(Int, Int, Boolean)]], 
    postingListFile: String
    ) : Unit = {

      val pw = new PrintWriter(new File(postingListFile)) 
      for ((k, v) <- postingList) {

        val meshId = k
        for ((pmid, count, isTopic) <- v) {
          pw.write("%s\t%s\t%s\t%s\n".format(meshId, pmid, count, isTopic))
        }
      }
      pw.close
  }

  def loadPostingList(postingListFile: String): 
    mutable.Map[String, ListBuffer[(Int, Int, Boolean)]] = {

      val postingList = mutable.Map[String, ListBuffer[(Int, Int, Boolean)]]()

      var currMesh = ""
 
      for (line <- Source.fromFile(postingListFile).getLines()){
        // parse line
        val fields = line.split("\t")
        val meshId = fields(0)
        val pmid = fields(1).toInt
        val count = fields(2).toInt
        val isTopic = fields(3).toBoolean
        val lb = postingList.getOrElseUpdate(meshId, 
          ListBuffer[(Int, Int, Boolean)]())
        lb += ((pmid, count, isTopic))
      }

      return postingList

    }

}
