package topic

import java.sql.{Connection, DriverManager}
import java.sql.ResultSet

class ConnectDownload(outputFile:String) {
  val url = "jdbc:mysql://192.168.2.10:3306/pubmed"
  val driver = "com.mysql.jdbc.Driver"
  val username = "devuser"
  val password = "111111"
  var connection: Connection = _
  val limitNum = 50000
  //val outputFile = "/home/shawn/git/SimilarPubmed/data/abs.txt"

  def this(): Unit = {
  }

  def getResultSet(query:String): ResultSet = {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement(
                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY)

      //val statement = connection.createStatement()
      statement.setFetchSize(Integer.MIN_VALUE)

      val rs = statement.executeQuery(query)
      return rs
  }

  def runMesh():Unit = {
    try {
      val query = """
      """
      val rs = getResultSet(query)
    }
  }

  def runAbs():Unit = {
    try {

      val query = """
      SELECT pmid, abstract_text
      FROM medline_citation
      WHERE abstract_text != ''
      LIMIT %s
      ;
      """.format(limitNum)

      val rs = getResultSet(query)
      val xtr = new XmlTagRemover("AbstractText")

      import java.io._
      val pw = new PrintWriter(new File(outputFile))

      var lineNum = 0

      while(rs.next) {
        val pmid = rs.getString("pmid")
        val absData = rs.getString("abstract_text")
        //println(absData)
        //val absXml = scala.xml.XML.load(absBytes)
        //val absText = absXml \ "AbstractText"
        val absText = xtr.trim(absData)
        //val absText = absXml.text
        pw.write(pmid + "\t" + absText + "\n")
        lineNum += 1
        //println(lineNum)

        if (lineNum % 10000 == 0){
          println("%s lines writed to %s".format(lineNum, outputFile))
        }
        //print(pmid + ": " + absText)
        //println("[xml label]: %s".format(absXml.label))
        //println("[pmid]: %s".format(pmid))
        //println("[abstract_text]: %s".format(absText))
      }
      println("%s lines writed to %s".format(lineNum, outputFile))
      pw.close
    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close
  }
}
