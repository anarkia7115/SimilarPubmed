package topic

import java.sql.{Connection, DriverManager}
import java.sql.ResultSet
import java.io._
import scala.io.Source
import scala.collection.mutable.ListBuffer

class Connector {
  val url = "jdbc:mysql://192.168.2.10:3306/pubmed"
  val driver = "com.mysql.jdbc.Driver"
  val username = "devuser"
  val password = "111111"
  var connection: Connection = _
  val limitNum = 5000000
  //val outputFile = "/home/shawn/git/SimilarPubmed/data/abs.txt"

  def execQuery(query:String): ResultSet = {
    if (connection == null || connection.isClosed()) {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    }
    val statement = connection.createStatement(
              java.sql.ResultSet.TYPE_FORWARD_ONLY,
              java.sql.ResultSet.CONCUR_READ_ONLY)

    //val statement = connection.createStatement()
    statement.setFetchSize(Integer.MIN_VALUE)

    val rs = statement.executeQuery(query)
    return rs
  }

  def execUpdate(query:String): Int = {
    if (connection == null || connection.isClosed()) {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    }
    val statement = connection.createStatement()

    val rs = statement.executeUpdate(query)
    return rs
  }

  def createPmidFocusTable(tableName:String):Unit = {

    val query1 = """
    drop table if exists %s;
    """.format(tableName)

    val query2 = """
    create table %s (pmid INT NOT NULL PRIMARY KEY );
    """.format(tableName)

    execUpdate(query1)
    execUpdate(query2)
    //connection.close
  }

  def generatePmidFocus(inputFile:String, outputFile:String
      , colNum:Int=0): Unit = {

    val pw = new PrintWriter(new File(outputFile))

    var prevPmid:String = ""

    for (line <- Source.fromFile(inputFile).getLines()) {
      val pmid = line.split("\t")(colNum)
      if (pmid == prevPmid) {
        // pass
      }
      else {
        pw.write("%s\n".format(pmid))
        prevPmid = pmid
      }
    }
    pw.close
  }

  def insertRecords(uniqLoadFile:String, tableName:String): Unit = {
    val query = """
    LOAD DATA LOCAL INFILE "%s"
    INTO TABLE %s
    COLUMNS TERMINATED BY '\t'
    LINES TERMINATED BY '\n';
    """.format(uniqLoadFile, tableName)
    execUpdate(query)
    //connection.close
  }

  def runTopicAll(outputFile:String): Unit = {
    val query = """
    select b.pmid, b.descriptor_name, b.descriptor_name_major_yn
    from medline_mesh_heading b
    """
    val rs = execQuery(query)
    val pw = new PrintWriter(new File(outputFile))

    var lineNum = 0

    while(rs.next) {
      val pmid = rs.getString("pmid")
      val descName = rs.getString("descriptor_name")
      val descMajorName = rs.getString("descriptor_name_major_yn")

      pw.write(pmid + "\t" + descName + "\t" + descMajorName + "\n")
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
    //connection.close
  }
  def runTopic(pmidTable:String, 
      outputFile:String): Unit = {
    val query = """
    select a.pmid, b.descriptor_name, descriptor_name_major_yn
    from %s a
    inner join medline_mesh_heading b
    on a.pmid = b.pmid;
    """.format(pmidTable)
    val rs = execQuery(query)
    val pw = new PrintWriter(new File(outputFile))

    var lineNum = 0

    while(rs.next) {
      val pmid = rs.getString("pmid")
      val descName = rs.getString("descriptor_name")
      val descMajorName = rs.getString("descriptor_name_major_yn")

      pw.write(pmid + "\t" + descName + "\t" + descMajorName + "\n")
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
    //connection.close
  }

  def runOneTitleAbs(pmid:Int):(String, String) = {

    val query = """
    SELECT article_title, abstract_text
    FROM medline_citation
    WHERE abstract_text != ''
    AND pmid = %s
    ;
    """.format(pmid)

    val rs = execQuery(query)
    var absText = ""
    var aTitle = ""

    if(rs.next){
    //val pmid = rs.getString("pmid").toInt
      aTitle = rs.getString("article_title").replaceAll("\t", " ").replaceAll("[^\\x00-\\x7F]", "")
      absText = rs.getString("abstract_text").replaceAll("\t", " ").replaceAll("[^\\x00-\\x7F]", "")
    }
    else {
      aTitle = ""
      absText = ""
    }

    rs.close
    return (aTitle, absText)
  }

  def runOneAbs(pmid:Int):String = {

    val query = """
    SELECT pmid, abstract_text
    FROM medline_citation
    WHERE abstract_text != ''
    AND pmid = %s
    ;
    """.format(pmid)

    val rs = execQuery(query)
    var absText = ""
    val xtr = new XmlTagRemover("AbstractText")

    if(rs.next){
    //val pmid = rs.getString("pmid").toInt
      absText = rs.getString("abstract_text").replaceAll("\t", " ").replaceAll("[^\\x00-\\x7F]", "")
    }
    else {
      absText = ""
    }
    rs.close
    return xtr.trim(absText)
  }

  def runOneTopic(pmid:Int):List[String] = {

    val query = """
    SELECT pmid, descriptor_name
    from medline_mesh_heading 
    WHERE 1 = 1
    AND pmid = %s
    ;
    """.format(pmid)

    val rs = execQuery(query)
    val topicList = ListBuffer[String]()

    while(rs.next) {
      val desName = rs.getString("descriptor_name")
      topicList += desName
    }
    return topicList.toList
  }

  def runExcludeAbs(excludePmidSet:scala.collection.Set[Int], outputFile:String):Unit = {

    val query = """
    SELECT pmid, abstract_text
    FROM medline_citation
    WHERE abstract_text != ''
    LIMIT %s
    ;
    """.format(limitNum)

    val rs = execQuery(query)
    val xtr = new XmlTagRemover("AbstractText")

    val pw = new PrintWriter(new File(outputFile))

    var lineNum = 0
    var skipLineNum = 0

    while(rs.next) {
      val pmid = rs.getString("pmid").toInt
      val absData = rs.getString("abstract_text")
      //println(absData)
      //val absXml = scala.xml.XML.load(absBytes)
      //val absText = absXml \ "AbstractText"
      val absText = xtr.trim(absData)
      //val absText = absXml.text
      if(!excludePmidSet.contains(pmid)) {
        pw.write(pmid + "\t" + absText + "\n")
        lineNum += 1
        if (lineNum % 10000 == 0){
          println("%s lines writed to %s".format(lineNum, outputFile))
        }
      }
      else{
        skipLineNum += 1
        if (skipLineNum % 5000 == 0){
          println("%s lines skipped. ")
        }
      }
      //println(lineNum)

      //print(pmid + ": " + absText)
      //println("[xml label]: %s".format(absXml.label))
      //println("[pmid]: %s".format(pmid))
      //println("[abstract_text]: %s".format(absText))
    }
    println("%s lines writed to %s".format(lineNum, outputFile))
    pw.close
    //connection.close
  }

  def runAbs(outputFile:String):Unit = {

    val query = """
    SELECT pmid, abstract_text
    FROM medline_citation
    WHERE abstract_text != ''
    ;
    """

    val rs = execQuery(query)
    val xtr = new XmlTagRemover("AbstractText")

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
    //connection.close
  }
}
