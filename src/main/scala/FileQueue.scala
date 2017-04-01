package topic
import java.nio.file.{Paths, Files}
import java.io.File
import scala.math

class FileQueue(parentFolder:String="/tmp/portQueue", portList:Range) {
  // constructor starts
  // singleton
  if (checkExists(parentFolder)) {
  }
  else{
    
    createFolder(parentFolder)

    for (p <- portList) {
      val fp = "%s/%s".format(parentFolder, p)
      createFolder(fp)
    }

  }

  // constructor ends

  def checkExists(folderPath: String): Boolean ={
    return Files.exists(Paths.get(folderPath))
  }

  def createFolder(folderPath: String): Boolean = {
    val dir = new File(folderPath)
    return dir.mkdir()
  }

  def deleteFolder(folderPath: String): Boolean = {
    val dir = new File(folderPath)
    return dir.delete()
  }

  def list(): Array[String] = {
    val dir = new File(parentFolder)
    return dir.list()
  }

  def deque(p: Int): Boolean = {
    val folderPath = "%s/%s".format(parentFolder, p.toString)
    return deleteFolder(folderPath)
  }

  def borrowOne(): Int = {
    val fq = list()
    val fsize = fq.size
    if(fsize == 0) {
      return 0
    }
    //val randomIndex = math.floor(math.random * (fsize - 0.001)).toInt
    val randomIndex = 0;
    println(fq(randomIndex))
    val one = fq(randomIndex).toInt
    val s = deque(one)
    if(s){
      println("borrow: %s success".format(one))
    }
    else{
      // fail to delete folder
      println("borrow failed")
      return borrowOne()
    }
    return one
  }

  def returnOne(p: Int): Unit = {
    val folderPath = "%s/%s".format(parentFolder, p.toString)
    if(createFolder(folderPath)){
      println("return: %s success".format(p))
    }
    else{
      println("return: %s failure".format(p))
    }
    //return createFolder(folderPath)
  }
}
