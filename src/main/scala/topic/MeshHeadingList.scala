package topic
import scala.io.Source
class MeshHeadingList(meshFile:String) {
  def load(): Unit ={
    for (line <- Source.fromFile(meshFile).getLines()) {
    }
  }
}
