package topic

import org.scalatest._

class FileQueueSpec extends FlatSpec {
  it should "print hello world" in {
    val fq = new FileQueue("/tmp/portQueue", 8066 to 8071)
    //val fq = new FileQueue()
    println("hello world")
  }
}
