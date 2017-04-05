package topic
import scala.xml._

object SparkTest {
  def main(args:Array[String]) {
    val JOBS_LEGEND =
      <div class="legend-area"><svg width="150px" height="85px">
        <rect class="succeeded-job-legend"
          x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
        <text x="35px" y="17px">Succeeded</text>
        <rect class="failed-job-legend"
          x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
        <text x="35px" y="42px">Failed</text>
        <rect class="running-job-legend"
          x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
        <text x="35px" y="67px">Running</text>
      </svg></div>.toString.filter(_ != '\n')
    println("Hello, World!")
    //println(a)
  }
}
