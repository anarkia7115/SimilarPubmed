package algorithms

import scala.math

object SerializableClass extends java.io.Serializable {

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

}
