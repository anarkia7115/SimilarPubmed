package algorithms

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import topic.PpScore

class Rank[T]() {
  def rank(df:Dataset[T], partitionField:String, orderField:String): DataFrame = {
    val w = Window.partitionBy(partitionField).orderBy(desc(orderField))
    return df.withColumn("rank", org.apache.spark.sql.functions.rank.over(w))
  }
}

