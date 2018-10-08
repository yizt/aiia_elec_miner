package com.es.analyze

import com.hankcs.hanlp.HanLP
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextInputFormat
import scala.collection.JavaConversions._
/**
  * Created by mick.yi on 2018/9/25.
  * 解析并分词
  */
object Segment {
  def main(args: Array[String]) {
    //args eg:   local[8] D:\competition\elec_competition\corpus out/segment

    val master = args(0)
    val inputPath = args(1)
    val outPath = args(2)


    val spark = SparkSession.builder.
      master(master).
      appName("Segment").
      getOrCreate()

    spark.sparkContext.
      hadoopFile[LongWritable, Text, TextInputFormat](inputPath).
      map(
        pair => {
          new String(pair._2.getBytes, 0, pair._2.getLength, "GBK")
        }).
      filter(_.startsWith("<content>")).
      map(line => {
        line.replace("<content>", "").replace("</content>", "")
      }).
//      map(line=>{
//      HanLP.segment(line).map(term=>term.word).mkString(" ")
//    }).
      repartition(128).
      saveAsTextFile(outPath)
  }

}
