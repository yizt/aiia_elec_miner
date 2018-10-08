package com.es.analyze

import com.hankcs.hanlp.HanLP
import scala.collection.JavaConversions._
import scala.io.Source
/**
  * Created by mick.yi on 2018/9/29.
  */
object Annotation {
  def main(args: Array[String]) {

    val negWords=Source.fromFile("./src/main/resources/negtive_todo.txt").getLines().map(line=>{
      (line,HanLP.segment(line).last.nature.name)
    })



    val allWords = Source.fromFile("out/pred_filter.txt").getLines().map(line => {
      val arrs = line.split(",")
      (arrs(0), arrs(1).toDouble, arrs(2).toDouble)
    }).toList.sortBy(_._2 * -1).map(elem=>(elem._1,elem)).toMap

    negWords.filter(elem=>{
      elem._2.startsWith("n") || elem._2.equals("vn")
    }).filter(x=>allWords.contains(x._1)).map(elem=>{
     val (word,rank,score)= allWords.get(elem._1).get
      (word,rank,score)
    }).filter(_._2 > -10000).toList.sortBy(_._3 * -1).map(_._1).foreach(println)


  }

}
