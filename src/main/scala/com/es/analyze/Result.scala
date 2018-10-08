package com.es.analyze

import java.io.{FileOutputStream, OutputStreamWriter, BufferedWriter}

import com.hankcs.hanlp.HanLP
import scala.collection.JavaConversions._

import scala.io.Source

/**
  * Created by mick.yi on 2018/9/26.
  * 获取过滤后的最终结果
  */
object Result {
  def main(args: Array[String]) {

  }


  def resultMain()={
    val dealType="filter" // filter or rank

    val predFile=s"out/pred_${dealType}.txt"
    val candidate = Source.fromFile(predFile).getLines().map(line => {
      val arrs = line.split(",")
      (arrs(0), arrs(1).toDouble, arrs(2).toDouble)
    }).toList.sortBy(_._2 * -1)


    //saveTodo(candidate)
    //保存候选
    val threshHold=0.8
    val rslt=candidate.filter(_._3>threshHold).filter(_._1.length>1).sortBy(_._2 * -1)
    //      zipWithIndex.map{case((word,score,pred),idx)=>{
    //      val newIdx=Math.exp(0.5-pred)*idx
    //      ((word,score,pred),newIdx)
    //    }}.
    //      sortBy(_._2).map(_._1)
    saveResult(rslt,savefile=s"out/svm_dw_${dealType}.${threshHold}.csv")
    FileUtil.diffFilter(candidate,rslt,filename=s"out/diff_${dealType}.${threshHold}.txt")

    saveResult(candidate.filter(_._3<=threshHold).sortBy(_._3),s"out/filter_words_${dealType}.${threshHold}.txt")
  }



  /**
    * 过滤掉动词
    *
    * @param candidate
    */
  def verbFilter(candidate: List[(String, Double, Double)])={
    candidate.filter(elem=>{
     ! HanLP.segment(elem._1).last.nature.startsWith("v")
    })
  }

  /**
    * 保存结果
    *
    * @param candidate
    */
  def saveResult(candidate: List[(String, Double, Double)],savefile:String="out/svm_dw.csv") = {
    val rslt = candidate.map(_._1)
    saveStrToFile(savefile, rslt.mkString("\n"))
  }

  def reSort(candidate:List[(String, Double, Double)])={
    val baikePos= Source.fromFile("./src/main/resources/baike_positive.txt").getLines().toList.filter(_.trim.length > 0) distinct //union
      //Source.fromFile("out/todo_pos.txt").getLines().toList.filter(_.trim.length > 0) distinct

    candidate.zipWithIndex.map{case((word,score,pred),idx)=>{
      val newIdx=if(baikePos.contains(word)) idx.toDouble*0.8 else idx.toDouble
      ((word,score,pred),newIdx)
    }}.sortBy(_._2).map(_._1)
  }

  def saveTodo(candidate: List[(String, Double, Double)]) ={
    val baike= Source.fromFile("out/baike_positive.txt").getLines().toList.filter(_.trim.length > 0) union
      Source.fromFile("out/todo_pos.txt").getLines().toList.filter(_.trim.length > 0) union
      Source.fromFile("out/todo_neg.txt").getLines().toList.filter(_.trim.length > 0) union
      Source.fromFile("out/neg.txt").getLines().toList.filter(_.trim.length > 0)

    val rslt=candidate.filter(x=> ! baike.contains(x._1)).map(_._1).sortBy(x=>x)
    saveStrToFile("out/todo_last.csv", rslt.mkString("\n"))
  }

  /**
    * 保存信息到文件中
    */
  def saveStrToFile(fileName: String, content: String) = {
    val fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8"))
    fw.write(content.toString())
    fw.close()
  }

}
