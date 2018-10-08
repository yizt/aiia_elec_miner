package com.es.analyze

import scala.io.Source

/**
  * Created by mick.yi on 2018/9/30.
  */
object KeyWords {
  def main(args: Array[String]) {

    val keyWords= FileUtil.getFromFiles("out/svm_dw_filter.0.2.csv")
    val baikePos = "./src/main/resources/baike_positive.txt"


    val baikeList = FileUtil.getFromFiles(baikePos)

    val kwPos = keyWords.filter(baikeList.contains)

    FileUtil.saveStrToFile("out/kw_filter02_baike.txt", kwPos.mkString("\n"))

  }

  def deal()={
    val rawData = Source.fromFile("D:\\competition\\elec_competition\\电力语料.txt").
      getLines().
      filter(line => {
        line.contains("关键词:") || line.contains("关键词：") || line.contains("关键词　")
      }).map(line => {
      val idx = line.replace("关键词　", "关键词:").replace("关键词：", "关键词:").indexOf("关键词:")
      var wordline = line.substring(idx + 4)
      var fidx = getEndIdx(wordline)
      wordline = if (fidx >= 0) wordline.substring(0, fidx) else wordline
      wordline = if (wordline.endsWith(";") || wordline.endsWith("；")) wordline.substring(0, wordline.length - 1) else wordline
      wordline
    }).map(_.split("　")).flatMap(x => x).
      map(_.split(":")).flatMap(x => x).
      map(_.split("，")).flatMap(x => x).
      map(_.split(",")).flatMap(x => x).
      map(_.split("、")).flatMap(x => x).
      map(_.split("：")).flatMap(x => x).map(word => {
      if (word.endsWith("1") || word.endsWith("0") || word.endsWith("。")) word.substring(0, word.length - 1) else word
    }).map(word => {
      val idx = getIdx(word, List("（", "("))
      if (idx >= 0) word.substring(0, idx) else word
    }).map(_.replace("KeyWords", "")).
      filter(word => {
        !word.contains("关键词") && !word.contains("\uF0B1") && !word.contains("\uF020") && word.length <= 12
      }).toList

    val keyWords = rawData.distinct.sortBy(_.length)

    val kwFreq=rawData.map((_,1)).groupBy(_._1).map(elem=>(elem._1,elem._2.length)).map{case(w,f)=>s"${w}\t${f}"}

    //keyWords.foreach(println)
    println(keyWords.length)
    FileUtil.saveStrToFile("out/keywords.txt", keyWords.mkString("\n"))
    FileUtil.saveStrToFile("out/kw_freq.txt", kwFreq.mkString("\n"))

    //positive命中
    //negtive命中

    val postFiles = Array("./src/main/resources/positive_manner.txt",
      "./src/main/resources/positive_todo.txt")
    val negFiles = Array("./src/main/resources/negtive_manner.txt",
      "./src/main/resources/negtive_todo.txt")
    val baikePos = "./src/main/resources/baike_positive.txt"

    val posList = FileUtil.getFromFiles(postFiles: _*)
    val negList = FileUtil.getFromFiles(negFiles: _*)
    val baikeList = FileUtil.getFromFiles(baikePos)

    val kwPos = keyWords.filter(posList.contains)
    val kwPosBaike = keyWords.filter(posList.contains).filter(baikeList.contains)
    val kwNeg = keyWords.filter(negList.contains)

    FileUtil.saveStrToFile("out/kw_pos.txt", kwPos.mkString("\n"))
    FileUtil.saveStrToFile("out/kw_pos_baike.txt", kwPosBaike.mkString("\n"))
    FileUtil.saveStrToFile("out/kw_neg.txt", negList.mkString("\n"))
  }

  def getEndIdx(line: String): Int = {
    val words = List("Keywords", "keywords", "Keywods", "中图分类号", "绪言", "引言", "引 言", "引　言", "概况", "概述", "概　述", "前言", "前　言")
    getIdx(line, words)
  }

  def getIdx(line: String, words: List[String]): Int = {
    val filters = words.map(line.indexOf).filter(_ >= 0)
    if (filters.isEmpty) -1 else filters.sorted.head
  }
}
