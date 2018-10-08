package com.es.analyze

import java.io.{FileOutputStream, OutputStreamWriter, BufferedWriter}
import java.util.regex.{Matcher, Pattern}

import com.hankcs.hanlp.HanLP
import scala.collection.JavaConversions._

import scala.io.Source

/**
  * Created by mick.yi on 2018/9/25.
  */
object FileUtil {
  def main(args: Array[String]) {

    //posNegInter()

    createPosNet()

  }

  def createPosNet() = {
    //正负样本排序的
    val postFiles = Array("./src/main/resources/positive_manner.txt",
      "./src/main/resources/positive_todo.txt")
    val negFiles = Array("./src/main/resources/negtive_manner.txt",
      "./src/main/resources/negtive_todo.txt")

    saveStrToFile("out/positives.txt", getFromFiles(postFiles: _*).mkString("\n"))
    saveStrToFile("out/negtives.txt", getFromFiles(negFiles: _*).mkString("\n"))
  }

  /**
    * 查看分类器过滤后的效果
    *
    * @param candidate
    * @param filtered
    * @param topN
    */
  def diffFilter(candidate: List[(String, Double, Double)], filtered: List[(String, Double, Double)], topN: Int = 10000, filename: String = "") = {

    val candList = for (i <- 0 until Math.min(candidate.length, topN)) yield candidate(i)
    val filteredList = for (i <- 0 until Math.min(filtered.length, topN)) yield filtered(i)

    val candWords = candList.map(_._1)
    val filteredWords = filteredList.map(_._1)

    val candMinusFilter = candList.filter(elem => !filteredWords.contains(elem._1))
    println("=====分类器过滤掉：" + candMinusFilter.length)
    //candMinusFilter.foreach(println)

    val filterMinuscand = filteredList.filter(elem => !candWords.contains(elem._1))
    println("=====分类器增加：" + filterMinuscand.length)
    //filterMinuscand.foreach(println)

    val outStr = "=====分类器过滤掉的词如下：\n" + candMinusFilter.mkString("\n") +
      "\n\n\n=====分类器增加的词如下：\n" + filterMinuscand.mkString("\n")
    if (!"".equals(filename))
      saveStrToFile(filename, outStr)
  }

  /**
    * 正负样本的交集
    */
  def posNegInter(): Unit = {
    val postFiles = Array("./src/main/resources/positive_dict.txt",
      "./src/main/resources/positive_manner.txt",
      "./src/main/resources/positive_todo.txt")
    val posWords = FileUtil.getFromFiles(postFiles: _*)

    val negFiles = Array("./src/main/resources/negtive_sample.txt",
      "./src/main/resources/negtive_manner.txt",
      "./src/main/resources/negtive_todo.txt",
      "./src/main/resources/negtive_paper_word.txt",
      "./src/main/resources/negtive_nz.txt")
    val negWords = FileUtil.getFromFiles(negFiles: _*)

    val truePos = FileUtil.getFromFiles("./src/main/resources/true_positive.txt")
    println(posWords.length)
    println(negWords.length)
    val interWords = posWords intersect negWords
    println(interWords.length)
    val trueNeg = interWords.filter(!truePos.contains(_))

    negFiles.map(filename => {
      val negs = getFromFiles(filename).filter(!truePos.contains(_))
      saveStrToFile(filename, negs.mkString("\n"))
    })

    postFiles.map(filename => {
      val negs = getFromFiles(filename).filter(!trueNeg.contains(_))
      saveStrToFile(filename, negs.mkString("\n"))
    })
    //FileUtil.getFromFiles("./src/main/resources/positive_dict.txt") intersect inter foreach println
  }

  def dealTodo() = {
    val todoList = getFromFiles("out/todo.txt")
    val todo_pos = getFromFiles("./src/main/resources/positive_todo.txt")
    val todo_neg = todoList.filter(!todo_pos.contains(_))
    saveStrToFile("./src/main/resources/negtive_todo.txt", todo_neg.mkString("\n"))
  }

  def dealBaike(): Unit = {
    val dwList = Source.fromFile("./src/main/resources/dw.txt").getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      arrs(0)
    })


    val baike_pos = getFromFiles("out/baike_positive.txt")

    val baike_neg = dwList.filter(!baike_pos.contains(_))

    saveStrToFile("./src/main/resources/baike_negtive.txt", baike_neg.mkString("\n"))
  }

  /**
    * 获取指定文件中的词
    *
    * @param files
    * @return
    */
  def getFromFiles(files: String*) = {
    files.map(file => {
      Source.fromFile(file).getLines().toList.filter(_.trim.length > 0)
    }).flatMap(x => x).distinct.toList
  }

  /**
    * 保存信息到文件中
    */
  def saveStrToFile(fileName: String, content: String) = {
    val fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8"))
    fw.write(content.toString())
    fw.close()
  }

  /**
    * 输入的字符是否是汉字
    *
    * @param a char
    * @return boolean
    */
  def isChinese(a: Char): Boolean = {
    val v: Int = a.toInt
    return (v >= 19968 && v <= 171941) && !'\uE40C'.equals(a)
  }

  val EMOJI_PATTERN: Pattern = Pattern.compile("[\ud83c\udc00-\ud83c\udfff]|[\ud83d\udc00-\ud83d\udfff]|[\u2600-\u27ff]|[\uE000-\uF8FF]",
    Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE)

  /**
    * 替换表情符号
    *
    * @param source
    * @param replacement
    * @return
    */
  def replaceEmoji(source: String, replacement: String): String = {
    if (source != null) {
      val emojiMatcher: Matcher = EMOJI_PATTERN.matcher(source)
      if (emojiMatcher.find) {
        emojiMatcher.replaceAll(replacement)
      } else
        source
    }
    else
      source
  }

}
