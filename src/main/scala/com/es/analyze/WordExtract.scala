package com.es.analyze

import com.hankcs.hanlp.HanLP
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
  * Created by mick.yi on 2018/9/30.
  */
object WordExtract {

  def main(args: Array[String]): Unit = {
    //args eg:local[8] D:\competition\elec_competition\电力语料.txt out/entropy
    //args eg: local[8] out/news/part-00000 out/entropy
    //test()
    val master = args(0)
    val inputPath = args(1)
    val outPath = args(2)


    val spark = SparkSession.builder.
      master(master).
      appName("WordExtract").
      getOrCreate()

    val enRdd=dealEntropy(spark.sparkContext,inputPath)
    enRdd.repartition(1).sortBy(_._2 * -1).saveAsTextFile(outPath)
  }

  def dealEntropy(sc: SparkContext, filePath: String) = {
    val segRdd: RDD[List[String]] = sc.textFile(filePath).map(line => {
      HanLP.newSegment().seg2sentence(line). //转为List[List[Term]]
        map(termList => {
        termList.map(_.word).toList
      }) //转为List[List[String]]
    }).flatMap(x => x)

    //计算右熵
    val enRdd = calculateEntropy(segRdd.map(x => wordXxfix(x)).flatMap(x => x))
    enRdd
    //计算互信息
  }

  /**
    * 计算词的左右熵
    *
    * @param wordXxfix 词,前缀,后缀
    * @param minFreq   最小词频
    * @return
    */
  def calculateEntropy(wordXxfix: RDD[(String, String, String)], minFreq: Int = 3,minEntropy: Int = 2): RDD[(String, Int, Double)] = {
    wordXxfix.map { case (word, prefix, suffix) => (word, (1, Map(prefix->1), Map(suffix->1))) }. //(词频,前缀词频，后缀词频
      reduceByKey((a, b) => {
      (a._1 + b._1, mergeWordFreqMap(a._2, b._2), mergeWordFreqMap(a._3 , b._3))
    }).filter { case (_, (freq, _, _)) => freq >= minFreq }.
      map { case (word, (freq, prefixes, suffixes)) => {
        //val prefixCount = wordCount(prefixes) //前缀词统计
        val leftEntropy = entropy(prefixes.values.toList) //左熵

        //val suffixCount = wordCount(suffixes) //后缀词统计
        val rightEntropy = entropy(suffixes.values.toList) //右熵
        (word, freq, Math.min(leftEntropy, rightEntropy)) //词，词频，左右熵(左熵和右熵较小的值)
      }
      }.filter { case (_, _, en) => en > minEntropy } //左右熵过滤
  }

  /**
    * 计算信息熵
    *
    * @param freqs 前缀词|后缀词 词频列表
    * @return
    */
  def entropy(freqs: List[Int]): Double = {
    if (freqs.isEmpty || freqs.length == 1)
      0.0
    else {
      val total = freqs.sum
      -freqs.map(_.toDouble / total). //求概率p
        map(p => p * Math.log(p) / Math.log(2)).sum
    }
  }

  /**
    * 词频统计
    *
    * @param words
    * @return
    */
  def wordCount(words: List[String]): Map[String, Int] = {
    var m = Map[String, Int]()
    for (i <- 0 until words.length) {
      val word = words.get(i)
      if ("$".equals(word))
        m += (s"${word}${i}" -> 1) //使句子开头结束的词有充分多样性
      else
        m += (word -> (m.getOrElse(word, 0) + 1))
    }
    m
  }

  /**
    * 合并词频统计(减小内存占用)
    * @param mapa
    * @param mapb
    * @return
    */
  def mergeWordFreqMap(mapa:Map[String,Int],mapb:Map[String,Int])={
    val keySet=mapa.keys.toSet union mapb.keys.toSet
    var m = Map[String, Int]()
    for(key <- keySet){
      m+=(key->(mapa.getOrElse(key, 0) +mapb.getOrElse(key, 0)))
    }
    m
  }

  /**
    * 生成一个句子中所有的 词组合 ,前缀词，后缀词
    *
    * @param segWords 一个句子中的所有词
    * @return
    */
  def wordXxfix(segWords: Seq[String], maxLen: Int = 4) = {
    val nWords = "$" ++ segWords ++ "$" //句子的开始和结束用$符号表示
    val wordSet = for (start <- 1 until nWords.length - 1)
        yield {
          for (end <- start + 1 until Math.min(nWords.length, start + maxLen + 1))
            yield (nWords.slice(start, end).mkString(""), nWords(start - 1).toString, nWords(end).toString) //(词，前缀，后缀)
        }
    wordSet.flatMap(x => x)
  }


  def test() = {

    wordXxfix("a,b,c,d,e,f,g".split(",")).foreach(println)

    //wordCount(List("a", "b", "c", "a", "b", "c")).foreach(println)

    //wordCount(List("$", "$", "$", "c", "a", "b", "c")).foreach(println)

    println(entropy(List(2, 2, 3)))
  }
}
