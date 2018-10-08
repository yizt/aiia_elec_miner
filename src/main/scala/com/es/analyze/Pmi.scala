package com.es.analyze

import java.util.Date

import com.googlecode.concurrenttrees.radix.RadixTree
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by mick.yi on 2018/10/2.
  * 计算互信息
  */
object Pmi {
  def main(args: Array[String]) {

  }

  def PmiMain(args: Array[String]) = {
    //args eg: local[8] out/entropy_bg/part-00000 out/entropy_pmi_bg_3.txt out/freq_bg_elec/part-00000
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val freqFile = args(3)
    val spark = SparkSession.builder.
      master(master).
      appName("PmiMain").
      config("spark.driver.memory", "10120m").
      config("spark.executor.memory", "10120m").
      config("spark.memory.offHeap.enabled", "True").
      config("spark.memory.offHeap.size", "5120m").
      config("spark.local.dir", "tmp").
      getOrCreate()

    //    val inputFile = "out/entropy_bg_elec/part-00000"
    //    val outputFile = "out/entropy_pmi_elec_bg_3.txt"
    //    val inputFile = "out/entropy_bg/part-00000"
    //    val outputFile = "out/entropy_pmi_bg_3.txt"
    val wordInfoList = Source.fromFile(inputFile).getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt, arrs(2).toDouble)
    }).toList


    //val freqFile = "out/freq_bg_elec/part-00000"
    val wordFreqList = Source.fromFile(freqFile).getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt)
    }).toList.sortBy(_._1)
    val total: Long = wordFreqList.map(_._2).sum
    val fMap = wordFreqList.filter(_._2>=5).toMap
    println(s"wordInfoList:${wordInfoList.length},freqwords:${wordFreqList.length},freqMapSize:${fMap.size},total:${total}")

    val bcfreqMap = spark.sparkContext.broadcast(fMap)

    val rslt = spark.sparkContext.parallelize(wordInfoList, 12).mapPartitions(iter => {
      val freqMap = bcfreqMap.value
      iter.map { case (word, freq, entropy) => {
        val pmi = getPmiFromMap(freqMap, word, freq, total)
        (word, freq, entropy, pmi)
      }
      }
    }).collect().
      sortBy(_._2 * -1).
      filter(_._4 > 1).
      map { case (word, freq, entropy, pmi) => s"${word}\t${freq}\t${entropy}\t${pmi}" }


    FileUtil.saveStrToFile(outputFile, rslt.mkString("\n"))
  }

  def getPmi(tree: RadixTree[Int], word: String, freq: Int, total: Long): Double = {
    if (word.contains(",")) {
      val ws = word.split(",")
      val lrEntrys = for (split <- 1 until ws.length) yield (ws.slice(0, split).mkString(","), ws.slice(split, ws.length).mkString(","))
      val ff = lrEntrys.map { case (lw, rw) => {
        val lfreq = tree.getValueForExactKey(lw)
        val rfreq = tree.getValueForExactKey(rw)
        lfreq * rfreq
      }
      }.max
      val pmi = Math.log(freq * total.toDouble / ff) / Math.log(2)
      pmi
    } else
      10.0 //单词给常数10
  }

  def getPmiFromMap(freqMap: Map[String, Int], word: String, freq: Int, total: Long): Double = {
    if (word.contains(",")) {
      val ws = word.split(",")
      val lrEntrys = for (split <- 1 until ws.length) yield (ws.slice(0, split).mkString(","), ws.slice(split, ws.length).mkString(","))
      val ff = lrEntrys.map { case (lw, rw) => {
        //println(s"lw:${lw},rw:${rw}")
        val lfreq = freqMap.get(lw).get
        val rfreq = freqMap.get(rw).get
        lfreq * rfreq
      }
      }.max
      val pmi = Math.log(freq * total.toDouble / ff) / Math.log(2)
      pmi
    } else
      0.8 * word.length + 0.5 //长度的线性值
  }


}
