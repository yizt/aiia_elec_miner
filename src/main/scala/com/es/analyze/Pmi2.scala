package com.es.analyze

import java.util.Date

import com.googlecode.concurrenttrees.radix.{ConcurrentRadixTree, RadixTree}
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory

import scala.io.Source

/**
  * Created by mick.yi on 2018/10/2.
  * 计算互信息
  */
object Pmi2 {
  def main(args: Array[String]) {

    val wordInfoList = Source.fromFile("./src/main/resources/entropy_bg.txt").getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt, arrs(2).toDouble)
    }).toList


    val wordFreqList = Source.fromFile("out/freq_bg_elec/part-00000").getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt)
    }).toList.sortBy(_._1)
    val total: Long = wordFreqList.map(_._2).sum

    val freqMap=wordFreqList.toMap
    println(s"wordInfoList:${wordInfoList.length},freqwords:${wordFreqList.length},total:${total}")



//    val rslt = wordInfoList.map { case (word, freq, entropy) => {
//      val pmi = getPmiFromMap(freqMap,word,freq,total)
//      (word, freq, entropy, pmi)
//    }
//    }.map { case (word, freq, entropy, pmi) => s"${word}\t${freq}\t${entropy}\t${pmi}" }
    val aa=for (i<-0 until wordInfoList.length) yield {
      if (i % 10000 == 0)
        println(s"${new Date()} 完成: ${i}")
      val (word, freq, entropy)=wordInfoList(i)
      val pmi = getPmiFromMap(freqMap,word,freq,total)
      (word, freq, entropy, pmi)
    }
    val rslt=aa.map { case (word, freq, entropy, pmi) => s"${word}\t${freq}\t${entropy}\t${pmi}" }

    FileUtil.saveStrToFile("out/entropy_pmi_bg.txt", rslt.mkString("\n"))
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
    }  else
    10.0 //单词给常数10
  }

  def getPmiFromMap(freqMap: Map[String,Int], word: String, freq: Int, total: Long): Double = {
    if (word.contains(",")) {
      val ws = word.split(",")
      val lrEntrys = for (split <- 1 until ws.length) yield (ws.slice(0, split).mkString(","), ws.slice(split, ws.length).mkString(","))
      val ff = lrEntrys.map { case (lw, rw) => {
        val lfreq = freqMap.get(lw).get
        val rfreq = freqMap.get(rw).get
        lfreq * rfreq
      }
      }.max
      val pmi = Math.log(freq * total.toDouble / ff) / Math.log(2)
      pmi
    }  else
      10.0 //单词给常数10
  }


}
