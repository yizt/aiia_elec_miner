package com.es.analyze

import java.util.Date

import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree
import com.googlecode.concurrenttrees.radix.RadixTree
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import scala.io.Source

/**
  * Created by mick.yi on 2018/10/2.
  * 计算互信息
  */
object Pmia {
  def main(args: Array[String]) {
    val tree: RadixTree[Int] = new ConcurrentRadixTree[Int](new DefaultCharArrayNodeFactory)

    val wordInfoList = Source.fromFile("./src/main/resources/entropy_bg.txt").getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt, arrs(2).toDouble)
    }).toList

    val allwords=wordInfoList.map(_._1.split(",")).flatMap(x=>x).distinct

    val wordFreqList = Source.fromFile("out/freq_bg_elec/part-00000").getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt)
    }).toList.sortBy(_._1)
    val total: Long = wordFreqList.map(_._2).sum

    println(s"words:${allwords.length},freqwords:${wordFreqList.length},total:${total}")
    //构建树
    for (i <- 0 until wordFreqList.length) {
      val (word, freq) = wordFreqList(i)
      tree.put(word, freq) //排序很重要！！！
      if (i % 10000 == 0)
        println(s"${new Date()}load freq to radix tree done: ${i}")
    }

    println(tree.size())


    val rslt = wordInfoList.map { case (word, freq, entrop) => {
      val pmi = getPmi(tree, word, freq, total)
      //val pmi = getPmiFromMap(wordFreqList.toMap,word,freq,total)
      (word, freq, entrop, pmi)
    }
    }.
      //filter(_._4 >= 0).
      map { case (word, freq, entrop, pmi) => s"${word}\t${freq}\t${entrop}\t${pmi}" }

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
