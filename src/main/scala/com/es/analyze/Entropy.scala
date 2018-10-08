package com.es.analyze

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.hankcs.hanlp.utility.CharacterHelper
import scala.collection.JavaConversions._

/**
  * Created by mick.yi on 2018/9/30.
  */
object Entropy {
  //val specialChars: String = FileUtil.getFromFiles("./src/main/resources/special_char.txt").mkString("")

  def main(args: Array[String]): Unit = {
    //args eg: local[16] out/news out/entropy_bg_elec
    //        local[16] D:\competition\elec_competition\电力语料.txt out/en
    //        local[16] out/news out/freq_bg_elec
    //test()
    //entropyMain(args)
    freqMain(args)
  }

  def entropyMain(args: Array[String]) = {
    val master = args(0)
    val inputPath = args(1)
    val outPath = args(2)
    val partitions = args(3).toInt


    val spark = SparkSession.builder.
      master(master).
      appName("WordExtract").
      config("spark.driver.memory", "5120m").
      config("spark.executor.memory", "5120m").
      //      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      //      config("spark.rdd.compress", "true").
      //      config("spark.storage.memoryFraction", "1").
      config("spark.memory.offHeap.enabled", "True").
      config("spark.memory.offHeap.size", "5120m").
      config("spark.local.dir", "tmp").
      getOrCreate()

    val enRdd = dealEntropy(spark.sparkContext, inputPath, partitions)
    enRdd.sortBy(_._2 * -1, numPartitions = 1).
      map { case (word, freq, entropy) => s"${word}\t${freq}\t${entropy}" }.
      saveAsTextFile(outPath)
  }

  def dealEntropy(sc: SparkContext, filePath: String, partitions: Int, minFreq: Int = 5, minEntropy: Double = 1.5) = {

    val segRdd: RDD[List[String]] = toSentenceSeg(sc.textFile(filePath).distinct). //filter(_.contains("三相接地")).
      repartition(partitions) //RDD[List[String]]

    val enRdd = segRdd.map(x => wordXxfix(x)). //计算句子的所有词组合、前缀词、后缀词
      mapPartitions(iter => {
      localFold(iter.flatMap(x => x))
    }).reduceByKey((a, b) => {
      (a._1 + b._1, mergeWordFreqMap(a._2, b._2), mergeWordFreqMap(a._3, b._3))
    }).filter { case (_, (freq, _, _)) => freq >= minFreq }. //词频过滤
      map { case (word, (freq, prefixes, suffixes)) => {
      val leftEntropy = entropy(prefixes, freq) //左熵
      val rightEntropy = entropy(suffixes, freq) //右熵
      val en = if (word.contains(",")) Math.min(leftEntropy, rightEntropy) else Math.log(freq) //非组合词,都是已经成词了的，熵使用词频的对数
      (word, freq, en) //词，词频，左右熵(左熵和右熵较小的值)
    }
    }.filter { case (_, _, en) => en >= minEntropy } //左右熵过滤

    enRdd

  }

  def dealFrequence(sc: SparkContext, filePath: String, partitions: Int, minFreq: Int = 5) = {
    val segRdd: RDD[List[String]] = toSentenceSeg(sc.textFile(filePath).distinct).repartition(partitions)
    segRdd.map(x => wordXxfix(x).map(_._1)).mapPartitions(iter => {
      localFoldFrequence(iter.flatMap(x => x))
    }).reduceByKey(_ + _).filter(_._2 >= minFreq)
  }

  def freqMain(args: Array[String]) = {
    val master = args(0)
    val inputPath = args(1)
    val outPath = args(2)
    val partitions = args(3).toInt


    val spark = SparkSession.builder.
      master(master).
      appName("WordExtract").
      getOrCreate()

    val freqRdd = dealFrequence(spark.sparkContext, inputPath, partitions)
    freqRdd.sortBy(_._2 * -1, numPartitions = 1).
      map { case (word, freq) => s"${word}\t${freq}" }.
      saveAsTextFile(outPath)
  }


  def toSentenceSeg(lineRdd: RDD[String]) = {

    val segToken = ","
    val stopWords = FileUtil.getFromFiles("./src/main/resources/stopwords.txt").sortBy(_.length * -1)
    //    def dealStopWords(line: String) = {
    //      var rslt = line
    //      stopWords.foreach(stopWord => {
    //        rslt = rslt.replace(stopWord, segToken)
    //      })
    //      rslt
    //    }
    //    def isStopWords(word: String) = {
    //      if (stopWords.contains(word)) true else false
    //      //stopWords.foldLeft(false)((a,sw)=>word.contains(sw))
    //    }
    def stopWordSplitSentence(wordList: List[String]): List[List[String]] = {
      val idxList = wordList.zipWithIndex.filter(x => stopWords.contains(x._1)).map(_._2).
        +:(-1).:+(wordList.length) //增加头尾
      val sentenceList = for (i <- 0 until idxList.length - 1) yield {
          val start = idxList(i) + 1
          val end = idxList(i + 1)
          if (start < end) wordList.slice(start, end)
          else List.empty
        }
      sentenceList.filter(!_.isEmpty).toList
    }
    /**
      * 分割句子
      * a)标点符号、连词，助词，介词，代词
      * b)非中文字
      * c)停用词
      *
      * @param wordList
      * @return
      */
    def splitSentence(wordList: List[Term]): List[List[String]] = {
      val idxList = wordList.zipWithIndex.filter { case (term, idx) => { //找到所有分割点
        if (Stream("w", "c", "u", "p", "r").foldLeft(false)((a, pos) => a || term.nature.startsWith(pos)))
          true
        else if (!term.word.foldLeft(true)((a, c) => a && FileUtil.isChinese(c)))
          true
        else if (stopWords.contains(term.word))
          true
        else
          false
      }
      }.map(_._2).
        +:(-1).:+(wordList.length) //增加头尾
      val sentenceList = for (i <- 0 until idxList.length - 1) yield {
          val start = idxList(i) + 1
          val end = idxList(i + 1)
          if (start < end) wordList.slice(start, end).map(_.word)
          else List.empty
        }
      sentenceList.filter(!_.isEmpty).toList
    }

    lineRdd.mapPartitions(iter => {
      val seg = NLPTokenizer.SEGMENT
      iter.map(line => {
        splitSentence(seg.seg(line).toList)
      }).
        flatMap(x => x) //打平
    })
  }

  def localFold(wordXxfixIter: Iterator[(String, String, String)]): Iterator[(String, (Int, Map[String, Int], Map[String, Int]))] = {
    val (preMap, sufMap, wordFreqMap) = wordXxfixIter.
      foldLeft(Map[String, Map[String, Int]](), Map[String, Map[String, Int]](), Map[String, Int]())((a, i) => {
        val (word, prefix, suffix) = i
        val prefixMap = a._1.getOrElse(word, Map[String, Int]())
        val suffixMap = a._2.getOrElse(word, Map[String, Int]())
        val freq = a._3.getOrElse(word, 0)
        (a._1 + (word -> (prefixMap + (prefix -> (prefixMap.getOrElse(prefix, 0) + 1)))),
          a._2 + (word -> (suffixMap + (suffix -> (suffixMap.getOrElse(suffix, 0) + 1)))),
          a._3 + (word -> (freq + 1))
          )
      })

    preMap.map { case (word, pWordMap) => {
      //(词,(词频,前缀词频,后缀词频))
      (word, (wordFreqMap.getOrElse(word, 0), pWordMap, sufMap.getOrElse(word, Map[String, Int]())))
    }
    }.toIterator
  }

  /**
    * 词频local分区统计
    *
    * @param wordXxfixIter
    * @return
    */
  def localFoldFrequence(wordXxfixIter: Iterator[String]): Iterator[(String, Int)] = {
    val wordFreqMap: Map[String, Int] = wordXxfixIter.
      foldLeft(Map[String, Int]())((a, i) => {
        val word = i
        val freq = a.getOrElse(word, 0)
        a + (word -> (freq + 1))
      })
    wordFreqMap.toIterator
  }

  //  /**
  //    * 计算词的左右熵
  //    *
  //    * @param wordXxfix 词,前缀,后缀
  //    * @param minFreq   最小词频
  //    * @return
  //    */
  //  def calculateEntropy(wordXxfix: RDD[(String, String, String)], minFreq: Int = 3): RDD[(String, Int, Double)] = {
  //    wordXxfix.map { case (word, prefix, suffix) => (word, (1, Map(prefix -> 1), Map(suffix -> 1))) }. //(词频,前缀词频，后缀词频
  //      reduceByKey((a, b) => {
  //      (a._1 + b._1, mergeWordFreqMap(a._2, b._2), mergeWordFreqMap(a._3, b._3))
  //    }).filter { case (_, (freq, _, _)) => freq >= minFreq }.
  //      map { case (word, (freq, prefixes, suffixes)) => {
  //        //val prefixCount = wordCount(prefixes) //前缀词统计
  //        val leftEntropy = entropy(prefixes.values.toList) //左熵
  //
  //        //val suffixCount = wordCount(suffixes) //后缀词统计
  //        val rightEntropy = entropy(suffixes.values.toList) //右熵
  //        (word, freq, Math.min(leftEntropy, rightEntropy)) //词，词频，左右熵(左熵和右熵较小的值)
  //      }
  //      }
  //  }

  /**
    * 计算信息熵
    *
    * @param xxfixFreqs 前缀词|后缀词、词频map
    * @return
    */
  def entropy(xxfixFreqs: Map[String, Int], total: Int): Double = {
    if (xxfixFreqs.isEmpty)
      0.0
    else {
      //分为两部分计算，句首的左熵和句尾的右熵充分大
      val segStartEnd = xxfixFreqs.filter(elem => "$".equals(elem._1)).values
      val segStartEndEntropy = if (segStartEnd.isEmpty) 0 else -Math.log(segStartEnd.head.toDouble / total) / Math.log(2) //toDouble很重要，切记切记
      val others = xxfixFreqs.filter(elem => !"$".equals(elem._1)).values
      -others.map(_.toDouble / total). //求概率p
        map(p => p * Math.log(p) / Math.log(2)).sum + segStartEndEntropy
    }
  }

  //  /**
  //    * 词频统计
  //    *
  //    * @param words
  //    * @return
  //    */
  //  def wordCount(words: List[String]): Map[String, Int] = {
  //    var m = Map[String, Int]()
  //    for (i <- 0 until words.length) {
  //      val word = words.get(i)
  //      if ("$".equals(word))
  //        m += (s"${word}${i}" -> 1) //使句子开头结束的词有充分多样性
  //      else
  //        m += (word -> (m.getOrElse(word, 0) + 1))
  //    }
  //    m
  //  }

  /**
    * 合并词频统计(减小内存占用)
    *
    * @param mapa
    * @param mapb
    * @return
    */
  def mergeWordFreqMap(mapa: Map[String, Int], mapb: Map[String, Int]) = {
    val keySet = mapa.keys.toSet union mapb.keys.toSet
    var m = Map[String, Int]()
    for (key <- keySet) {
      m += (key -> (mapa.getOrElse(key, 0) + mapb.getOrElse(key, 0)))
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
            yield (nWords.slice(start, end).mkString(","), nWords(start - 1).toString, nWords(end).toString) //(词，前缀，后缀)
        }
    wordSet.flatMap(x => x)
  }

  def replaceNotChinese(str: String, raplaceChar: Char): String = {
    str.map(chr => {
      if (FileUtil.isChinese(chr)) chr else raplaceChar
    })
  }


  def test() = {

    wordXxfix("母线,三相,接地,故障".split(",")).foreach(println)
    println(-Math.log(1.0 / 20003))
    println(-Math.log(1.0 / Math.sqrt(203)))

    //wordCount(List("a", "b", "c", "a", "b", "c")).foreach(println)

    //wordCount(List("$", "$", "$", "c", "a", "b", "c")).foreach(println)

    //println(entropy(List(2, 2, 3)))
  }
}
