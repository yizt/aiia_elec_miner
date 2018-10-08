package com.es.analyze

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.utility.CharacterHelper
import com.vdurmont.emoji.EmojiParser
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by mick.yi on 2018/9/26.
  */
object Test {
  def main(args: Array[String]) {

    //    println("\uF020".replaceAll("\\p{Graph}", "yes"))
    //    println(FileUtil.replaceEmoji("(\uF020,15310", "yes"))
    //    println(EmojiParser.removeAllEmojis("(\uF020,15310"))
    //
    //    //getALlSpecialChar()
    //    println(StringUtil.isReduplication("心心心心"))
    //    println(StringUtil.isReduplication("心心"))
    //    println(StringUtil.isReduplication("心"))
    "\uE40C"
    "\uF020"
    //    testPMI()
    //    testDYFZ()
    //    testFilterStopWords()
    //testHanLP()
   // dealStopWords()
//    val aa=stopWordSplitSentence(HanLP.segment("带有安全约束的电力系统有功功率、无功功率和电压的最优控制胡珠光傅书遏(电科院电网自动化研究所)内容提要本文根据PQ解辐条件").map(_.word).toList)
//    println(aa)
    //getALlSpecialCharLine()
    //val line="t\uF064\uF020\uF03D\uF020t1时刻电阻降到2\uF020\uF057，进入燃弧阶段。"
    val line="研究所用混响室的最低可用频率为８０ＭＨｚ，通过表１可知当敏感频率为２００ＭＨｚ时，搅拌器旋转一周的步进位置为５０个；"
    //val line="网易\uE40C网易女人\uE40C韩流明星写真：\uE40C池珍熙廉晶雅深情相拥\uE40C网易女人，更多精彩在首页，\uE40C我也评两句"
    //specialCharDeal(line)
    println(line)
    testHanLP(line)
  }

  val stopWords = FileUtil.getFromFiles("./src/main/resources/stopwords.txt").sortBy(_.length * -1)


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

  def dealStopWords() = {
    val stopWords: List[String] = FileUtil.getFromFiles("out/stopwords.txt").sortBy(_.length)
    val outfile = "./src/main/resources/stopwords.txt"
    //FileUtil.saveStrToFile(outfile, stopWords.mkString("\n"))
    stopWords.filter(_.length==1).foreach(println)


  }

  def specialCharDeal(line:String)={
    println(HanLP.convertToSimplifiedChinese(line))
  }

  def testHanLP(line:String) = {
    println(HanLP.segment(line))
    println(HanLP.newSegment().seg2sentence(line))
  }

  def testFilterStopWords() = {
    val origin = Source.fromFile("out/svm_dw_sort.csv").getLines().toList
    val stopWords = FileUtil.getFromFiles("./src/main/resources/stopwords.txt")
    val rslt = origin.filter(word => {
      stopWords.toStream.foldLeft(true)((a, sw) => {
        a && !word.startsWith(sw) && !word.endsWith(sw)
      })
    })
    FileUtil.saveStrToFile("out/svm_dw_sort_sw.csv", rslt.mkString("\n"))

    //评估postive和negtive命中数
    val postList = Source.fromFile("out/positives.txt").getLines().toList
    val negList = Source.fromFile("out/negtives.txt").getLines().toList
    println(s"命中positives数量:${rslt.intersect(postList).length},命中negtives数量:${rslt.intersect(negList).length}")
  }

  def testDYFZ() = {
    val stopWords: String = "的很了么呢是嘛个都也比还这于不与才上用就好在和对挺去后没说"
    val segToken = ","
    val data = Source.fromFile("D:\\competition\\elec_competition\\电力语料.txt").
      getLines().filter(_.contains("电压幅")).toList.distinct.map(line => {
      var nLine: String = HanLP.convertToSimplifiedChinese(line.map(CharacterHelper.regularize)). //全角转半角,繁体转简体，转为List[List[Term]]
        replaceAll("[" + stopWords + "]", segToken)
      nLine = FileUtil.replaceEmoji(nLine, segToken) //表情符号，以及自行使用区域 (Private Use Zone)替换
      nLine = Entropy.replaceNotChinese(nLine, segToken.charAt(0)) //非中文字符替换
      val sentences = nLine.split(segToken).
          filter(_.contains("电压幅")) //过滤空串
      //所有的句子
      sentences.toList
    })

    data.flatMap(x => x).filter(!_.contains("电压幅值")).foreach(println)
  }

  def testSomething() = {
    //    Array("了肯德基","；阿斯利康大姐夫").flatMap(x=>x).foreach(println)
    //
    //    "a,b,c,d".split(",").zipWithIndex.foreach(println)

    //    val outFeatures=Source.fromFile("./src/main/resources/wordInfo.txt").getLines().
    //      map(_.split("\t")).toList
    //    print(outFeatures.head.length)
    //    val data=Source.fromFile("out/keywords.txt").getLines().zipWithIndex.map(elem=>s"${elem._1}\t${elem._2 * -1}")
    //    FileUtil.saveStrToFile("out/kw.txt",data.mkString("\n"))
    //    val data=Source.fromFile("out/svm_dw_filter.csv").getLines().toList
    //    val posWords=Source.fromFile("out/positives.txt").getLines().toList
    //    val negWords=Source.fromFile("out/negtives.txt").getLines().toList
    //    val dataTop=for (i<-0 until 10000) yield data(i)
    //    val len=dataTop.filter(posWords.contains).length
    //    println(s"posWords: ${dataTop.filter(posWords.contains).length},negWords: ${dataTop.filter(negWords.contains).length}")
    //
    //    val dataSubmit=Source.fromFile("out/submit_f.csv").getLines().toList
    //
    //    val diff=dataSubmit.toSet diff data.toSet
    //    println(diff)

    //    val aa="a,b,c,d,c,e,f,a,b,c,d,c,e,f,a,b,c,d,c,e,f".split(",").toList.foldLeft(Map[String,Int]())((a,i)=>{
    //      a + (i->(a.getOrElse(i,0)+1))
    //    })
    //    println(aa)
    //
    //    HanLP.segment("杀掉了开发及,三相接地").toList.foreach(println)
    //
    //    HanLP.newSegment.seg2sentence("此时相当于图3能量送端SD侧在t0时发生三相接地短路故障。").toList.foreach(println)
    //
    //    val str="杀掉了开？发及、三相接地"
    //    println(str.replaceAll("[.，,。？“”：］［【】、+）．（％《》]", ","))
    //
    //    HanLP.newSegment.seg2sentence(str.replaceAll("[.，,。？“”：］［【】、+）．（％《》]", ",")).toList.foreach(println)
    //    "sa;dlkjtast\tas;dlkfj\tdsadf".split("\t").foreach(println)

    //testDistinc()
    //rankChar()
  }

  /**
    * 所有的非汉语字符排序统计
    */
  def rankChar() = {
    val wordFreqList = Source.fromFile("out/freq_bg_elec/part-00000").getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt)
    }).filter(_._1.length == 1).filter(elem => {
      !isChinese(elem._1.charAt(0))
    }).toList.sortBy(_._2 * -1)
    wordFreqList.foreach(println)
    println(wordFreqList.length)
    //FileUtil.saveStrToFile("out/special_char.txt",wordFreqList.map(_._1).distinct.mkString("\n"))
  }

  def getALlSpecialChar() = {
    val spark = SparkSession.builder.
      master("local[4]").
      appName("WordExtract").
      getOrCreate()

    //val filePath="D:\\competition\\elec_competition\\电力语料.txt"  //rdd.length: 950018,distrdd.lenght:917309
    val filePath = "out/news" //rdd.length: 1138319,distrdd.lenght:1070405
    val charList = spark.sparkContext.textFile(filePath).map(_.trim).mapPartitions(iter => {
        iter.map(_.toCharArray.filter(!isChinese(_))).flatMap(x => x).toList.distinct.toIterator
      }).repartition(1).distinct().collect()
    charList.foreach(println)
    println(charList.length)
    FileUtil.saveStrToFile("out/special_char.txt", charList.mkString("\n"))
  }
  def getALlSpecialCharLine() = {
    val spark = SparkSession.builder.
      master("local[4]").
      appName("WordExtract").
      getOrCreate()

    //val filePath="D:\\competition\\elec_competition\\电力语料.txt"  //rdd.length: 950018,distrdd.lenght:917309
    val filePath = "out/news" //rdd.length: 1138319,distrdd.lenght:1070405
    val charList = spark.sparkContext.textFile(filePath).map(_.trim).mapPartitions(iter => {
        iter.filter(line=>{
          line.foldLeft(true)((a,i)=>a&&isChinese(i))
        })
      }).distinct(1).collect()
    charList.foreach(println)
    println(charList.length)
    FileUtil.saveStrToFile("out/special_char_line.txt", charList.mkString("\n"))
  }

  /**
    * 输入的字符是否是汉字
    *
    * @param a char
    * @return boolean
    */
  def isChinese(a: Char): Boolean = {
    val v: Int = a.toInt
    return (v >= 19968 && v <= 171941)
  }

  /**
    * 查看重复行，对统计可能有较大影响
    */
  def testDistinc() = {

    val spark = SparkSession.builder.
      master("local[4]").
      appName("WordExtract").
      getOrCreate()

    //val filePath="D:\\competition\\elec_competition\\电力语料.txt"  //rdd.length: 950018,distrdd.lenght:917309
    val filePath = "out/news" //rdd.length: 1138319,distrdd.lenght:1070405
    val rdd = spark.sparkContext.textFile(filePath).map(_.trim)
    println(s"rdd.length: ${rdd.collect.length},distrdd.lenght:${rdd.distinct.collect.length}")
  }

  def test() = {
    val spark = SparkSession.builder.
      master("local[4]").
      appName("WordExtract").
      getOrCreate()
    val stopwords: String = "的很了么呢是嘛个都也比还这于不与才上用就好在和对挺去后没说"
    val segToken = ","
    spark.sparkContext.textFile("D:\\competition\\elec_competition\\电力语料.txt").filter(_.contains("三相接地")).map(
      line => {
        val nLine: String = HanLP.convertToSimplifiedChinese(line.map(CharacterHelper.regularize)). //全角转半角,繁体转简体，转为List[List[Term]]
          //val nLine: String = HanLP.convertToSimplifiedChinese(line).
          replaceAll("[" + stopwords + "]", segToken).
          replaceAll("\\p{Punct}", segToken).
          replaceAll("\\p{Alnum}", segToken).
          replaceAll("\\pP", segToken).
          replaceAll("　", segToken).
          replaceAll("\\p{Blank}", segToken).
          replaceAll("\\p{Space}", segToken).
          replaceAll("\\p{Cntrl}", segToken)
        nLine
      }).map(_.split(segToken).filter(_.contains("三相接地"))).
      flatMap(x => x).
      map(sentence => {
        HanLP.segment(sentence).map(_.word)
      }).
      repartition(1).saveAsTextFile("out/test")
  }


  def testPMI() = {
    val map = Map[String, Int](
      ("a" -> 10), ("b" -> 3), ("c" -> 3)
    )
    getPmiFromMap(map, "a,b,c,a,c,b", 30, 1000)
  }

  def getPmiFromMap(freqMap: Map[String, Int], word: String, freq: Int, total: Long): Double = {
    if (word.contains(",")) {
      val ws = word.split(",")
      val lrEntrys = for (split <- 1 until ws.length) yield (ws.slice(0, split).mkString(","), ws.slice(split, ws.length).mkString(","))
      val ff = lrEntrys.map { case (lw, rw) => {
        println(s"lw:${lw},rw:${rw}")
        val lfreq = freqMap.get(lw).getOrElse(0)
        val rfreq = freqMap.get(rw).getOrElse(0)
        lfreq * rfreq
      }
      }.max
      val pmi = Math.log(freq * total.toDouble / ff) / Math.log(2)
      pmi
    } else
      5.0 //单词给常数5
  }

}
