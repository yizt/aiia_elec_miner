package com.es.analyze

import com.hankcs.hanlp.HanLP

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by mick.yi on 2018/10/2.
  */
object Main {

  def main(args: Array[String]) {
    //    //背景语料左右熵
    //    Entropy.entropyMain("local[16] out/news out/entropy_bg 2048".split(" "))
    //    //背景和前景语料左右熵
    //    Entropy.entropyMain("local[16] out/news_elec out/entropy_bg_elec 2048".split(" "))
    //
    //    //背景和前景语料词频统计
    //    Entropy.freqMain("local[16] out/news_elec out/freq_bg_elec 256".split(" "))
    //
    //    //互信息统计(背景)
    //    Pmi.PmiMain("local[4] out/entropy_bg/part-00000 out/entropy_pmi_bg.txt out/freq_bg_elec/part-00000".split(" "))
    //    //互信息统计(背景和前景)
    //    Pmi.PmiMain("local[4] out/entropy_bg_elec/part-00000 out/entropy_pmi_bg_elec.txt out/freq_bg_elec/part-00000".split(" "))

    //通过比较获取候选结果
    //dealDiff("out/entropy_pmi_bg.txt", "out/entropy_pmi_bg_elec.txt")

    resultMain()
    evalutor()
  }

  def dealDiff(bgFile: String, bgElecFile: String) = {
    val (diff, diff2, bgTop) = getDiff(bgFile, bgElecFile)

    FileUtil.saveStrToFile("out/diff_result.txt", sort(diff).mkString("\n"))

    FileUtil.saveStrToFile("out/diff_result_2.txt", diff2.mkString("\n"))

    FileUtil.saveStrToFile("out/diff_result_bgtop.txt", bgTop.mkString("\n"))

    val diffAll = diff union
      diff2.map { case (elem, _) => elem }
    val allTmp = sort(diffAll).
      filter(elem => !StringUtil.isReduplication(elem._1.replace(",", ""))) //过滤叠字

    val rslt = allTmp.map({ case (word, freq, _, _, sortVal) =>
      s"${word.replace(",", "")}\t${sortVal}"
    })
    //diff_all  原始特征
    val rsltFeature = allTmp.map({ case (word, freq, entropy, pmi, _) =>
      s"${word.replace(",", "")}\t${freq}\t${entropy}\t${pmi}"
    })

    FileUtil.saveStrToFile("out/diff_result_all.txt", rslt.mkString("\n"))

    FileUtil.saveStrToFile("out/diff_result_all_feature.txt", rsltFeature.mkString("\n"))


    println(diff.length)
    println(diff2.length)
  }


  def getDiff(bgFile: String, bgElecFile: String, minFreq: Int = 5, minEntropy: Double = 2, minPmi: Double = 1.5) = {
    //背景不过滤
    val bgList = Source.fromFile(bgFile).getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt, arrs(2).toDouble, arrs(3).toDouble)
    }).toList

    val bgMap = bgList.map { case (word, freq, entropy, pmi) => (word, (freq, entropy, pmi)) }.toMap


    val bgElecList = filter(Source.fromFile(bgElecFile).getLines().map(line => {
      val arrs = line.split("\t")
      (arrs(0), arrs(1).toInt, arrs(2).toDouble, arrs(3).toDouble)
    }).toList, minFreq, minEntropy, minPmi)

    println(s"${bgElecList.length},${bgList.length}")

    //多的部分
    val diff = bgElecList.filter(elem => !bgMap.contains(elem._1))

    //都有的，词频更高
    var diff2 = bgElecList.filter(elem => {
      bgMap.contains(elem._1) &&
        bgMap.get(elem._1).get._1 * 10 < elem._2 && //词频高10倍
        (bgMap.get(elem._1).get._2 < elem._3 || //左右熵更大
          bgMap.get(elem._1).get._3 < elem._4) //互信息更大
    }).map(elem => (elem, bgMap.get(elem._1).get))

    val diff2WordSet = diff2.map(_._1._1).toSet

    val bgTopWords = bgList.sortBy(_._2 * -1).slice(0, (bgElecList.length / 50).toInt).map(_._1)

    val bgTop = diff2.filter(x => bgTopWords.contains(x._1._1) && !x._1._1.contains("电"))

    diff2 = diff2.filter(x => !bgTopWords.contains(x._1._1) || x._1._1.contains("电"))

    (diff.toList, diff2.toList, bgTop)
  }

  def filter(wordList: List[(String, Int, Double, Double)], minFreq: Int, minEntropy: Double, minPmi: Double) = {
    wordList.filter { case (word, freq, entropy, pmi) => {
      freq >= minFreq && entropy >= minEntropy && pmi >= minPmi
    }
    }
  }

  def sort(wordList: List[(String, Int, Double, Double)]) = {
    wordList.map { case (word, freq, entropy, pmi) => {
      val ws = word.split(",")
      val sortVal = Math.sqrt(freq) * entropy * pmi * lengthFactor(word) //词频开根号*左右熵*互信息*
      (word, freq, entropy, pmi, sortVal)
    }
    }.sortBy(-1 * _._5)
  }

  /**
    * 词的长度因子，词太长或太短都抑制
    *
    * @param word
    * @return
    */
  def lengthFactor(word: String): Double = {
    1.0 / Math.exp(Math.abs(word.length - 4.5) * 0.3)
  }

  def resultMain() = {
    val dealType = "filter" // filter or rank

    //预测特征
    val predFile = s"out/pred_${dealType}.txt"
    val predMap = Source.fromFile(predFile).getLines().map(line => {
      val arrs = line.split(",")
      (arrs(0), arrs(2).toDouble)
    }).toMap

    val candFile = "out/diff_result_all_feature.txt"
    //所有词
    val candWords = Source.fromFile(candFile).getLines().
      map(_.split("\t")).map(arrs => {
      val (word, freq, entropy, pmi) = (arrs(0), arrs(1).toInt, arrs(2).toDouble, arrs(3).toDouble)
      val predVal = predMap.get(word).get
      s"${word}\t${freq}\t${entropy}\t${pmi}\t${predVal}"
    }).toList

    //saveTodo(candidate)
    FileUtil.saveStrToFile(s"out/svm_dw.csv", candWords.mkString("\n"))
    //FileUtil.diffFilter(candidate, rslt, filename = s"out/diff_${dealType}.${threshHold}.txt")

    //saveResult(candidate.filter(_._3 <= threshHold).sortBy(_._3), s"out/filter_words_${dealType}.${threshHold}.txt")
  }

  def evalutor() = {
    val candFile = "out/svm_dw.csv"
    val sortFile = "out/svm_dw_sort.csv"
    val sortFileFeature = "out/svm_dw_sort_feature.txt"
    //所有词
    val candWords = Source.fromFile(candFile).getLines().
      map(_.split("\t")).map(arrs => {
      val (word, freq, entropy, pmi, predVal) = (arrs(0), arrs(1).toInt, arrs(2).toDouble, arrs(3).toDouble, arrs(4).toDouble)
      (word, freq, entropy, pmi, predVal)
    }).toList

    //排序，查看结果

    def sortAndFilter(words: List[(String, Int, Double, Double, Double)]) = {
      val segment = HanLP.newSegment().enableAllNamedEntityRecognize(true)
      words.sortBy { case (word, freq, entropy, pmi, predVal) => {
        -Math.sqrt(freq) * Math.sqrt(freq) * entropy * pmi * lengthFactor(word) * Math.exp(predVal)
      }
      }.
        filter { case (word, freq, entropy, pmi, predVal) => predVal >= 0.3 && pmi >= 2 && freq >= 20 }. //过滤分类器预测小于0.1的
        filter { case (word, _, _, _, _) => {
        //过滤人名和单个动词
        val segList = segment.seg(word)
        if (segList.size() == 1 && (segList.get(0).nature.startsWith("nr") || segList.get(0).nature.startsWith("v")))
          false
        else
          true
      }
      }.filter { case (word, _, _, _, _) => {
        //不能开头的词，不能结尾的词
        val noBeginList = List("和", "变量", "机理").toStream
        val noEndList = List("和", "所示", "变量", "时", "随","前","后","可","参考","参考值").toStream
        val noInList = List("的", "函数", "次数", "误差", "参数", "处于", "计算", "使", "接口", "各","统计","网格","温度","方案","对数","配置").toStream
        !noBeginList.foldLeft(false)((a, i) => a || word.startsWith(i)) &&
          !noEndList.foldLeft(false)((a, i) => a || word.endsWith(i)) &&
          !noInList.foldLeft(false)((a, i) => a || word.contains(i))
      }
      }.filter { case (word, _, _, _, _) => {
        //停用词过滤
        true
      }
      }.filter { case (word, _, _, _, _) => {
        //词性过滤
        val segList = segment.seg(word)
        val natureList = segList.map(_.nature)
        if (natureList.head.startsWith("p")) //介词开头
          false
        else if (natureList.last.startsWith("f")) //方位词结束
          false
        else if (Stream("c", "u", "p", "r").foldLeft(false)((a, pos) => a ||
          natureList.foldLeft(false)((a1, n) => a1 || n.startsWith(pos)))) //不能包含连词，助词，介词，代词
          false
        else
          true
      }
      }
    }
    val sortCands = sortAndFilter(candWords)

    //评估postive和negtive命中数
    val postList = Source.fromFile("out/positives.txt").getLines().toList
    val negList = Source.fromFile("out/negtives.txt").getLines().toList
    val todoEvalList = sortCands.slice(0, 10000).map(_._1)
    println(s"命中positives数量:${todoEvalList.intersect(postList).length},命中negtives数量:${todoEvalList.intersect(negList).length}")
    //保存结果
    FileUtil.saveStrToFile(sortFile, sortCands.map(_._1).mkString("\n"))
    FileUtil.saveStrToFile(sortFileFeature, sortCands.mkString("\n"))
  }

  /**
    * 保存结果
    *
    * @param candidate
    */
  def saveResult(candidate: List[(String, Double, Double)], savefile: String = "out/svm_dw.csv") = {
    val rslt = candidate.map(_._1)
    FileUtil.saveStrToFile(savefile, rslt.mkString("\n"))
  }

}
