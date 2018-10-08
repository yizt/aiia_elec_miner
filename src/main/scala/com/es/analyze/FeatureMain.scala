package com.es.analyze

import java.io.{FileOutputStream, OutputStreamWriter, BufferedWriter}
import com.hankcs.hanlp.HanLP
import scala.collection.JavaConversions._
import scala.io.Source
import com.es.analyze.FileUtil

/**
  * Created by mick.yi on 2018/9/26.
  * 训练和预测数据的特征生成
  */
object FeatureMain {
  def main(args: Array[String]) {
    val dealType = "filter" //filter or rank

    //候选词,用于生成带预测文件
    val candFile = "out/diff_result_all.txt"//"out/submit_30.txt"

    //训练，预测文件名
    val (postFiles, negFiles, trainFile, predFile) = if ("filter".equals(dealType)) {
      //正负样本过滤的
      val postFiles = Array("./src/main/resources/positive_dict.txt")
      val negFiles = Array("./src/main/resources/negtive_sample.txt",
        "./src/main/resources/negtive_paper_word.txt",
        "./src/main/resources/negtive_nz.txt")
      //训练、预测文件
      val trainFile = "out/train_filter.txt"
      val predFile = "out/predict_filter.txt"
      (postFiles, negFiles, trainFile, predFile)
    }
    else {
      //正负样本排序的
      val postFiles = Array("./src/main/resources/positive_manner.txt",
        "./src/main/resources/positive_todo.txt")
      val negFiles = Array("./src/main/resources/negtive_manner.txt",
        "./src/main/resources/negtive_todo.txt",
        "./src/main/resources/filter_words_0-1w.txt")
      //训练、预测文件
      val trainFile = "out/train_rank.txt"
      val predFile = "out/predict_rank.txt"
      (postFiles, negFiles, trainFile, predFile)
    }



    if ("filter".equals(dealType))
      dealFilterFeature(postFiles, negFiles, candFile, trainFile, predFile)
    else
      dealRankFeature(postFiles, negFiles, candFile, trainFile, predFile)

  }

  /**
    * 处理排序特征
    *
    * @param postFiles
    * @param negFiles
    * @param candFile
    * @param trainFile
    * @param predFile
    */
  def dealRankFeature(postFiles: Array[String], negFiles: Array[String], candFile: String, trainFile: String, predFile: String) = {
    // 正负样本获取
    val postWords = FileUtil.getFromFiles(postFiles: _*)
    val negWords = FileUtil.getFromFiles(negFiles: _*)

    // idf获取
    val charIdfMap = Source.fromFile("out/charidf/part-00000").getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      (arrs(0), arrs(1).toDouble)
    }).toMap

    val idfMap = Source.fromFile("out/idf/part-00000").getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      (arrs(0), arrs(1).toDouble)
    }).toMap

    val dwMap = Source.fromFile(candFile).getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      (arrs(0), arrs(1).toDouble)
    })


    //外部特征获取
    val outFeatureMap = Source.fromFile("./src/main/resources/wordInfo.txt").getLines().
      map(_.split("\t")).filter(_.length == 13).toList.tail.map(arrs => {
      val features = for (i <- 1 until arrs.length) yield {
        if (i <= 7 && i >= 5) arrs(i).toBoolean else arrs(i).toDouble
      }
      (arrs(0), features.mkString(","))
    }).toMap



    // 正样本热点字、词
    val hotWords = hotWord(postWords)
    val hotChars = hotChar(postWords)


    //    postWords.map((_, 1)). //正样本
    //      union(negWords.map((_, 0))).filter(elem=>{! outFeatureMap.contains(elem._1)}).foreach(println)
    //    println("======")
    //    dwMap.filter(elem=>{! outFeatureMap.contains(elem._1)}).foreach(println)

    // 训练预测数据特征生成
    val trainData = postWords.map((_, 1)). //正样本
      union(negWords.map((_, 0))). //负样本
      filter(elem => outFeatureMap.contains(elem._1)).
      map { case (word, label) => {
        val (charIdfSum, charIdfAvg, charIdfMin,charIdfMax) = charFeature(word, charIdfMap)
        val (wordIdfSum, wordIdfAvg,wordIdfMin,wordIdfMax) = wordFeature(word, idfMap)
        val (hotCharSum, hotCharAvg) = hotCharFeature(word, hotChars)
        val (hotWordSum, hotWordAvg) = hotWordFeature(word, hotWords)
        val outFeatures = outFeatureMap.get(word).get
        Array(word, label, word.length, charIdfSum, charIdfAvg, wordIdfSum, wordIdfAvg, hotCharSum, hotCharAvg, hotWordSum, hotWordAvg, outFeatures).mkString(",")
      }
      }



    val predData = dwMap.
      map { case (word, score) => {
        val (charIdfSum, charIdfAvg,charIdfMin, charIdfMax) = charFeature(word, charIdfMap)
        val (wordIdfSum, wordIdfAvg,wordIdfMin,wordIdfMax) = wordFeature(word, idfMap)
        val (hotCharSum, hotCharAvg) = hotCharFeature(word, hotChars)
        val (hotWordSum, hotWordAvg) = hotWordFeature(word, hotWords)
        val outFeatures = outFeatureMap.get(word).get
        Array(word, score, word.length, charIdfSum, charIdfAvg, wordIdfSum, wordIdfAvg, hotCharSum, hotCharAvg, hotWordSum, hotWordAvg, outFeatures).mkString(",")
      }
      }
    //生成训练和测试数据
    saveStrToFile(trainFile, trainData.mkString("\n"))
    saveStrToFile(predFile, predData.mkString("\n"))
  }

  /**
    * 处理过滤特征
    *
    * @param postFiles
    * @param negFiles
    * @param candFile
    * @param trainFile
    * @param predFile
    */
  def dealFilterFeature(postFiles: Array[String], negFiles: Array[String], candFile: String, trainFile: String, predFile: String) = {
    // 正负样本获取
    val postWords = FileUtil.getFromFiles(postFiles: _*)
    val negWords = FileUtil.getFromFiles(negFiles: _*)

    // idf获取
    val charIdfMap = Source.fromFile("out/charidf/part-00000").getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      (arrs(0), arrs(1).toDouble)
    }).toMap

    val idfMap = Source.fromFile("out/idf/part-00000").getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      (arrs(0), arrs(1).toDouble)
    }).toMap

    // 正样本热点字、词
    val hotWords = hotWord(postWords)
    val hotChars = hotChar(postWords)


    // 训练预测数据特征生成
    val trainData = postWords.map((_, 1.0)). //正样本
      union(negWords.map((_, 0.0))). //负样本
      map { case (word, label) => {
      val (charIdfSum, charIdfAvg, charIdfMin,charIdfMax) = charFeature(word, charIdfMap)
      val (wordIdfSum, wordIdfAvg,wordIdfMin,wordIdfMax) = wordFeature(word, idfMap)
      val (hotCharSum, hotCharAvg) = hotCharFeature(word, hotChars)
      val (hotWordSum, hotWordAvg) = hotWordFeature(word, hotWords)
      Array(word, label, word.length, charIdfSum, charIdfAvg,charIdfMin, charIdfMax, wordIdfSum, wordIdfAvg,
        wordIdfMin,wordIdfMax,hotCharSum, hotCharAvg, hotWordSum, hotWordAvg).mkString(",")
    }
    }


    val dwMap = Source.fromFile(candFile).getLines().
      map(_.split("\t")).filter(_.length == 2).map(arrs => {
      (arrs(0), arrs(1).toDouble)
    })

    val predData = dwMap.
      map { case (word, score) => {
        val (charIdfSum, charIdfAvg,charIdfMin, charIdfMax) = charFeature(word, charIdfMap)
        val (wordIdfSum, wordIdfAvg,wordIdfMin,wordIdfMax) = wordFeature(word, idfMap)
        val (hotCharSum, hotCharAvg) = hotCharFeature(word, hotChars)
        val (hotWordSum, hotWordAvg) = hotWordFeature(word, hotWords)
        Array(word, score, word.length, charIdfSum, charIdfAvg,charIdfMin, charIdfMax, wordIdfSum, wordIdfAvg,
          wordIdfMin,wordIdfMax, hotCharSum, hotCharAvg, hotWordSum, hotWordAvg).mkString(",")
      }
      }

    //生成训练和测试数据
    saveStrToFile(trainFile, trainData.mkString("\n"))
    saveStrToFile(predFile, predData.mkString("\n"))
  }

  /**
    * char idf的求和、均值
    *
    * @param word
    * @param charIdfMap
    * @return
    */
  def charFeature(word: String, charIdfMap: Map[String, Double]) = {
    val idfs = word.toList.map(_.toString).filter(charIdfMap.contains).map(charIdfMap(_))
    val (min,max) = if (idfs.isEmpty) (0.0,0.0) else (idfs.sortBy(x=>x).head,idfs.sortBy(x=>x).last)
    (idfs.sum, idfs.sum / (idfs.length + 1e-8),min,max)
  }

  /**
    * idf的求和、均值
    *
    * @param word
    * @param idfMap
    * @return
    */
  def wordFeature(word: String, idfMap: Map[String, Double]) = {
    val idfs = HanLP.segment(word).map(_.word).filter(idfMap.contains).map(idfMap)
    val (min,max) = if (idfs.isEmpty) (0.0,0.0) else (idfs.sortBy(x=>x).head,idfs.sortBy(x=>x).last)
    (idfs.sum, idfs.sum / (idfs.length + 1e-8),min,max)
  }


  def hotCharFeature(word: String, hotCharMap: Map[String, Int]) = {
    val idfs = word.toList.map(_.toString).filter(hotCharMap.contains).map(hotCharMap(_))
    (idfs.sum, idfs.sum.toDouble / (idfs.length + 1e-8))
  }

  def hotWordFeature(word: String, hotWordMap: Map[String, Int]) = {
    val idfs = HanLP.segment(word).map(_.word).filter(hotWordMap.contains).map(hotWordMap(_))
    (idfs.sum, idfs.sum.toDouble / (idfs.length + 1e-8))
  }

  def hotWord(wordList: List[String]) = {
    val rslt = wordList.map(HanLP.segment).
      flatMap(x => x).
      map(_.word).groupBy(x => x).
      mapValues(_.length).
      toList.sortBy(_._2 * -1).toMap //.map { case (w, f) => s"${w}\t${f}" }
    rslt
  }

  def hotChar(wordList: List[String]) = {
    val rslt = wordList.toList.
      flatMap(x => x).map(_.toString).
      groupBy(x => x).
      mapValues(_.length).
      toList.sortBy(_._2 * -1).toMap //.map { case (w, f) => s"${w}\t${f}" }
    rslt
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
