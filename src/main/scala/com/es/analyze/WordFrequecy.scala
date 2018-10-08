package com.es.analyze

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by mick.yi on 2018/9/25.
  */
object WordFrequecy {
  def main(args: Array[String]) {
    //args eg:   local[8] out/segment out/idf
    val master = args(0)
    val inputPath = args(1)
    val outPath = args(2)

    val spark = SparkSession.builder.
      master(master).
      appName("WordFrequecy").
      getOrCreate()

    dealGram2(spark, inputPath, outPath)
    //saveidf(spark, inputPath, outPath)
    //savetf(spark, inputPath, outPath)
    //saveCharTf(spark, inputPath, outPath)
    //saveCharidf(spark, inputPath, outPath)
  }

  /**
    * 逆文档字频统计
    *
    * @param spark
    * @param inputPath
    * @param outputPath
    */
  def saveCharidf(spark: SparkSession, inputPath: String, outputPath: String) = {
    val rdd = spark.sparkContext.textFile(inputPath)

    val docCount = rdd.count()
    val idfRdd = rdd.map(line => {
      line.split(" ").flatMap(x => x).distinct.map((_, 1))
    }).flatMap(x => x).reduceByKey(_ + _).map { case (word, wf) => {
      (word, Math.log(docCount.toDouble / wf))
    }
    }.repartition(1).sortBy(_._2).map(m => s"${m._1}\t${m._2}").saveAsTextFile(outputPath)
  }

  /**
    * 保存字词频
    *
    * @param spark
    * @param inputPath
    * @param outputPath
    */
  def saveCharTf(spark: SparkSession, inputPath: String, outputPath: String) = {
    spark.sparkContext.textFile(inputPath).map(line => {
      line.split(" ").flatMap(x => x).map((_, 1))
    }).flatMap(x => x).reduceByKey(_ + _).
      repartition(1).
      sortBy(_._2, ascending = false).
      map { case (word, tf) => {
        s"${word}\t${tf}"
      }
      }.saveAsTextFile(outputPath)
  }

  /**
    * 词频统计
    *
    * @param spark
    * @param inputPath
    * @param outputPath
    */
  def savetf(spark: SparkSession, inputPath: String, outputPath: String) = {
    spark.sparkContext.textFile(inputPath).map(line => {
      line.split(" ").map((_, 1))
    }).flatMap(x => x).reduceByKey(_ + _).
      repartition(1).
      sortBy(_._2, ascending = false).
      map { case (word, tf) => {
        s"${word}\t${tf}"
      }
      }.saveAsTextFile(outputPath)
  }

  /**
    * 逆文档词频统计
    *
    * @param spark
    * @param inputPath
    * @param outputPath
    */
  def saveidf(spark: SparkSession, inputPath: String, outputPath: String) = {
    val rdd = spark.sparkContext.textFile(inputPath)

    val docCount = rdd.count()
    val idfRdd = rdd.map(line => {
      line.split(" ").distinct.map((_, 1))
    }).flatMap(x => x).reduceByKey(_ + _).map { case (word, wf) => {
      (word, Math.log(docCount.toDouble / wf))
    }
    }.repartition(1).sortBy(_._2).map(m => s"${m._1}\t${m._2}").saveAsTextFile(outputPath)
  }

  /**
    * 2Gram统计
    *
    * @param spark
    * @param inputPath
    * @param outputPath
    */
  def dealGram2(spark: SparkSession, inputPath: String, outputPath: String) = {
    //val words = spark.sparkContext.broadcast(getAllWords())
    //val wordsRdd=spark.sparkContext.parallelize(words)
    val gram2Rdd = spark.sparkContext.textFile(inputPath).map(line => {
      val arrs = line.split(" ")
      val gram2 = for (i <- 0 until arrs.length - 1) yield s"${arrs(i)}${arrs(i + 1)}"
      gram2
    }).
      flatMap(x => x).
      map((_, 1)).reduceByKey(_ + _).repartition(16).
      sortBy(_._2 * -1). //倒序
      map { case (key, value) => {
      s"${key}\t${value}"
    }
    }.saveAsTextFile(outputPath)
  }

  def getAllWords() = {
    val aa = Source.fromFile("./src/main/resources/dw.txt").getLines().map(line => {
      val arrs = line.split("\t")
      arrs(0).
        replaceAll("[\\[|\\*]", "") //剔除特殊字符
    }).toArray
    aa
  }
}
