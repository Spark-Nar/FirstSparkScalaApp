package com.nar.spark

import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApplication {

  def main(args:Array[String]) : Unit = {
    //Creating spark config and Spark Context
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Spark App")

    val sc = new SparkContext(conf)

    // Creating RDD
    var rdd = sc.makeRDD(Array(1,2,3,4,5))
    rdd.collect().foreach(println)

    // Creating WordCount Program
    val inputfile = sc.textFile("E:\\Tutorials\\BigData\\Spark\\WordCountProgram\\wordCountText.txt")
    val counts = inputfile.flatMap( line => line.split(" ")).map( word => (word ,1)).reduceByKey(_ + _ )
    counts.cache()
    counts.saveAsTextFile("E:\\Tutorials\\BigData\\Spark\\WordCountProgram\\outputIntellij")

  }

}
