package com.work.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object spark_example1 extends App {
  val conf = new SparkConf().setAppName("Spark_example1").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val file = sc.textFile("employee.txt")
  val file_filter = file.filter(x => x.contains("amith"))
  file_filter.persist()
  val a = file_filter.count()
  println(a)

  
  
  
}