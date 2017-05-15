package com.work.sample

import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.commons.io.FileUtils._
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import org.jboss.netty.handler.queue.BufferedWriteHandler
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.log4j.{Level,Logger}
import scala.util.Properties
import java.util.Properties


object multi_tables extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("SLF4J").setLevel(Level.OFF)
  val database = readLine("enter database Name:")
 val props = new Properties
    props.setProperty("user", "scott")
    props.setProperty("password", "tiger")
    //props.setProperty("header", "true")
    //props.setProperty("inferSchema", "true")
  
        val driver = "com.mysql.cj.jdbc.Driver"
				val url = "jdbc:mysql://localhost/"+database+"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
				val username = "scott"
				val password = "tiger"
				val conf = new SparkConf().setAppName("sourcing").setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer.mb","24")
				val sc = new SparkContext(conf)
				val sqlContext = new SQLContext(sc)
		    sc.setLogLevel("WARN")
				import sqlContext.implicits._
				// there's probably a better way to do this
				var connection:Connection = null
				var table = ""
        val tables =  List("movie", "genre", "user")
        val dfs = for { table <- tables} 
        yield (table, sqlContext.read.jdbc(url, table, props));
       for { (name, df) <- dfs }
       df.toDF().printSchema()
       //write.format("parquet").save()
       
       
       
       //df.toDF().write.format("parquet").save("/Users/NaveenKumar/workspace/project/"+table)
       //val saveRDD = 
       
        

}