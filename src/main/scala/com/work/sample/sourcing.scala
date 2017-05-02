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


object sourcing {


	def main(args: Array[String]) {

		//Hadoop Configuration
		val conf1 = new Configuration()
				conf1.set("fs.defaultFS", "hdfs://localhost:9000")  
				val fs= FileSystem.get(conf1)


				//Details Of Database and Table 
				val database =  readLine("Please enter Database Name:")
				val table = readLine("Enter Mysql Database Table:")
				var location = "hdfs://localhost:9000/user/"+table
				var status = fs.exists(new Path(location))
				//var status = new java.io.File(location.toString()).exists()
				println(status)
				if (status == true)
				{
					println (" Deleting directory : " +location)
					//FileUtils.deleteDirectory(new File(location.toString()))   
					fs.delete(new Path(location))
				}
		//var createPath = fs.create(new Path(location))


		val driver = "com.mysql.cj.jdbc.Driver"
				val url = "jdbc:mysql://localhost/"+database+"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
				val username = "scott"
				val password = "tiger"
				val conf = new SparkConf().setAppName("sourcing").setMaster("local[*]")
				val sc = new SparkContext(conf)
				val sqlContext = new SQLContext(sc)
				import sqlContext.implicits._
				// there's probably a better way to do this
				var connection:Connection = null

				try {
					// make the connection
					Class.forName(driver)
					connection = DriverManager.getConnection(url, username, password)
					val DataRdd = sqlContext.read.format("jdbc").option("url",url).option("driver",driver).option("dbtable",table).option("user",username).option("password",password).load()
					DataRdd.registerTempTable(table)
					val SaveRdd = sqlContext.sql("select * from "+ table)
					println(SaveRdd.columns.toString())
					SaveRdd.write.format("parquet").save(location+"/")
					val statement = connection.createStatement()
					val ResultSet = statement.executeQuery("SELECT * from "+table)
					val ResultSetMetaData = ResultSet.getMetaData()
					val id = ResultSetMetaData.getColumnName(1)
					val name = ResultSetMetaData.getColumnName(2)
					val year = ResultSetMetaData.getColumnName(3)
					
//					val columnCount = ResultSetMetaData.getColumnCount();
//					for(i <- 1 to columnCount){
//					  val file = new File(table+".hql")
//					  val bw = new BufferedWriter(new FileWriter(file))
//					  bw.write(ResultSetMetaData.getColumnName(i) + ", ");
//					  bw.close()
//					}
//					
//					while (ResultSet.next()) {
//						val row = "";
//						for(i <- 1 to columnCount){
//							val row = ResultSet.getString(i) + ", "; 
//							
//						}
//						System.out.println();
//						println(row);
//
//					}

										println(name)
										val file = new File(table+".hql")
										val bw = new BufferedWriter(new FileWriter(file))
										bw.write("create table " +table + "("+ id +","+name+"," +year+")"+"\n"+"Stored as Parquet"+"\n"+ "location '"+location+"'")
										bw.close()

				} catch {
				case e => e.printStackTrace
				}
				connection.close()
	}

}