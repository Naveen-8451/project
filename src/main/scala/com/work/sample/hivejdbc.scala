package com.work.sample

import org.apache.hadoop.hive._
import java.sql.DriverManager
import java.sql.Connection
 object hivejdbc {
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    
      val database = readLine("enter your database")
      val driver = "org.apache.hive.jdbc.HiveDriver"
      val url = "jdbc:hive2://localhost/"+database+";transportMode=http;httpPath=cliservice"
      val username = "hive"
      val password = ""
    // there's probably a better way to do this
    var connection:Connection = null
    
    try {
      //make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url,username,password)
      println(connection)
      // create the statement, and run the select query
      val statement = connection.createStatement()
      while(true){
      val query = readLine("enter the query to execute:\n")
      val resultSet = statement.executeQuery(query)
      //println(resultSet.getString(0))
      while ( resultSet.next() ) {
        val id = resultSet.getString("id")
        println("id = " + id)
      }

      }
    }
      
      catch {
      case e => e.printStackTrace
    }
    connection.close()
  }
}