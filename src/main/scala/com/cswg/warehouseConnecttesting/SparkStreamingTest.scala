package com.cswg.testing

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.util.ShutdownHookManager

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File

import com.typesafe.config.ConfigFactory

class SparkStreamingTest {
  
}

object SparkStreamingTest {
    var logger = LoggerFactory.getLogger("Examples");
  
  def getCurrentTime() : java.sql.Timestamp = {
     val now = Calendar.getInstance()
     var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)

  }
  
  def getCurrentDate(): String = {
    val now = Calendar.getInstance()
    var sdf = new SimpleDateFormat("yyyy-MM-dd")     
    return sdf.format(now.getTime)
  }
  
   def getCurrentDate(date: String): java.sql.Timestamp = {
     var sdf = new SimpleDateFormat("yyyy-MM-dd");
     return new java.sql.Timestamp(sdf.parse(date).getTime())
  }
   
  def getEnvironmentName(): (String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/environment_QA.conf"))
      val conf = ConfigFactory.load(parsedFile)
      ///opt/cassandra/default/resources/spark/conf/
      ///opt/cassandra/default/FileMove/
      val oracleEnvName  = conf.getString("environment.oracleEnv")
      val cassandraEnvName  = conf.getString("environment.cassandraEnv")
      logger.debug("oracleEnvName - " + oracleEnvName + "\n" + "cassandraEnvName - " + cassandraEnvName)
      
      (oracleEnvName, cassandraEnvName)
    }
   
   def getOracleConnectionProperties(): (String, String, String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/oracle_properties.conf"))
      val conf = ConfigFactory.load(parsedFile)
      ///opt/cassandra/default/resources/spark/conf/
      ///opt/cassandra/default/FileMove/
      val (oracleEnv, cassandraEnv) = getEnvironmentName()
      
      val driver  = conf.getString("exadata." + oracleEnv + ".driver")
      val conn  = conf.getString("exadata." + oracleEnv + ".conn")
      val userName  = conf.getString("exadata." + oracleEnv + ".username")
      val password  = conf.getString("exadata." + oracleEnv + ".password")
      logger.debug("driver - " + driver + "\n" + "conn - " + conn + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
      (driver, conn, userName, password)
    }
   
   def getCassandraConnectionProperties(): (String, String, String) = {
       val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/cassandra_properties.conf"))
       val conf = ConfigFactory.load(parsedFile)
       ///opt/cassandra/default/resources/spark/conf/
       ///opt/cassandra/default/FileMove/
       val (oracleEnv, cassandraEnv) = getEnvironmentName()
       
       val connectionHost = conf.getString("cassandra." + cassandraEnv + ".host")
       val userName = conf.getString("cassandra." + cassandraEnv + ".username")
       val password = conf.getString("cassandra." + cassandraEnv + ".password")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
       (connectionHost, userName, password)
   }
   
  def main(args: Array[String]) {    
     logger.debug("Main for Examples")
     val (hostName, userName, password) = getCassandraConnectionProperties()
     
     val conf = new SparkConf()
      .setAppName("WIPTrailerNeedsAvail")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      .set("spark.cassandra.output.ignoreNulls","true")
      .set("spark.cassandra.connection.host",hostName)
      //.set("spark.cassandra.connection.host","enjrhdbcasra01adev.cswg.com")
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      //.set("spark.eventLog.enabled", "true" )
      .setMaster("local[*]")  // This is required for IDE run
     
     val sc = new SparkContext(conf)
     val ssc = new StreamingContext(sc, Seconds(1)) 
     val sqlContext: SQLContext = new HiveContext(sc)
     val currentTime = getCurrentTime()
     val currentDate = getCurrentDate()
     
     //ssc.f
     
     try{
          ssc.textFileStream("/spark/MFT/inbound/SHIPPING")
          //ssc.s
       
     }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
     
     }
}