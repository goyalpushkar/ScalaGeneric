package com.cswg.testing

import scala.concurrent.{ Future, future, Await }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.util.Random
import org.slf4j.LoggerFactory

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.hive.HiveContext

import com.typesafe.config.ConfigFactory
import java.io.File

class HelloFuture {
  
}

// 0 -> Environment Name
// 1 -> Job Id
object HelloFuture {
  var logger = LoggerFactory.getLogger("Examples");
  var productivityDate = null: Option[DataFrame]
  var operatorDate = null: Option[DataFrame]
  var liftWorkDate = null: Option[DataFrame]
  
  def getEnvironmentName(environment: String): (String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/environment.conf"))
      val conf = ConfigFactory.load(parsedFile)
      val oracleEnvName  = conf.getString("environment." + environment + ".oracleEnv")
      val cassandraEnvName  = conf.getString("environment." + environment + ".cassandraEnv")
      logger.debug("oracleEnvName - " + oracleEnvName + "\n" + "cassandraEnvName - " + cassandraEnvName)
      
      (oracleEnvName, cassandraEnvName)
    }
   
   def getCassandraConnectionProperties(environment: String): (String, String, String) = {
       val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/cassandra_properties.conf"))
       val conf = ConfigFactory.load(parsedFile)
       val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
       
       val connectionHost = conf.getString("cassandra." + cassandraEnv + ".host")
       val userName = conf.getString("cassandra." + cassandraEnv + ".username")
       val password = conf.getString("cassandra." + cassandraEnv + ".password")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
       (connectionHost, userName, password)
   }
   
  def main( args: Array[String]) = {
       val environment = if ( args(0) == null ) "dev" else args(0).toString 
       val jobId = if ( args(1) == null ) "4" else args(1).toString 
       
       val (hostName, userName, password) = getCassandraConnectionProperties(environment)
       //val sc = sparkContext(environment ,"M_FUTURE") 
       val conf = new SparkConf()
      .setAppName("M_FUTURE")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      .set("spark.cassandra.output.ignoreNulls","true")
      //.set("spark.eventLog.enabled", "true" )
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]") // This is required for IDE run
     
     val sc = new SparkContext(conf)
       val hiveContext: SQLContext = new HiveContext(sc)
       
       try{
         
         val dfJobControl =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "lift_job_control"
                                      , "keyspace" -> "wip_lift_ing") )
                         .load()
                         .filter(s"job_id = '$jobId' AND group_key = 'LIFT' ")   // 
                         
         val warehouseId = "'" + dfJobControl.select(dfJobControl("warehouse_id"))
                            .collect()
                            .map{ x => x.getString(0) }
                            .mkString("','") + "'"
         
         logger.info( " - Warehouse Ids - " + warehouseId )
         
         val dfLiftProductivity = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_productivity_data"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          .filter(s"destination IN ( $warehouseId ) ")   
                          
         val dfLiftOperator = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_operator_data"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          .filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          
         val dfLiftWorkAssignment = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_work_transactions_data"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          .filter(s"destination IN ( $warehouseId ) ") 
                          
         logger.debug( " - DataFrame Declared")
         
         dfLiftProductivity.registerTempTable("warehouse_lift_productivity_data")
         dfLiftOperator.registerTempTable("warehouse_lift_operator_data")
         dfLiftWorkAssignment.registerTempTable("warehouse_lift_work_transactions_data")
         logger.debug( " - Temp Tables Registered")         
         
         getDates(hiveContext)
         
         logger.debug( " - Program Completed")
     }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def getDates(sqlContext: SQLContext) = {
       logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                       getDates                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val currentDate = "20170418"
      try{
          val latestProductivityDateQuery = s""" SELECT destination
                                                         ,MAX( from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', wlpd.entry_time ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                     FROM warehouse_lift_productivity_data wlpd
                                                    WHERE wlpd.entry_date = '$currentDate'
                                                 GROUP BY destination
                                                    UNION
                                                    SELECT destination
                                                          ,MAX( from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', wlpd.entry_time ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                     FROM warehouse_lift_productivity_data wlpd
                                                 GROUP BY destination
                                                 ORDER BY destination, max_entry_datetime DESC
                                          """   

          val latestOperatorDateQuery = s""" SELECT dest_id
                                                     ,MAX( ext_timestamp ) max_ext_timestamp
                                                FROM warehouse_lift_operator_data
                                               WHERE run_date = '$currentDate'
                                            GROUP BY dest_id
                                               UNION
                                              SELECT dest_id
                                                    ,MAX( ext_timestamp ) max_ext_timestamp
                                                FROM warehouse_lift_operator_data
                                            GROUP BY dest_id
                                            ORDER BY dest_id, max_ext_timestamp DESC
                                          """  

            val latestWorkAssignmentsQuery = s"""SELECT destination
                                                       ,MAX( from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', substr( wlwd.entry_time, 1, 6) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                   FROM warehouse_lift_work_transactions_data wlwd
                                                  WHERE wlwd.entry_date = '$currentDate'
                                               GROUP BY destination
                                                  UNION
                                                   SELECT destination
                                                        ,MAX( from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', substr( wlwd.entry_time, 1, 6) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                   FROM warehouse_lift_work_transactions_data wlwd
                                               GROUP BY destination
                                             ORDER BY destination, max_entry_datetime DESC
                                          """    
                                                  
           logger.debug( " - latestProductivityDateQuery - " + latestProductivityDateQuery )
           val latestProductivityDateFT = Future( sqlContext.sql(latestProductivityDateQuery) )
           //latestProductivityDate.show()
           
           logger.debug( " - latestOperatorDateQuery - " + latestOperatorDateQuery )
           val latestOperatorDateFT = Future( sqlContext.sql(latestOperatorDateQuery) )
           //latestOperatorDate.show()
           
           logger.debug( " - latestWorkAssignmentsQuery - " + latestWorkAssignmentsQuery )
           val latestWorkAssignmentsFT = Future( sqlContext.sql(latestWorkAssignmentsQuery) )
           //latestWorkAssignments.show()   
           
           //val productivityDate = for { productivityDate <- latestProductivityDateFT } yield productivityDate
           
           latestProductivityDateFT.onComplete { case Success(result) => logger.debug( s" Result for Productivity Dates - $result "  )
                                                                         setProductivityDataResult(result)
                                               case Failure(t) => logger.debug("Failed to get Productivity Dates - " + t.getMessage) 
                                              }
          
           latestOperatorDateFT.onComplete { case Success(result) => logger.debug( s" Result for Operator Dates - $result " )
                                                                     setOperatorDataResult(result)
                                               case Failure(t) => logger.debug("Failed to get Operator Dates - " + t.getMessage) 
                                              }
           
           latestWorkAssignmentsFT.onComplete { case Success(result) => logger.debug( s" Result for Work Assignments Dates - $result " )
                                                                        setLiftWorkDataResult(result)
                                               case Failure(t) => logger.debug("Failed to get Work Assignments Dates - " + t.getMessage) 
                                              }  
           
           
           logger.debug( " Productivity Dates " )
           productivityDate.get.show()
           logger.debug( " Operator Dates " )
           operatorDate.get.show()
           logger.debug( " Lift Work Dates " )
           liftWorkDate.get.show()
           /*val latestProductivityDate = Await.result(latestProductivityDateFT, 3 minutes)
           val latestOperatorDate = Await.result(latestOperatorDateFT, 3 minutes)
           val latestWorkAssignments = Await.result(latestWorkAssignmentsFT, 3 minutes)
           
           latestProductivityDate.show()
           latestOperatorDate.show()
           latestWorkAssignments.show() */
      }
       catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getDates at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def setProductivityDataResult(dataFrame: DataFrame) = {
      productivityDate = Some(dataFrame)
  }
  
  def setOperatorDataResult(dataFrame: DataFrame) = {
      operatorDate = Some(dataFrame)
  }
  
  def setLiftWorkDataResult(dataFrame: DataFrame) = {
      liftWorkDate = Some(dataFrame)
  }  
  
}