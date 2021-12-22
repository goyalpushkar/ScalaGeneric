package com.cswg.testing

import org.neo4j.jdbc
import org.neo4j.graphdb._
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Values.parameters
import org.apache.spark.{SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import org.apache.spark.util.ShutdownHookManager

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, DataFrame, Row }
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

//import org.apache.hadoop.conf._

import java.text.SimpleDateFormat
import java.util.Calendar
import java.sql.Timestamp
import java.io.File
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ WrappedArray, ListBuffer }
import scala.concurrent.Future
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.FutureTaskRunner
import scala.concurrent.forkjoin._
import scala.concurrent.ExecutionContext.Implicits.global

import com.typesafe.config.ConfigFactory

class neo4jData {
  
}

object neo4jData {
 
  val DB_PATH = "C:\\Program Files\\Neo4j CE 3.1.3"
  def main(args: Array[String]) {    
     logger.debug("Main for neo4jData")
     val environment = if ( args(1) == null ) "dev" else args(1).toString 
     logger.info( " - environment - " + environment )
     
     val (hostName, userName, password) = getCassandraConnectionProperties(environment)
     logger.info( " Host Details got" )
     
     val conf = new SparkConf()
      .setAppName("WIPNeao4J")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      //.set("spark.cassandra.output.ignoreNulls","true")
      //.set("spark.eventLog.enabled", "true" )      
      //.set("spark.cassandra.connection.host","enjrhdbcasra01adev.cswg.com")      
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]") // This is required for IDE run
     
     val sc = new SparkContext(conf)
     //val (sc, conf) = sparkContext(environment, "WIPShippingTesting")
     val sqlContext: SQLContext = new HiveContext(sc)
     val currentTime = getCurrentTime()
     val currentDate = getCurrentDate()
     val previousDate = addDaystoCurrentDatePv(-7) 
     val jobId = if ( args(0) == null ) 3 else args(0).toString() 

     logger.info( " - jobId - " + jobId + " - previousDate - " + previousDate + " - currentDate - " + currentDate 
                 + " - currentTime - " + currentTime + "\n"
                 ) 
                 
      
     try{
        val dfJobControl =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "shipping_job_control"
                                      , "keyspace" -> "wip_shipping_ing") )
                         .load()
                         .filter(s"job_id = '$jobId' ")   //  AND group_key = 'CUSTOMER_HOURLY' 
                         
         val warehouseId = "'" + dfJobControl.select(dfJobControl("warehouse_id"))
                            .collect()
                            .map{ x => x.getString(0) }
                            .mkString("','") + "'"
         logger.debug( " - Warehouse Ids - " + warehouseId )
         
         val dfLoadDetails =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "load_details_by_warehouse"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load()
                         .filter(s"warehouse_id IN ( $warehouseId )  ")
                         
         val dfStopDetails =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "stop_details_by_warehouse"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load()
                         .filter(s"warehouse_id IN ( $warehouseId ) ")
         
         val dfAssignmentDetails = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "assignment_details_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")
         
        val dfAssignmentDetailsPrev = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "assignment_details_by_warehouse_prev"
                                        ,"keyspace" -> "wip_shipping_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")
                      
         val dfAssgnDetailsWarehouseHist =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "assignment_details_by_warehouse_history"
                                      , "keyspace" -> "wip_shipping_hist") )
                         .load() 
                         .filter(s"warehouse_id IN ( $warehouseId )  ") 
                         
          val dfAssignmentsMerged = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "assignments_merged"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load() 
                          .filter(s"invoice_site IN ( $warehouseId ) ")
                          
         val dfLoadDetailsCustomer = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "load_details_by_customer"
                                        ,"keyspace" -> "wip_shipping_prs" ) )
                          .load()   
                          .filter(s"warehouse_id IN ( $warehouseId ) ")
                          
         val dfWarehouseShipping =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "warehouse_shipping_data"
                                      , "keyspace" -> "wip_shipping_ing") )
                         .load().filter("com_code NOT IN ('SHU' )")
                         .filter(s"whse_nbr IN ( $warehouseId ) ")
         
         val dfWarehouseShippingHist =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "warehouse_shipping_data_history"
                                      , "keyspace" -> "wip_shipping_hist") )
                         .load()
                         //.filter("com_code NOT IN ('SHU' )")
                         .filter(s"whse_nbr IN ( $warehouseId ) AND ext_timestamp >= '$previousDate'  ")
                         
         val dfPartySites = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "ar_party_sites_t"
                                        ,"keyspace" -> "ebs_master_data" ) )
                          .load()
         
       val dfAbaCapacity = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "aba_capacity_by_opsday"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load()
        
        val dfWarehouseOperationTime = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_operation_time"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load() 
                          .filter(s"facility IN ( $warehouseId ) ")                          
                          
         val dfEmployeeImages = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_images_by_id"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
         
         val dfEmployeeDetailByWarehouse = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_details_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()

         val dfFileStreamin = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "file_streaming_tracker"
                                        ,"keyspace" -> "wip_configurations" ) )
                          .load()     

         val dfFileStreaminTracker = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "file_streaming_tracker_status"
                                        ,"keyspace" -> "wip_configurations" ) )
                          .load()    
                          
           val dfFileIngestionController =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "file_ingestion_controller"
                                      , "keyspace" -> "wip_configurations") )
                         .load()
                         .filter(s"file_id = 1 AND job_id = 1") 
                         
          val dfYMSByWarehouse = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "yms_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load() 
                          
          val dfLoadLoaders = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "load_loaders_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load() 
                          
          val dfLoadPercent = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "load_percent_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load()                 
                          
          val dfEmployeePunches = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_punches_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()   
                          
          val dfTrailerNeed = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "trailer_need_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_prs" ) )
                          .load() 
                          
          val dfTrailerAvailability = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "trailer_availability_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_prs" ) )
                          .load()                           
                          
         logger.debug(" - DataFrame Declared")
         
         dfWarehouseShipping.registerTempTable("warehouse_shipping_data")
         dfWarehouseShippingHist.registerTempTable("warehouse_shipping_data_history")
         dfPartySites.registerTempTable("ar_party_sites_t")
         dfEmployeeImages.registerTempTable("employee_images_by_id")   
         dfLoadDetails.registerTempTable("load_details_by_warehouse")                 
         dfStopDetails.registerTempTable("stop_details_by_warehouse")               
         dfAssignmentDetails.registerTempTable("assignment_details_by_warehouse")  
         dfAssgnDetailsWarehouseHist.registerTempTable("assignment_details_by_warehouse_history")
         dfEmployeeDetailByWarehouse.registerTempTable("employee_details_by_warehouse")
         dfYMSByWarehouse.registerTempTable("yms_by_warehouse")
         dfFileStreamin.registerTempTable("file_streaming_tracker")
         dfFileStreaminTracker.registerTempTable("file_streaming_tracker_status")
         dfFileIngestionController.registerTempTable("file_ingestion_controller")
         dfWarehouseOperationTime.registerTempTable("warehouse_operation_time")
         dfAbaCapacity.registerTempTable("aba_capacity_by_opsday")         
         dfLoadDetailsCustomer.registerTempTable("load_details_by_customer")
         dfAssignmentDetailsPrev.registerTempTable("assignment_details_by_warehouse_prev")
         dfAssignmentsMerged.registerTempTable("assignments_merged")
         dfLoadLoaders.registerTempTable("load_loaders_by_warehouse")
         dfLoadPercent.registerTempTable("load_percent_by_warehouse")
         dfEmployeePunches.registerTempTable("employee_punches_by_warehouse")
         dfTrailerNeed.registerTempTable("trailer_need_by_warehouse")
         dfTrailerAvailability.registerTempTable("trailer_availability_by_warehouse")
         logger.debug(" - Temp Tables Registered")
         
         
         logger.debug(" - Process Completed")
         
         
     }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }    
  
  def createDB() = {
      //val connection = DriverManager.getConnection();
      //val graphDB = new  GraphDatabase.driver(DB_PATH, AuthTokens.basic("neo4j","graphs@21")) 
      //graphDB.s
  }
}