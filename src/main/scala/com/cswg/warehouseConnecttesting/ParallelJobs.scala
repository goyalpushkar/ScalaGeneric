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
import java.sql.Timestamp
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray
import scala.concurrent.Future
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.FutureTaskRunner
import scala.concurrent.forkjoin._
import scala.concurrent.ExecutionContext.Implicits.global

import com.typesafe.config.ConfigFactory

class ParallelJobs {
  
}

object ParallelJobs {
  var logger = LoggerFactory.getLogger("Examples");
  var fileId = 1
  var extDateTime = "" //getCurrentTime()
  
  def getCurrentTime() : java.sql.Timestamp = {
     val now = Calendar.getInstance()
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)

  }
  
  def getCurrentDate(): String = {
    val now = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")     
    return sdf.format(now.getTime)
  }
  
   def getCurrentDate(date: String): java.sql.Timestamp = {
     val sdf = new SimpleDateFormat("yyyy-MM-dd");
     return new java.sql.Timestamp(sdf.parse(date).getTime())
  }
   
  def getEnvironmentName(): (String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/environment.conf"))
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
     logger.debug("Main for Parallel Jobs")
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
     val sqlContext: SQLContext = new SQLContext(sc)
     val hiveContext: SQLContext = new HiveContext(sc)
     val currentTime = getCurrentTime()
     val currentDate = getCurrentDate()
     //val previousTime = getPreviousTime(-4)
     val warehouseTesting = "44"
     fileId = if ( args(0) == null ) 1 else args(0).toInt 
     val jobId = if ( args(1) == null ) 3 else args(1).toString()  //"3" "1"
     extDateTime = if ( args(2) == null ) currentTime.toString() else  args(2).toString()
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     //extDateTime = new java.sql.Timestamp( sdf.parse( extDateTimeS ).getTime )
     
     import sqlContext.implicits._
     
     logger.debug( " - Conf Declared")      
     
     try{
         logger.debug( " - Current Date - " + currentDate + " - Current Time - " + currentTime  + "\n"
                     + " - file Id - " + fileId + " - Job Id - " + jobId  + "\n"
                     + " - Ext Time - " +  extDateTime
                   //  + " - Ext Time S - " + extDateTimeS
                    );
         
         val dfJobControl =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "warehouse_job_control"
                                      , "keyspace" -> "wip_shipping_ing") )
                         .load()
                         .filter(s"job_id = '$jobId' AND group_key = 'CUSTOMER_HOURLY' ")   // 
                         
         val warehouseId = "'" + dfJobControl.select(dfJobControl("warehouse_id"))
                            .collect()
                            .map{ x => x.getString(0) }
                            .mkString("','") + "'"
         
         logger.info( " - Warehouse Ids - " + warehouseId )
         
         val dfFileStreaming =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "file_streaming_tracker"
                                      , "keyspace" -> "wip_shipping_ing") )
                         .load()
                         .filter(s"file_id = " + fileId + s" AND ( houry_cases_status = 'Waiting' OR houry_cases_status IS NULL )  AND load_status = 'Load_complete' ")  //AND snapshot_id =  AND ( creation_time BETWEEN '$currentDate' AND '$currentTime' )
                         // " AND log_time = " + currentTime  AND error IS NULL 
          
         //dfFileStreaming.registerTempTable("file_streaming_tracker") 
         
         /*val fileStreamingQuery = s""" SELECT file_name 
                                         FROM file_streaming_tracker 
                                        WHERE creation_time BETWEEN '$currentDate' AND '$currentTime' """
         logger.debug(" - fileStreamingQuery - " + fileStreamingQuery )
         val fileStreaming = sqlContext.sql(fileStreamingQuery)
         fileStreaming.show()*/
         val fileNames =  dfFileStreaming.select(dfFileStreaming("file_name"))
                         .collect()
                         .map { x => x.getString(0) }
                            
         logger.debug( " - File Names - " )
         fileNames.foreach { println }
         
         val dfWarehouseOperationTime = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_operation_time"
                                        ,"keyspace" -> "wip_shipping_ebs" ) )
                          .load()
                          .filter(s"facility IN ( $warehouseId ) ")  //AND '$currentTime' BETWEEN start_time AND end_time
                          //This check is removed from here as we have to calculate hourly update for Previous ops Date too
         
         val dfABACapacity = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "aba_capacity_by_opsday"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")
                                   
         val dfLoadDetailsCustomer =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "load_details_by_customer"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load() 
                         .filter(s"warehouse_id IN ( $warehouseId ) ")
                         
          val dfLoadDetailsWarehouse =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "load_details_by_warehouse"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load() 
                         .filter(s"warehouse_id IN ( $warehouseId ) ")
                         
         val dfAssgnDetailsWarehouse =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "assignment_details_by_warehouse"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load() 
                         .filter(s"warehouse_id IN ( $warehouseId ) ")
         
         val dfAssgnDetailsWarehousePrev =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "assignment_details_by_warehouse_prev"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load() 
                         .filter(s"warehouse_id IN ( $warehouseId ) ")
         
         logger.debug( " - DataFrame Declared")
         
         //dfWarehouseShipping.registerTempTable("warehouse_shipping_data")
         //dfLoadDetailsEntDt.registerTempTable("load_details_by_entry_date")
         //dfFileStreamingTracker.registerTempTable("file_streaming_tracker")
         dfABACapacity.registerTempTable("aba_capacity_by_opsday")
         dfWarehouseOperationTime.registerTempTable("warehouse_operation_time")
         dfLoadDetailsCustomer.registerTempTable("load_details_by_customer")
         dfLoadDetailsWarehouse.registerTempTable("load_details_by_warehouse")
         dfAssgnDetailsWarehouse.registerTempTable("assignment_details_by_warehouse")
         dfAssgnDetailsWarehousePrev.registerTempTable("assignment_details_by_warehouse_prev")
         logger.debug( " - Temp Tables Registered")
         
         checkHourChange(hiveContext)
         
         val status = hourlyCalculations(hiveContext, sc, dfAssgnDetailsWarehouse, dfLoadDetailsWarehouse, dfAssgnDetailsWarehousePrev)
         logger.info( " - hourlyCalculations status - " + status )
         //updateFileIngestion(hiveContext, sc, fileNames, if ( status == "1" ) "Complete" else "error" )
        
         logger.debug( " - Program Completed!")
     }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
     
   }
   
  def checkHourChange(sqlContext: SQLContext){
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   checkHourChange                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val awaitDuration = scala.concurrent.duration.Duration(300000, java.util.concurrent.TimeUnit.MILLISECONDS)
      val warehouseTesting = "29"
      try{
             
            val maxExtTimePrevQuery = s"""WITH prev_max AS (
                                                   SELECT DISTINCT warehouse_id
                                                         ,ops_date ops_date
                                                         ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id ) whs_max_ext_timestamp
                                                         ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id, ops_date ) max_ext_timestamp
                                                         ,hour( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_hour
                                                         ,minute( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_minute
                                                         ,'PD' date_type
                                                     FROM assignment_details_by_warehouse_prev adwp
                                                    WHERE adwp.ext_timestamp < '$extDateTime'                                                   
                                                             )
                                     SELECT pmm.warehouse_id
                                           ,pmm.ops_date
                                           ,pmm.max_ext_timestamp
                                           ,wot.oracle_start_time
                                           ,( CASE WHEN ( pmm.ext_timestart_minute > minute(wot.oracle_start_time) ) THEN 
                                                      pmm.ext_timestart_hour
                                                 ELSE ( pmm.ext_timestart_hour - 1 )
                                             END ) ext_hour    
                                           ,from_unixtime( unix_timestamp( concat( ( CASE WHEN ( pmm.ext_timestart_minute > minute(oracle_start_time) ) THEN 
                                                                              pmm.ext_timestart_hour
                                                                         ELSE ( pmm.ext_timestart_hour - 1 )
                                                                     END )
                                                                   ,minute(oracle_start_time) )
                                                          , 'HHmm' ), "HH:mm" ) ext_hour_bucket
                                           ,pmm.date_type
                                       FROM warehouse_operation_time wot
                                           ,prev_max pmm
                                      WHERE wot.facility = pmm.warehouse_id
                                        AND wot.ops_date = pmm.ops_date
                                        AND pmm.whs_max_ext_timestamp = pmm.max_ext_timestamp
                                      """
           logger.info(" - maxExtTimePrevQuery - " + maxExtTimePrevQuery)
            val maxExtTimePrev = sqlContext.sql(maxExtTimePrevQuery )    
           
           logger.debug("Prev Assignments Max Ops Date")
           maxExtTimePrev.show()
           
           val warehouseOperationQuery = s"""SELECT facility warehouse_id
                                           ,ops_date
                                           ,'$extDateTime' max_ext_timestamp
                                           ,wot.oracle_start_time
                                           ,(CASE WHEN minute('$extDateTime') > minute(oracle_start_time) THEN 
                                                      hour( '$extDateTime' )
                                                 ELSE ( hour( '$extDateTime' ) - 1 )
                                             END ) ext_hour
                                           ,from_unixtime(  unix_timestamp( concat( (CASE WHEN minute('$extDateTime') > minute(oracle_start_time) THEN 
                                                                              hour( '$extDateTime' )
                                                                         ELSE ( hour( '$extDateTime' ) - 1 ) 
                                                                     END)
                                                                  , minute(oracle_start_time) )
                                                          , 'HHmm' ), "HH:mm") ext_hour_bucket
                                           ,'CD' date_type
                                       FROM warehouse_operation_time wot
                                      WHERE '$extDateTime' BETWEEN start_time AND end_time 
                                 """
           logger.info(" - warehouseOperationQuery - " + warehouseOperationQuery)
           //val hourChangeCurrFT = Future( sqlContext.sql( warehouseOperationQuery ) ) 
           val hourChangeCurr =  sqlContext.sql( warehouseOperationQuery )  
           
           logger.debug("Hour Change Curr Data")
           hourChangeCurr.show()
           
           /*val hourChangePrevFT =  hourChangeCurrFT
                                  .map { x => x.join(maxExtTimePrev, ( x("warehouse_id") ===  maxExtTimePrev("warehouse_id") ) , "left")
                                  .select( maxExtTimePrev("warehouse_id"), maxExtTimePrev("ops_date"), maxExtTimePrev("max_ext_timestamp"), maxExtTimePrev("ext_hour_bucket"), x("ext_hour_bucket"), maxExtTimePrev("date_type"), maxExtTimePrev("oracle_start_time"), maxExtTimePrev("ext_hour"), x("ops_date") )
                                  .withColumn("date_type_new", ( when ( x("ops_date") === maxExtTimePrev("ops_date"), lit("PH") ).otherwise(maxExtTimePrev("date_type")) ) )
                                  .filter( ( x("ext_hour_bucket") !== maxExtTimePrev("ext_hour_bucket") ) )
                                  .select( maxExtTimePrev("warehouse_id"), maxExtTimePrev("ops_date"), maxExtTimePrev("max_ext_timestamp"), maxExtTimePrev("oracle_start_time"), maxExtTimePrev("ext_hour"), maxExtTimePrev("ext_hour_bucket"), col("date_type_new").alias("date_type")  )  
                                     }*/
             
           val hourChangePrev =  hourChangeCurr
           .join(maxExtTimePrev, ( hourChangeCurr("warehouse_id") ===  maxExtTimePrev("warehouse_id") ) , "left")
           .select( maxExtTimePrev("warehouse_id"), maxExtTimePrev("ops_date"), maxExtTimePrev("max_ext_timestamp"), maxExtTimePrev("ext_hour_bucket"), hourChangeCurr("ext_hour_bucket"), maxExtTimePrev("date_type"), maxExtTimePrev("oracle_start_time"), maxExtTimePrev("ext_hour"), hourChangeCurr("ops_date") )
           .withColumn("date_type_new", ( when ( hourChangeCurr("ops_date") === maxExtTimePrev("ops_date"), lit("PH") ).otherwise(maxExtTimePrev("date_type")) ) )
           .filter( ( hourChangeCurr("ext_hour_bucket") !== maxExtTimePrev("ext_hour_bucket") ) )
           .select( maxExtTimePrev("warehouse_id"), maxExtTimePrev("ops_date"), maxExtTimePrev("max_ext_timestamp"), maxExtTimePrev("oracle_start_time"), maxExtTimePrev("ext_hour"), maxExtTimePrev("ext_hour_bucket"), col("date_type_new").alias("date_type")  )
           
           logger.debug("Hour Change Prev Data")
           hourChangePrev.show()
           
           //val hourChangeFT = hourChangeCurrFT.flatMap { x => hourChangePrevFT.map { y => x.unionAll(y) } }
           //val hourChange =  Await.result(hourChangeFT, awaitDuration )  
          
           val hourChange = hourChangeCurr.unionAll(hourChangePrev)
           logger.debug("Hour Change Data")
           hourChange.show()
           
           hourChange
           .filter( s" warehouse_id = '$warehouseTesting' " )
           .registerTempTable("warehouse_ops_days")
      }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkHourChange at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def hourlyCalculations(sqlContext: SQLContext, sc: SparkContext
                        ,dfAssgnDetailsWarehouse: DataFrame
                        ,dfLoadDetailsWarehouse: DataFrame
                        ,dfAssgnDetailsWarehousePrev: DataFrame ): String = {
    logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   hourlyCalculations               " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
    
    val currentTime = getCurrentTime()
    //val previousTime = getPreviousTime(-4)
    val warehouseTesting = "29"
    val allCustomer = "ALL"
    val awaitDuration = scala.concurrent.duration.Duration(360000, java.util.concurrent.TimeUnit.MILLISECONDS) 
    try{
         val timeSequenceQuery = s"""SELECT facility
                                           ,facility_description
                                           ,ops_date
                                           ,oracle_start_time
                                           ,wot.start_time
                                           ,wot.end_time
                                       FROM warehouse_operation_time wot
                                      WHERE '$extDateTime' BETWEEN start_time AND end_time 
                                 """
         /*
          * WHERE facility = '$warehouseTesting' 
                                       AND '$currentTime' BETWEEN start_time AND end_time
          */
         logger.info(" - timeSequenceQuery - " + timeSequenceQuery )
         val timeSequence = sqlContext.sql(timeSequenceQuery)
         timeSequence.show() 
         
         val warehouseOpsDayQuery = """ SELECT warehouse_id
                                              ,ops_date
                                              ,max_ext_timestamp
                                              ,date_type
                                              ,ext_hour_bucket
                                              ,oracle_start_time
                                              ,ext_hour
                                          FROM warehouse_ops_days
                                    """
         logger.info(" - warehouseOpsDayQuery - " + warehouseOpsDayQuery )
         val warehouseOpsDay = sqlContext.sql(warehouseOpsDayQuery)
         warehouseOpsDay.show() 
         
         //cast( SUBSTR( wot.oracle_start_time, 1, INSTR( wot.oracle_start_time, ':' ) - 1 ) as int)
         /* Considering scenarios where start time is 05:30 or 5:45 etc., start hour calculation is changed */
         //cast( SUBSTR( wot.oracle_start_time, 1, INSTR( wot.oracle_start_time, ':' ) - 1 ) as int )
         val abaCapacityQuery = s""" WITH aba_capacity AS 
                                          ( SELECT acb.warehouse_id as whse_nbr
                                                   ,acb.ops_date
                                                   ,acb.interval_hour
                                                   ,acb.aba_capacity
                                                   ,( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                hour( wot.oracle_start_time ) + 1
                                                           ELSE hour( wot.oracle_start_time )  
                                                       END ) start_hour  
                                              FROM aba_capacity_by_opsday acb
                                                  ,warehouse_ops_days wot
                                             WHERE acb.warehouse_id = wot.warehouse_id
                                               AND acb.ops_date = wot.ops_date  
                                            )
                                      SELECT whse_nbr
                                            ,ops_date
                                            ,interval_hour
                                            ,aba_capacity
                                            ,start_hour
                                            ,CASE WHEN start_hour = interval_hour THEN 
                                                       -1
                                                  WHEN ( start_hour > interval_hour 
                                                      OR interval_hour == 0 ) THEN
                                                      24 + interval_hour
                                                  ELSE interval_hour
                                              END orderby_col
                                       FROM aba_capacity
                                  ORDER BY orderby_col
                                """
          /*val abaCapacityQuery = s""" SELECT acb.warehouse_id as whse_nbr
                                           ,acb.ops_date
                                           ,acb.interval_hour
                                           ,acb.aba_capacity
                                           ,cast( SUBSTR( wot.oracle_start_time, 1, INSTR( wot.oracle_start_time, ':' ) - 1 ) as int) start_hour
                                           ,CASE WHEN cast( SUBSTR( wot.oracle_start_time, 1, INSTR( wot.oracle_start_time, ':' ) - 1 ) as int ) = acb.interval_hour THEN 
                                                        -1
                                                 WHEN ( cast( SUBSTR( wot.oracle_start_time, 1, INSTR( wot.oracle_start_time, ':' ) - 1 ) as int ) > acb.interval_hour 
                                                      OR acb.interval_hour == 0 )
                                                      THEN
                                                      24 + acb.interval_hour
                                                 ELSE acb.interval_hour
                                              END orderby_col
                                      FROM aba_capacity_by_opsday acb
                                          ,warehouse_operation_time wot
                                     WHERE acb.warehouse_id = wot.facility
                                       AND acb.ops_date = wot.ops_date                                       
                                  ORDER BY orderby_col
                                """*/
         //AND acb.warehouse_id = '$warehouseTesting'
         logger.info(" - abaCapacityQuery - " + abaCapacityQuery )
         val abaCapacity = sqlContext.sql(abaCapacityQuery) 
         abaCapacity.show()
         
         //logger.debug(" - AbaCapacity Schema - " + abaCapacity.schema )
         
         val abaCapacityGrouped = 
           abaCapacity
         .map( x => Row( x.getString(0), x.getTimestamp(1), x.getInt(2), x.getDouble(3), x.getInt(4), x.getInt(5) ) )
         .groupBy(x => ( x.getString(0), x.getTimestamp(1) ) )
          /*abaCapacity.map { x => Row( x("whse_nbr"), x("ops_date"), x("interval_hour"), x("aba_capacity"), x("start_hour"), x("orderby_col") )
                 }.*/
         
         val ABACapacityStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("time_sequence", ArrayType(StringType, true), nullable=false),
                                                        StructField("aba_capacity_list", ArrayType(StringType, true), nullable=true)
                                                       )
                                               )
         
         val ABACapacityGroupedRow = Future{ getMappedABACapacity(abaCapacityGrouped) } 
         //logger.debug(" - ABACapacityGroupedRow - ")
         //ABACapacityGroupedRow.foreach { println }
         
         //val ABACapacityFinal = sqlContext.createDataFrame(ABACapacityGroupedRow, ABACapacityStruct )
         val ABACapacityFinalFT = ABACapacityGroupedRow map
         { ABACapacity => sqlContext.createDataFrame(ABACapacity, ABACapacityStruct) }
         //ABACapacityFinal.cache()
         
         val loadDetailsQuery = s""" SELECT ldbc.warehouse_id
                                          ,ldbc.ops_date
                                          ,customer_number
                                          ,customer_name
                                          ,time_sequence                                          
                                          ,cases_selected_list
                                          ,cases_remaining_list
                                          ,scratch_quantity_list
                                          ,loads_closed_list
                                          ,loads_remaining_list
                                          ,total_scratches_list
                                          ,aba_capacity_list
                                          ,hourly_bills_list
                                      FROM load_details_by_customer ldbc
                                          ,warehouse_ops_days wot
                                     WHERE ldbc.warehouse_id = wot.warehouse_id
                                       AND ldbc.ops_date = wot.ops_date
                                """
         //AND ldbc.warehouse_id = '$warehouseTesting'
         logger.info(" - loadDetailsQuery - " + loadDetailsQuery )
         val loadDetails = sqlContext.sql(loadDetailsQuery)
         loadDetails.show()
         
          //cast( SUBSTR( wot.oracle_start_time, 1, INSTR( wot.oracle_start_time, ':' ) - 1 ) as int )
         //hour('$extDateTime') 
         //'$extDateTime'
          val qtyQuery = s""" SELECT DISTINCT wot.warehouse_id 
                                    ,wot.ops_date ops_date
                                    ,wot.oracle_start_time
                                    ,adbw.customer_number
                                    ,adbw.customer_name
                                    ,( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                 hour( wot.oracle_start_time ) + 1
                                            ELSE hour( wot.oracle_start_time )  
                                        END )  whse_start_time
                                    ,wot.ext_hour data_for_hour
                                    ,wot.max_ext_timestamp ext_datetime_fromunix
                                    ,CASE WHEN ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) = wot.ext_hour THEN 
                                                        -1
                                                 WHEN ( ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) > wot.ext_hour 
                                                      OR wot.ext_hour == 0 ) THEN
                                                      24 + wot.ext_hour
                                                 ELSE wot.ext_hour
                                              END orderby_col
                                    ,NVL( SUM( adbw.cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                         ), 0 ) sel_qty
                                    ,NVL( SUM( adbw.bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                         ), 0 ) bill_qty
                                    ,NVL( SUM( adbw.scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                         ), 0 ) scr_qty
                                    ,( SELECT NVL( SUM( adbw_remain.bill_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                      ,adbw_remain.customer_number
                                                                      ,adbw_remain.ops_date
                                                         ) 
                                                - SUM( adbw_remain.cases_selected ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                  ,adbw_remain.customer_number
                                                                                  ,adbw_remain.ops_date
                                                                     )
                                                - SUM( adbw_remain.scr_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                  ,adbw_remain.customer_number
                                                                                  ,adbw_remain.ops_date
                                                                     )
            
                                                , 0 )  
                                          FROM load_details_by_warehouse ldbw_remain
                                              ,assignment_details_by_warehouse adbw_remain
                                         WHERE ldbw_remain.warehouse_id = adbw_remain.warehouse_id 
                                           AND ldbw_remain.ops_date = adbw_remain.ops_date
                                           AND ldbw_remain.load_number = adbw_remain.load_number
                                           AND adbw_remain.customer_number IS NOT NULL
                                           AND ldbw_remain.warehouse_id = ldbw.warehouse_id 
                                           AND ldbw_remain.ops_date = ldbw.ops_date
                                           AND ldbw_remain.load_number = ldbw.load_number
                                           AND ldbw_remain.is_load_deferred = 'N'
                                       ) cases_remaining
                                    ,( SELECT wot_prev.ext_hour 
                                         FROM warehouse_ops_days wot_prev
                                        WHERE wot_prev.warehouse_id = wot.warehouse_id
                                          AND wot_prev.ops_date = wot.ops_date
                                          AND wot_prev.date_type = 'PH'
                                      ) data_for_hour_prev
                                    ,NVL( SUM( adbw_prev.cases_selected ) OVER ( PARTITION BY adbw_prev.warehouse_id 
                                                                      ,adbw_prev.customer_number
                                                                      ,adbw_prev.ops_date
                                                         ), 0 ) sel_qty_prev
                                    ,NVL( SUM( adbw_prev.bill_qty ) OVER ( PARTITION BY adbw_prev.warehouse_id 
                                                                      ,adbw_prev.customer_number
                                                                      ,adbw_prev.ops_date
                                                         ), 0 ) bill_qty_prev
                                    ,NVL( SUM( adbw_prev.scr_qty ) OVER ( PARTITION BY adbw_prev.warehouse_id 
                                                                      ,adbw_prev.customer_number
                                                                      ,adbw_prev.ops_date
                                                         ), 0 ) scr_qty_prev
                                    ,SELECT( NVL( SUM( adbw_remain.bill_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                      ,adbw_remain.customer_number
                                                                      ,adbw_remain.ops_date
                                                         ) 
                                                - SUM( adbw_remain.cases_selected ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                  ,adbw_remain.customer_number
                                                                                  ,adbw_remain.ops_date
                                                                     )
                                                - SUM( adbw_remain.scr_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                  ,adbw_remain.customer_number
                                                                                  ,adbw_remain.ops_date
                                                                     )
            
                                                , 0 )  
                                          FROM load_details_by_warehouse ldbw_remain
                                              ,assignment_details_by_warehouse adbw_remain
                                         WHERE ldbw_remain.warehouse_id = adbw_remain.warehouse_id 
                                           AND ldbw_remain.ops_date = adbw_remain.ops_date
                                           AND ldbw_remain.load_number = adbw_remain.load_number
                                           AND adbw_remain.customer_number IS NOT NULL
                                           AND ldbw_remain.warehouse_id = ldbw.warehouse_id 
                                           AND ldbw_remain.ops_date = ldbw.ops_date
                                           AND ldbw_remain.load_number = ldbw.load_number
                                           AND ldbw_remain.is_load_deferred = 'N'
                                       ) cases_remaining_prev
                                FROM load_details_by_warehouse ldbw
                                    ,warehouse_ops_days wot
                                    ,assignment_details_by_warehouse adbw
                                    ,assignment_details_by_warehouse_prev adbw_prev
                               WHERE wot.warehouse_id = ldbw.warehouse_id
                                 AND wot.ops_date = ldbw.ops_date
                                 AND ldbw.warehouse_id = adbw.warehouse_id 
                                 AND ldbw.ops_date = adbw.ops_date
                                 AND ldbw.load_number = adbw.load_number
                                 AND adbw.customer_number IS NOT NULL
                                 AND wot.date_type IN ( 'CD', 'PH' )
                                 AND ldbw.warehouse_id = adbw_prev.warehouse_id 
                                 AND ldbw.ops_date = adbw_prev.ops_date
                                 AND ldbw.load_number = adbw_prev.load_number
                               UNION
                           SELECT DISTINCT wot.warehouse_id 
                                    ,wot.ops_date ops_date
                                    ,wot.oracle_start_time
                                    ,'$allCustomer' customer_number
                                    ,'$allCustomer' customer_name
                                    ,( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                 hour( wot.oracle_start_time ) + 1
                                            ELSE hour( wot.oracle_start_time )  
                                        END )  whse_start_time
                                    ,wot.ext_hour data_for_hour
                                    ,wot.max_ext_timestamp ext_datetime_fromunix
                                    ,CASE WHEN ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) = wot.ext_hour THEN 
                                                        -1
                                                 WHEN ( ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) > wot.ext_hour 
                                                      OR wot.ext_hour == 0 ) THEN
                                                      24 + wot.ext_hour
                                                 ELSE wot.ext_hour
                                              END orderby_col
                                    ,NVL( SUM( adbw.cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) sel_qty
                                    ,NVL( SUM( adbw.bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) bill_qty
                                    ,NVL( SUM( adbw.scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) scr_qty
                                    ,( SELECT NVL( SUM( adbw_remain.bill_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                      ,adbw_remain.ops_date
                                                         ) 
                                                  - SUM( adbw_remain.cases_selected ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                    ,adbw_remain.ops_date
                                                                       )
                                                  - SUM( adbw_remain.scr_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                    ,adbw_remain.ops_date
                                                                       )
              
                                                  , 0 ) 
                                          FROM load_details_by_warehouse ldbw_remain
                                              ,assignment_details_by_warehouse_prev adbw_remain
                                         WHERE ldbw_remain.warehouse_id = adbw_remain.warehouse_id 
                                           AND ldbw_remain.ops_date = adbw_remain.ops_date
                                           AND ldbw_remain.load_number = adbw_remain.load_number
                                           AND adbw_remain.customer_number IS NOT NULL
                                           AND ldbw_remain.warehouse_id = ldbw.warehouse_id 
                                           AND ldbw_remain.ops_date = ldbw.ops_date
                                           AND ldbw_remain.load_number = ldbw.load_number
                                           AND ldbw_remain.is_load_deferred = 'N'
                                       ) cases_remaining
                                    ,( SELECT wot_prev.ext_hour 
                                         FROM warehouse_ops_days wot_prev
                                        WHERE wot_prev.warehouse_id = wot.warehouse_id
                                          AND wot_prev.ops_date = wot.ops_date
                                          AND wot_prev.date_type = 'PH'
                                      ) data_for_hour_prev
                                    ,NVL( SUM( adbw_prev.cases_selected ) OVER ( PARTITION BY adbw_prev.warehouse_id 
                                                                      ,adbw_prev.customer_number
                                                                      ,adbw_prev.ops_date
                                                         ), 0 ) sel_qty_prev
                                    ,NVL( SUM( adbw_prev.bill_qty ) OVER ( PARTITION BY adbw_prev.warehouse_id 
                                                                      ,adbw_prev.customer_number
                                                                      ,adbw_prev.ops_date
                                                         ), 0 ) bill_qty_prev
                                    ,NVL( SUM( adbw_prev.scr_qty ) OVER ( PARTITION BY adbw_prev.warehouse_id 
                                                                      ,adbw_prev.customer_number
                                                                      ,adbw_prev.ops_date
                                                         ), 0 ) scr_qty_prev
                                    ,( SELECT NVL( SUM( adbw_remain.bill_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                      ,adbw_remain.customer_number
                                                                      ,adbw_remain.ops_date
                                                         ) 
                                                - SUM( adbw_remain.cases_selected ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                  ,adbw_remain.customer_number
                                                                                  ,adbw_remain.ops_date
                                                                     )
                                                - SUM( adbw_remain.scr_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                  ,adbw_remain.customer_number
                                                                                  ,adbw_remain.ops_date
                                                                     )
            
                                                , 0 )  
                                          FROM load_details_by_warehouse ldbw_remain
                                              ,assignment_details_by_warehouse adbw_remain
                                         WHERE ldbw_remain.warehouse_id = adbw_remain.warehouse_id 
                                           AND ldbw_remain.ops_date = adbw_remain.ops_date
                                           AND ldbw_remain.load_number = adbw_remain.load_number
                                           AND adbw_remain.customer_number IS NOT NULL
                                           AND ldbw_remain.warehouse_id = ldbw.warehouse_id 
                                           AND ldbw_remain.ops_date = ldbw.ops_date
                                           AND ldbw_remain.load_number = ldbw.load_number
                                           AND ldbw_remain.is_load_deferred = 'N'
                                       ) cases_remaining_prev
                                FROM load_details_by_warehouse ldbw
                                    ,warehouse_ops_days wot
                                    ,assignment_details_by_warehouse adbw
                                    ,assignment_details_by_warehouse_prev adbw_prev
                               WHERE wot.warehouse_id = ldbw.warehouse_id
                                 AND wot.ops_date = ldbw.ops_date
                                 AND ldbw.warehouse_id = adbw.warehouse_id 
                                 AND ldbw.ops_date = adbw.ops_date
                                 AND ldbw.load_number = adbw.load_number
                                 AND adbw.customer_number IS NOT NULL
                                 AND wot.date_type IN ( 'CD', 'PH' )
                                 AND ldbw.warehouse_id = adbw_prev.warehouse_id 
                                 AND ldbw.ops_date = adbw_prev.ops_date
                                 AND ldbw.load_number = adbw_prev.load_number
                               UNION
                              SELECT DISTINCT wot.warehouse_id 
                                    ,wot.ops_date ops_date
                                    ,wot.oracle_start_time
                                    ,adbw.customer_number
                                    ,adbw.customer_name
                                    ,( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                 hour( wot.oracle_start_time ) + 1
                                            ELSE hour( wot.oracle_start_time )  
                                        END )  whse_start_time
                                    ,wot.ext_hour data_for_hour
                                    ,wot.max_ext_timestamp ext_datetime_fromunix
                                    ,CASE WHEN ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) = wot.ext_hour THEN 
                                                        -1
                                                 WHEN ( ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) > wot.ext_hour 
                                                      OR wot.ext_hour == 0 ) THEN
                                                      24 + wot.ext_hour
                                                 ELSE wot.ext_hour
                                              END orderby_col
                                    ,NVL( SUM( cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                         ), 0 ) sel_qty
                                    ,NVL( SUM( bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                         ), 0 ) bill_qty
                                    ,NVL( SUM( scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                         ), 0 ) scr_qty
                                    ,( SELECT NVL( SUM( adbw_remain.bill_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                      ,adbw_remain.customer_number
                                                                      ,adbw_remain.ops_date
                                                         ) 
                                              - SUM( adbw_remain.cases_selected ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                ,adbw_remain.customer_number
                                                                                ,adbw_remain.ops_date
                                                                   )
                                              - SUM( adbw_remain.scr_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                ,adbw_remain.customer_number
                                                                                ,adbw_remain.ops_date
                                                                   )
          
                                              , 0 )
                                          FROM load_details_by_warehouse ldbw_remain
                                              ,assignment_details_by_warehouse_prev adbw_remain
                                         WHERE ldbw_remain.warehouse_id = adbw_remain.warehouse_id 
                                           AND ldbw_remain.ops_date = adbw_remain.ops_date
                                           AND ldbw_remain.load_number = adbw_remain.load_number
                                           AND adbw_remain.customer_number IS NOT NULL
                                           AND ldbw_remain.warehouse_id = ldbw.warehouse_id 
                                           AND ldbw_remain.ops_date = ldbw.ops_date
                                           AND ldbw_remain.load_number = ldbw.load_number
                                           AND ldbw_remain.is_load_deferred = 'N'
                                       )  cases_remaining
                                     ,null data_for_hour_prev
                                     ,0 sel_qty_prev
                                     ,0 bill_qty_prev
                                     ,0 scr_qty_prev
                                     ,0 cases_remaining_prev
                                FROM load_details_by_warehouse ldbw
                                    ,warehouse_ops_days wot
                                    ,assignment_details_by_warehouse_prev adbw
                               WHERE wot.warehouse_id = ldbw.warehouse_id
                                 AND wot.ops_date = ldbw.ops_date
                                 AND ldbw.warehouse_id = adbw.warehouse_id 
                                 AND ldbw.ops_date = adbw.ops_date
                                 AND ldbw.load_number = adbw.load_number
                                 AND adbw.customer_number IS NOT NULL
                                 AND wot.date_type = 'PD'
                               UNION
                           SELECT DISTINCT wot.warehouse_id 
                                    ,wot.ops_date ops_date
                                    ,wot.oracle_start_time
                                    ,'$allCustomer' customer_number
                                    ,'$allCustomer' customer_name
                                    ,( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                 hour( wot.oracle_start_time ) + 1
                                            ELSE hour( wot.oracle_start_time )  
                                        END )  whse_start_time
                                    ,wot.ext_hour data_for_hour
                                    ,wot.max_ext_timestamp ext_datetime_fromunix
                                    ,CASE WHEN ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) = wot.ext_hour THEN 
                                                        -1
                                                 WHEN ( ( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                                  hour( wot.oracle_start_time ) + 1
                                                              ELSE hour( wot.oracle_start_time )  
                                                          END ) > wot.ext_hour 
                                                      OR wot.ext_hour == 0 ) THEN
                                                      24 + wot.ext_hour
                                                 ELSE wot.ext_hour
                                              END orderby_col
                                    ,NVL( SUM( cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) sel_qty
                                    ,NVL( SUM( bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) bill_qty
                                    ,NVL( SUM( scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) scr_qty
                                    ,( SELECT NVL( SUM( adbw_remain.bill_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                      ,adbw_remain.ops_date
                                                                       ) 
                                                  - SUM( adbw_remain.cases_selected ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                    ,adbw_remain.ops_date
                                                                       )
                                                  - SUM( adbw_remain.scr_qty ) OVER ( PARTITION BY adbw_remain.warehouse_id 
                                                                                    ,adbw_remain.ops_date
                                                                       )
              
                                                  , 0 ) 
                                          FROM load_details_by_warehouse ldbw_remain
                                              ,assignment_details_by_warehouse_prev adbw_remain
                                         WHERE ldbw_remain.warehouse_id = adbw_remain.warehouse_id 
                                           AND ldbw_remain.ops_date = adbw_remain.ops_date
                                           AND ldbw_remain.load_number = adbw_remain.load_number
                                           AND adbw_remain.customer_number IS NOT NULL
                                           AND ldbw_remain.warehouse_id = ldbw.warehouse_id 
                                           AND ldbw_remain.ops_date = ldbw.ops_date
                                           AND ldbw_remain.load_number = ldbw.load_number
                                           AND ldbw_remain.is_load_deferred = 'N'
                                       ) cases_remaining
                                     ,null data_for_hour_prev
                                     ,0 sel_qty_prev
                                     ,0 bill_qty_prev
                                     ,0 scr_qty_prev
                                     ,0 cases_remaining_prev
                                FROM load_details_by_warehouse ldbw
                                    ,warehouse_ops_days wot
                                    ,assignment_details_by_warehouse adbw
                               WHERE wot.warehouse_id = ldbw.warehouse_id
                                 AND wot.ops_date = ldbw.ops_date
                                 AND ldbw.warehouse_id = adbw.warehouse_id 
                                 AND ldbw.ops_date = adbw.ops_date
                                 AND ldbw.load_number = adbw.load_number
                                 AND adbw.customer_number IS NOT NULL
                                 AND wot.date_type = 'PD'
                          """
          //AND ldbw.warehouse_id = '$warehouseTesting'                      
          //AND '$currentTime' BETWEEN wot.start_time AND wot.end_time
          //AND from_unixtime( unix_timestamp( concat( wsd.ext_date, ' ', wsd.ext_time ), 'yyyyMMdd HHmmss' ) )  >= '$maxLogTime'
                                 /*, ( SELECT COUNT( DISTINCT load_number ) 
                                          FROM assignment_details_by_warehouse adbw_closed
                                         WHERE adbw_closed.warehouse_id = adbw.warehouse_id
                                           AND adbw_closed.ops_date = adbw.ops_date
                                           AND adbw_closed.customer_number = adbw.customer_number
                                           AND adbw_closed.closed_timestamp IS NOT NULL
                                      ) closed_loads
                                    , ( SELECT COUNT( DISTINCT load_number ) 
                                          FROM assignment_details_by_warehouse adbw_open
                                         WHERE adbw_open.warehouse_id = adbw.warehouse_id
                                           AND adbw_open.ops_date = adbw.ops_date
                                           AND adbw_open.customer_number = adbw.customer_number
                                           AND adbw_open.closed_timestamp IS NULL
                                      ) open_loads*/
          logger.info(" - qtyQuery - " + qtyQuery )
          val qtyCountFt = Future{ sqlContext.sql(qtyQuery) }
          //qtyCount.show
          //qtyCount.cache()
              
          //.withColumn("ext_datetime", concat( dfAssgnDetailsWarehouse.col("ext_date"), lit(" "), dfAssgnDetailsWarehouse.col("ext_time") ) )
          val closedLoadsFT = Future{ dfAssgnDetailsWarehouse
          .join(warehouseOpsDay, (dfAssgnDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL ")
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - Closed Loads - " )
          //closedLoads.show()
          //closedLoads.cache()
          
          val openLoadsFT = Future{ dfAssgnDetailsWarehouse
          .join(warehouseOpsDay, (dfAssgnDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          //.join(dfLoadDetailsWarehouse, (dfAssgnDetailsWarehouse("warehouse_id") === dfLoadDetailsWarehouse("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === dfLoadDetailsWarehouse("ops_date") && dfAssgnDetailsWarehouse("load_number") === dfLoadDetailsWarehouse("load_number") ), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL ) ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads")
          }
          logger.debug(" - Open Loads - " )
          //openLoads.show()
          //openLoads.cache()
          
          val allClosedLoadsFT = Future{ dfAssgnDetailsWarehouse  //dfLoadDetailsWarehouse
          .join(warehouseOpsDay, (dfLoadDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfLoadDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL ")
          .select(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") , dfLoadDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - All Closed Loads - " )
          //allClosedLoads.show()
          //allClosedLoads.cache()
          
          val allOpenLoadsFT = Future{ dfAssgnDetailsWarehouse  //dfLoadDetailsWarehouse
          .join(warehouseOpsDay, (dfLoadDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfLoadDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          //.join(dfLoadDetailsWarehouse, (dfAssgnDetailsWarehouse("warehouse_id") === dfLoadDetailsWarehouse("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === dfLoadDetailsWarehouse("ops_date") && dfAssgnDetailsWarehouse("load_number") === dfLoadDetailsWarehouse("load_number") ), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL ) ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"),lit("ALL").alias("$customer_number") ,lit("ALL").alias("customer_name"), dfLoadDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"),lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads")
          }
          logger.debug(" - All Open Loads - " )
          
          val closedLoadsPrevFT = Future{ dfAssgnDetailsWarehousePrev
          .join(warehouseOpsDay, (dfAssgnDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL ")
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - Closed Loads Prev - " )
          //closedLoads.show()
          //closedLoads.cache()
          
          val openLoadsPrevFT = Future{ dfAssgnDetailsWarehousePrev
          .join(warehouseOpsDay, (dfAssgnDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          //.join(dfLoadDetailsWarehouse, (dfAssgnDetailsWarehouse("warehouse_id") === dfLoadDetailsWarehouse("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === dfLoadDetailsWarehouse("ops_date") && dfAssgnDetailsWarehouse("load_number") === dfLoadDetailsWarehouse("load_number") ), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL ) ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads")
          }
          logger.debug(" - Open Loads Prev - " )
          //openLoads.show()
          //openLoads.cache()
          
          val allClosedLoadsPrevFT = Future{ dfAssgnDetailsWarehousePrev  //dfLoadDetailsWarehouse
          .join(warehouseOpsDay, (dfLoadDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfLoadDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL ")
          .select(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") , dfLoadDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - All Closed Loads Prev - " )
          //allClosedLoads.show()
          //allClosedLoads.cache()
          
          val allOpenLoadsPrevFT = Future{ dfAssgnDetailsWarehousePrev  //dfLoadDetailsWarehouse
          .join(warehouseOpsDay, (dfLoadDetailsWarehouse("warehouse_id") === warehouseOpsDay("warehouse_id") && dfLoadDetailsWarehouse("ops_date")  === warehouseOpsDay("ops_date")), "inner")
          //.join(dfLoadDetailsWarehouse, (dfAssgnDetailsWarehouse("warehouse_id") === dfLoadDetailsWarehouse("warehouse_id") && dfAssgnDetailsWarehouse("ops_date")  === dfLoadDetailsWarehouse("ops_date") && dfAssgnDetailsWarehouse("load_number") === dfLoadDetailsWarehouse("load_number") ), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL ) ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"),lit("ALL").alias("$customer_number") ,lit("ALL").alias("customer_name"), dfLoadDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          //.withColumn("data_for_hour" , hour( from_unixtime( unix_timestamp( currentTime, "HHmmss" ) ) ) )
          //.withColumn("ext_datetime_fromunix" ,  currentTime )
          //.withColumn("closed_loads" , count(  dfAssgnDetailsWarehouse.col("load_nbr") ) )
          .groupBy(dfLoadDetailsWarehouse("warehouse_id"), dfLoadDetailsWarehouse("ops_date"),lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads")
          }
          logger.debug(" - All Open Loads Prev - " )          
          //allOpenLoads.show()
          //allOpenLoads.cache()
          
          /*val allOpenLoads = Await.result(allOpenLoadsFT, awaitDuration )   
          val openLoads = Await.result(openLoadsFT, awaitDuration )  
          val allClosedLoads = Await.result(allClosedLoadsFT, awaitDuration )   
          val closedLoads = Await.result(closedLoadsFT, awaitDuration )  
          val totalOpenLoads = openLoads.unionAll(allOpenLoads)
          val totalClosedLoads = closedLoads.unionAll(allClosedLoads)*/
          
          /*allOpenLoadsFT onSuccess{
            case result => 
          }*/
          //onComplete { x => x.get }

          val totalOpenLoadsFT = openLoadsFT.flatMap { x => allOpenLoadsFT.map { y => y.unionAll(x) } } 
          val totalClosedLoadsFT = closedLoadsFT.flatMap { x => allClosedLoadsFT.map { y => x.unionAll(y) } }
          //val totalClosedLoads = closedLoadsFT.map { x => x.unionAll(allClosedLoadsFT.value.get.get) }.value.get.get
          val totalOpenLoads =  Await.result(totalOpenLoadsFT, awaitDuration )  
          val totalClosedLoads =  Await.result(totalClosedLoadsFT, awaitDuration )  
          
          //val totalOpenLoads2  = totalOpenLoadsFT.flatMap { x => x. } 
          logger.debug(" - Total Closed and Open Loads - " )
          totalClosedLoads.show()
          totalOpenLoads.show()
          
          val totalOpenLoadsPrevFT = openLoadsPrevFT.flatMap { x => allOpenLoadsPrevFT.map { y => y.unionAll(x) } } 
          val totalClosedLoadsPrevFT = closedLoadsPrevFT.flatMap { x => allClosedLoadsPrevFT.map { y => x.unionAll(y) } }
          //val totalClosedLoads = closedLoadsFT.map { x => x.unionAll(allClosedLoadsFT.value.get.get) }.value.get.get
          val totalOpenLoadsPrev =  Await.result(totalOpenLoadsPrevFT, awaitDuration )  
          val totalClosedLoadsPrev =  Await.result(totalClosedLoadsPrevFT, awaitDuration )  
          
          //val totalOpenLoads2  = totalOpenLoadsFT.flatMap { x => x. } 
          logger.debug(" - Total Closed and Open Loads Prev Hr- " )
          totalClosedLoadsPrev.show()
          totalOpenLoadsPrev.show()          
          
          val completeDataFT = qtyCountFt map { qtyCount => 
            qtyCount
          .join(totalClosedLoads, Seq("warehouse_id", "ops_date", "customer_number","customer_name"), "left" )  //, "ext_date", "data_for_hour" 
          .join(totalOpenLoads, Seq("warehouse_id", "ops_date", "customer_number","customer_name" ), "left" )
          .join(totalClosedLoadsPrev, Seq("warehouse_id", "ops_date", "customer_number","customer_name"), "left" )  //, "ext_date", "data_for_hour" 
          .join(totalOpenLoadsPrev, Seq("warehouse_id", "ops_date", "customer_number","customer_name" ), "left" )          
          .join(loadDetails, Seq("warehouse_id", "ops_date", "customer_number","customer_name" ), "left" )
          //.join(ABACapacityFinal, Seq("warehouse_id", "ops_date") ,"left")
          .select(qtyCount("warehouse_id"), qtyCount("ops_date"), qtyCount("oracle_start_time"), qtyCount("customer_number"), qtyCount("customer_name")
                 ,qtyCount("orderby_col"), qtyCount("whse_start_time"), qtyCount("ext_datetime_fromunix")   //,qtyCount("ext_date"),qtyCount("ext_time")
                 ,qtyCount("data_for_hour"), qtyCount("sel_qty"), qtyCount("cases_remaining" ), qtyCount("scr_qty"), qtyCount("bill_qty")
                 ,totalClosedLoads("closed_loads"), totalOpenLoads("open_loads")
                 ,qtyCount("data_for_hour_prev"), qtyCount("sel_qty_prev"), qtyCount("cases_remaining_prev" ), qtyCount("scr_qty_prev"), qtyCount("bill_qty_prev")
                 ,totalClosedLoadsPrev("closed_loads").alias("closed_loads_prev"), totalOpenLoadsPrev("open_loads").alias("open_loads_prev")
                 ,loadDetails("time_sequence"), loadDetails("cases_selected_list"), loadDetails("cases_remaining_list"), loadDetails("scratch_quantity_list")
                 ,loadDetails("hourly_bills_list"), loadDetails("loads_closed_list"),loadDetails("loads_remaining_list"), loadDetails("total_scratches_list")
                 
                 )
          .orderBy("warehouse_id", "ops_date", "customer_number","customer_name", "orderby_col", "data_for_hour")  //customer_name
          }
          //.withColumn("load_remaining", closedLoads("closed_loads") + openLoads("open_loads"))
          logger.debug(" - Complete Data after joining - " )
          val completeData = Await.result(completeDataFT, awaitDuration )         
          completeData.show()
                    
          /*completeData.coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
          .mode("overwrite")
          .save("C:/LoadDetails/Run2/TotalQty")*/
          
          //logger.debug(" Schema - " + completeData.schema )
          
          val completeDataGrouped = completeData
          .map { x => completeLoad( keyColumns( x.getString(0), x.getTimestamp(1), x.getString(2), x.getString(3), x.getString(4)
                                               ,x.getInt(5), x.getInt(6), x.getString(7)
                                              )
                                   ,currentHour( x.getInt(8), (if ( x.get(9) == null ) 0 else x.getLong(9) ), (if ( x.get(10) == null ) 0 else x.getLong(10) ), (if ( x.get(11) == null ) 0 else x.getLong(11) ), ( if ( x.get(12) == null ) 0 else x.getLong(12) )
                                                ,( if ( x.get(13) == null )  0 else x.getLong(13) ), ( if ( x.get(14) == null )  0 else x.getLong(14) )
                                               )
                                   ,prevHour( x.getInt(15), (if ( x.get(16) == null ) 0 else x.getLong(16) ), (if ( x.get(17) == null ) 0 else x.getLong(17) ), (if ( x.get(18) == null ) 0 else x.getLong(18) ), ( if ( x.get(19) == null ) 0 else x.getLong(19) )
                                                ,( if ( x.get(20) == null )  0 else x.getLong(20) ), ( if ( x.get(21) == null )  0 else x.getLong(21) )
                                               )
                                   ,existingHourly( x.getAs[WrappedArray[String]](22), x.getAs[WrappedArray[String]](23), x.getAs[WrappedArray[String]](24), x.getAs[WrappedArray[String]](25), x.getAs[WrappedArray[String]](26)
                                                   ,x.getAs[WrappedArray[String]](27), x.getAs[WrappedArray[String]](28), x.getAs[WrappedArray[String]](29)
                                                  )            
                          ) 
              }
          .groupBy( x => ( x.keyColumnsData.warehouseId, x.keyColumnsData.opsDate, x.keyColumnsData.customerNumber ) )      
          //( x.warehouseId, x.opsDate, x.customerNumber ) )
          completeDataGrouped.foreach(println)
          
          /*val completeDataMapped = getMappedData(completeDataGrouped)
          //logger.debug( " - Final Data - " )
          //completeDataMapped.foreach { println }
          
         val CustomerDataStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("customer_number", StringType, nullable=false),
                                                        StructField("customer_name", StringType, nullable=true),
                                                        StructField("time_sequence", ArrayType(StringType, true), nullable=false),
                                                        StructField("cases_selected_list", ArrayType(StringType, true), nullable=true),
                                                        StructField("cases_remaining_list", ArrayType(StringType, true), nullable=true),
                                                        StructField("scratch_quantity_list", ArrayType(StringType, true), nullable=true),
                                                        StructField("loads_closed_list", ArrayType(StringType, true), nullable=true),
                                                        StructField("loads_remaining_list", ArrayType(StringType, true), nullable=true),
                                                        StructField("total_scratches_list", ArrayType(StringType, true), nullable=true),
                                                        StructField("update_time", TimestampType, nullable=true),
                                                        StructField("hourly_bills_list", ArrayType(StringType, true), nullable=true)
                                                       )
                                               )
          
         val CustomerDataFinal = sqlContext.createDataFrame(completeDataMapped, CustomerDataStruct )
         //logger.debug( " - Final Data after Data Structure - " )
         //CustomerDataFinal.foreach { println }
         
         val ABACapacityFinal = Await.result(ABACapacityFinalFT, awaitDuration )
         //ABACapacityFinalFT.value.get.get
                   
         val finalData = CustomerDataFinal
         .join(ABACapacityFinal, Seq("warehouse_id","ops_date"), "left") 
         //(CustomerDataFinal("warehouse_id") === ABACapacityFinal("warehouse_id") && CustomerDataFinal("ops_date")  === ABACapacityFinal("ops_date"))
         .select( CustomerDataFinal("warehouse_id"), CustomerDataFinal("ops_date"), CustomerDataFinal("customer_number"),CustomerDataFinal("customer_name")  
                 ,CustomerDataFinal("time_sequence"), CustomerDataFinal("cases_selected_list"), CustomerDataFinal("cases_remaining_list")
                 ,CustomerDataFinal("scratch_quantity_list"), CustomerDataFinal("loads_closed_list"), CustomerDataFinal("loads_remaining_list")
                 ,CustomerDataFinal("total_scratches_list"), ABACapacityFinal("aba_capacity_list"), CustomerDataFinal("update_time")
                 ,CustomerDataFinal("hourly_bills_list")
                )
          
          logger.debug(" - Final Data to Save - ")
          finalData.show()*/
          
         "1"
     }
    catch {
      case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing hourlyCalculations at : " + e.getMessage + " " +  e.printStackTrace()  )
          "0"
    }
  }
   
  case class keyColumns( warehouseId: String, opsDate: Timestamp, oracleStartTime: String, customerNumber: String, CustomerName: String
                        ,orderBy: Integer, whseStartime: Integer, extDateTime: String  
                        )
  
  case class currentHour( dataHour: Integer, selQty: Long, casesRemaining: Long, scrQty: Long, billQty: Long, closedLoads: Long, openLoads: Long )
  
  case class prevHour( dataHourPrev: Integer, selQtyPrev: Long, casesRemainingPrev: Long, scrQtyPrev: Long, billQtyPrev: Long, closedLoadsPrev: Long, openLoadsPrev: Long )
  
  case class existingHourly( warehouseOpsDay: WrappedArray[String], casesSelectedList: WrappedArray[String], casesRemainingList: WrappedArray[String]                   
                            ,scrQtyList: WrappedArray[String], loadBillsList: WrappedArray[String]
                            ,loadClosedList: WrappedArray[String], loadRemainingList: WrappedArray[String], totalScratchesList: WrappedArray[String]
                           )
  
  case class completeLoad( keyColumnsData: keyColumns, currentHourData: currentHour, prevHourData: prevHour, existingHourlyData: existingHourly )
  
  /*case class completeLoad(warehouseId: String, opsDate: Timestamp, oracleStartTime: String, customerNumber: String, CustomerName: String
                         ,dataHour: Integer, orderBy: Integer, loadBillsList: WrappedArray[String], selQty: Long, casesRemaining: Long, scrQty: Long   //, extDateTime: String
                         ,closedLoads: Long, openLoads: Long, billQty: Long, casesSelectedList: WrappedArray[String], casesRemainingList: WrappedArray[String]                   
                         ,scrQtyList: WrappedArray[String], loadClosedList: WrappedArray[String], loadRemainingList: WrappedArray[String], totalScratchesList: WrappedArray[String]
                         ,warehouseOpsDay: WrappedArray[String], whseStartime: Integer
                         )*/
                         
   def getMappedABACapacity( groupedData: RDD[ ( (String, Timestamp ), Iterable[Row] ) ] ) = {
      logger.debug("inside getMappedABACapacity")
      val ABACapacityRowMap = groupedData.map(f => Row( f._1._1
                                                       ,f._1._2
                                                       ,f._2.map { x => Map( x.getInt(5) -> (x.getInt(2), x.getDouble(3)) )
                                                                       .toSeq.sortBy(_._1)
                                                                       .flatMap( f => Seq(f._2._1.toString() ) )// Array(Row( (f._2._1), f._2._2 ) ))
                                                                 }.flatten
                                                       ,f._2.map { x => Map( x.getInt(5) -> (x.getInt(2), x.getDouble(3)) )
                                                                       .toSeq.sortBy(_._1)
                                                                       .flatMap( f =>  Seq(f._2._2.toString() ) )
                                                                 }.flatten
                                                      )
                                            )
      //logger.debug(" - ABACapacityRowMap - ")                      
      //ABACapacityRowMap.foreach { println }
      
      val ABACapacityFinal = ABACapacityRowMap.map { x => Row( x.getString(0)
                                                              ,x.getTimestamp(1)
                                                              ,x.getList(2).asScala.toList
                                                              ,x.getList(3).asScala.toList
                                                              )}      
      ABACapacityFinal
  }

    def getMappedData(groupedData: RDD[ ( (String, Timestamp, String), Iterable[completeLoad] ) ] ) = {
      logger.debug("inside getMappedData")
      val currentTime = getCurrentTime()
          
      val returnRow = groupedData.map(f => Row( f._1._1
                                                   ,f._1._2
                                                   ,f._1._3
                                                   ,f._2.map{ x => x.keyColumnsData.CustomerName }.mkString
                                                   ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield
                                                                     ( if (i>=24) (i-24).toString() else i.toString() ) 
                                                            }.flatten
                                                   ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield 
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                         if ( ( x.currentHourData.selQty - 
                                                                                x.prevHourData.selQtyPrev
                                                                               )
                                                                                /*( if (x.existingHourlyData.casesSelectedList == null) 0.toLong 
                                                                                else x.existingHourlyData.casesSelectedList.foldLeft(0)(_.toInt +_.toInt) ).toLong 
                                                                              )*/ < 0 
                                                                            ) "0" 
                                                                         else ( x.currentHourData.selQty - x.prevHourData.selQtyPrev
                                                                                 /*( if (x.existingHourlyData.casesSelectedList == null) 0.toLong
                                                                                 else x.existingHourlyData.casesSelectedList.foldLeft(0)(_.toInt +_.toInt) ).toLong*/ 
                                                                              ).toString() 
                                                                                 //  + inner(x.existingHourlyData.casesSelectedList.tail) else x.existingHourlyData.casesSelectedList.map( x=> x.trim().sum.toLong ) applyOrElse(i-x.keyColumnsData.whseStartime.toInt-1,"0").toString().toLong ) ).toString()
                                                                      } 
                                                                      else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                         if ( ( x.prevHourData.selQtyPrev - 
                                                                                ( if (x.existingHourlyData.casesSelectedList == null) 0.toLong  // if Existing List is Null then 0
                                                                                else ( x.existingHourlyData.casesSelectedList.foldLeft(0)(_.toInt +_.toInt) // else Sum of all elements from the existing list
                                                                                     - ( if ( x.existingHourlyData.casesSelectedList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                         else x.existingHourlyData.casesSelectedList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt,"0") 
                                                                                         ).toString().toInt       // but subtract element present at Prev hour as it is already summed up in foldLeft but it should not be subtracted
                                                                                     ).toLong // Sum of all elements - Element present in list for Prev hour
                                                                                 ) 
                                                                               ) < 0 ) "0"   
                                                                         else ( x.prevHourData.selQtyPrev - 
                                                                                ( if (x.existingHourlyData.casesSelectedList == null) 0.toLong 
                                                                                else ( x.existingHourlyData.casesSelectedList.foldLeft(0)(_.toInt +_.toInt) 
                                                                                     - ( if ( x.existingHourlyData.casesSelectedList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.casesSelectedList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt 
                                                                                     ).toLong 
                                                                                 ) 
                                                                               ) 
                                                                      }
                                                                     else{
                                                                          ( if ( x.existingHourlyData.casesSelectedList == null ) "0" 
                                                                          else ( if ( x.existingHourlyData.casesSelectedList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                               else x.existingHourlyData.casesSelectedList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt,"0") 
                                                                               ) 
                                                                          ).toString()
                                                                     }
                                                            }.flatten
                                                    ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield 
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                        ( x.currentHourData.casesRemaining ).toString() //Seq().:+ //- ( if (x.currentHourData.casesRemainingList == null) 0 else x.currentHourData.casesRemainingList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0").toString().toLong )
                                                                      }
                                                                      else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                        ( x.prevHourData.casesRemainingPrev ).toString() 
                                                                      }
                                                                     else{
                                                                        ( if ( x.existingHourlyData.casesRemainingList == null ) "0" else ( if ( x.existingHourlyData.casesRemainingList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" else x.existingHourlyData.casesRemainingList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0") ) ).toString()
                                                                     }
                                                             }.flatten 
                                                    ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield 
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                          if ( ( x.currentHourData.scrQty - x.prevHourData.scrQtyPrev
                                                                                 /*( if (x.existingHourlyData.scrQtyList == null) 0 
                                                                                 else x.existingHourlyData.scrQtyList.foldLeft(0)(_.toInt +_.toInt) ).toLong*/ 
                                                                                ) < 0 ) "0"
                                                                          else ( x.currentHourData.scrQty - x.prevHourData.scrQtyPrev
                                                                                /*( if (x.existingHourlyData.scrQtyList == null) 0 
                                                                                else x.existingHourlyData.scrQtyList.foldLeft(0)(_.toInt +_.toInt) ).toLong*/ 
                                                                               ).toString()  //Seq().:+ //.existingHourlyData.scrQtyList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt-1, "0") 
                                                                        }
                                                                      else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                         if ( ( x.prevHourData.scrQtyPrev - 
                                                                                ( if (x.existingHourlyData.scrQtyList == null) 0.toLong  // if Existing List is Null then 0
                                                                                else ( x.existingHourlyData.scrQtyList.foldLeft(0)(_.toInt +_.toInt) // else Sum of all elements from the existing list
                                                                                     - ( if ( x.existingHourlyData.scrQtyList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.scrQtyList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt       // but subtract element present at Prev hour as it is already summed up in foldLeft but it should not be subtracted
                                                                                     ).toLong // Sum of all elements - Element present in list for Prev hour
                                                                                 ) 
                                                                               ) < 0 ) "0"   
                                                                         else ( x.prevHourData.scrQtyPrev - 
                                                                                ( if (x.existingHourlyData.scrQtyList == null) 0.toLong 
                                                                                else ( x.existingHourlyData.scrQtyList.foldLeft(0)(_.toInt +_.toInt) 
                                                                                     - ( if ( x.existingHourlyData.scrQtyList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.scrQtyList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt 
                                                                                     ).toLong 
                                                                                 ) 
                                                                               ) 
                                                                      }
                                                                      else{
                                                                         ( if ( x.existingHourlyData.scrQtyList == null ) "0" else ( if ( x.existingHourlyData.scrQtyList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" else x.existingHourlyData.scrQtyList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0") ) ).toString()  //Seq().:+ 
                                                                       }
                                                              }.flatten 
                                                    ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                          if ( ( x.currentHourData.closedLoads - x.prevHourData.closedLoadsPrev
                                                                                /*( if (x.existingHourlyData.loadClosedList == null) 0 
                                                                                else x.existingHourlyData.loadClosedList.foldLeft(0)(_.toInt +_.toInt) ).toLong */
                                                                               ) < 0 
                                                                             ) "0" 
                                                                          else ( x.currentHourData.closedLoads - x.prevHourData.closedLoadsPrev
                                                                                /*( if (x.existingHourlyData.loadClosedList == null) 0 
                                                                                else x.existingHourlyData.loadClosedList.foldLeft(0)(_.toInt +_.toInt) ).toLong */
                                                                               ).toString() //Seq().:+  //applyOrElse(i-x.keyColumnsData.whseStartime.toInt-1, "0") 
                                                                        } 
                                                                     else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                         if ( ( x.prevHourData.closedLoadsPrev - 
                                                                                ( if (x.existingHourlyData.loadClosedList == null) 0.toLong  // if Existing List is Null then 0
                                                                                else ( x.existingHourlyData.loadClosedList.foldLeft(0)(_.toInt +_.toInt) // else Sum of all elements from the existing list
                                                                                     - ( if ( x.existingHourlyData.loadClosedList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.loadClosedList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt       // but subtract element present at Prev hour as it is already summed up in foldLeft but it should not be subtracted
                                                                                     ).toLong // Sum of all elements - Element present in list for Prev hour
                                                                                 ) 
                                                                               ) < 0 ) "0"   
                                                                         else ( x.prevHourData.closedLoadsPrev - 
                                                                                ( if (x.existingHourlyData.scrQtyList == null) 0.toLong 
                                                                                else ( x.existingHourlyData.scrQtyList.foldLeft(0)(_.toInt +_.toInt) 
                                                                                     - ( if ( x.existingHourlyData.loadClosedList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.loadClosedList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt 
                                                                                     ).toLong 
                                                                                 ) 
                                                                               ) 
                                                                      }
                                                                       else{
                                                                         ( if ( x.existingHourlyData.loadClosedList == null ) "0" else ( if ( x.existingHourlyData.loadClosedList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" else x.existingHourlyData.loadClosedList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0") ) ).toString()  //Seq().:+
                                                                       }
                                                              }.flatten 
                                                    ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                          ( x.currentHourData.openLoads ).toString() //Seq().:+  //- ( if(x.existingHourlyData.loadRemainingList == null) 0 else x.existingHourlyData.loadRemainingList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0").toString().toLong ) 
                                                                        }
                                                                      else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                        ( x.prevHourData.openLoadsPrev ).toString() 
                                                                      }
                                                                       else{
                                                                         ( if ( x.existingHourlyData.loadRemainingList == null ) "0" else ( if ( x.existingHourlyData.loadRemainingList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" else x.existingHourlyData.loadRemainingList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0") ) ).toString() //Seq().:+ 
                                                                       }
                                                            }.flatten 
                                                    ,f._2.map{ x => /*var totalScratches = 0.toLong*/
                                                                      for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                          ( x.currentHourData.scrQty ).toString()  //Seq().:+  //+ ( if( x.existingHourlyData.totalScratchesList == null) 0 else x.existingHourlyData.totalScratchesList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt-1, "0").toString().toLong )
                                                                        }
                                                                      else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                        ( x.prevHourData.scrQtyPrev ).toString() 
                                                                      }
                                                                       else{
                                                                         ( if ( x.existingHourlyData.totalScratchesList == null ) "0" else ( if ( x.existingHourlyData.totalScratchesList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" else x.existingHourlyData.totalScratchesList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0") ) ).toString()   //Seq().:+
                                                                       }
                                                                      /*totalScratches = totalScratches + 
                                                                                      ( if ( x.existingHourlyData.scrQtyList == null ) 0 
                                                                                        else .existingHourlyData.scrQtyList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt,"0") 
                                                                                        ).toString().toLong                                                                      
                                                                      Seq().:+(totalScratches.toString())*/
                                                              }.flatten 
                                                     ,currentTime
                                                     ,f._2.map{ x => for(i <-x.keyColumnsData.whseStartime.toInt to 
                                                                          (if ( x.keyColumnsData.orderBy.toInt == -1 ) x.keyColumnsData.whseStartime.toInt 
                                                                          else x.keyColumnsData.orderBy.toInt //completeLoadElements.dataHour
                                                                           )  
                                                                      ) yield 
                                                                      if ( x.currentHourData.dataHour == ( if (i>=24) (i-24) else i ) ) {
                                                                          if ( ( x.currentHourData.billQty - x.prevHourData.billQtyPrev
                                                                                 /*( if (x.existingHourlyData.loadBillsList == null) 0 
                                                                                 else x.existingHourlyData.loadBillsList.foldLeft(0)(_.toInt +_.toInt) ).toLong */
                                                                                ) < 0 ) "0"
                                                                          else ( x.currentHourData.billQty - x.prevHourData.billQtyPrev
                                                                                 /*( if (x.existingHourlyData.loadBillsList == null) 0
                                                                                 else x.existingHourlyData.loadBillsList.foldLeft(0)(_.toInt +_.toInt) ).toLong*/ 
                                                                                ).toString()  //Seq().:+ //x.existingHourlyData.scrQtyList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt-1, "0") 
                                                                        }
                                                                     else if ( x.prevHourData.dataHourPrev == ( if (i>=24) (i-24) else i ) ) {
                                                                         if ( ( x.prevHourData.billQtyPrev - 
                                                                                ( if (x.existingHourlyData.loadBillsList == null) 0.toLong  // if Existing List is Null then 0
                                                                                else ( x.existingHourlyData.loadBillsList.foldLeft(0)(_.toInt +_.toInt) // else Sum of all elements from the existing list
                                                                                     - ( if ( x.existingHourlyData.loadBillsList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.loadBillsList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt       // but subtract element present at Prev hour as it is already summed up in foldLeft but it should not be subtracted
                                                                                     ).toLong // Sum of all elements - Element present in list for Prev hour
                                                                                 ) 
                                                                               ) < 0 ) "0"   
                                                                         else ( x.prevHourData.billQtyPrev - 
                                                                                ( if (x.existingHourlyData.loadBillsList == null) 0.toLong 
                                                                                else ( x.existingHourlyData.loadBillsList.foldLeft(0)(_.toInt +_.toInt) 
                                                                                     - ( if ( x.existingHourlyData.loadBillsList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" 
                                                                                        else x.existingHourlyData.loadBillsList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0" ) 
                                                                                       ).toString().toInt 
                                                                                     ).toLong 
                                                                                 ) 
                                                                               ) 
                                                                      }
                                                                       else{
                                                                         ( if ( x.existingHourlyData.loadBillsList == null ) "0" else ( if ( x.existingHourlyData.loadBillsList.length <= ( i-x.keyColumnsData.whseStartime.toInt ) ) "0" else x.existingHourlyData.loadBillsList.applyOrElse(i-x.keyColumnsData.whseStartime.toInt, "0") ) ).toString()  //Seq().:+ 
                                                                       }
                                                              }.flatten 
                                                    ) 
                                           )          
          returnRow
  }
   
  def updateFileIngestion(sqlContext: SQLContext, sc: SparkContext, fileNames: Array[String], status: String) {
    logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   updateFileIngestion              " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
    
    try{
            val updateRDD = fileNames.map { x => Row(status, fileId, x ) }
            
            val updateRDD1 = sc.parallelize( updateRDD )
            logger.debug( " - Update RDD - " )
            updateRDD1.foreach { println }
            
            val tblStruct = new StructType( Array( StructField("houry_cases_status", StringType, true ) 
                                                  ,StructField("file_id", IntegerType, false ) 
                                                  ,StructField("file_name", StringType, false ) 
                                                 ) )
             
            val updateDF = sqlContext.createDataFrame(updateRDD1, tblStruct)
            logger.debug( " - Update Dataframe - " )
            updateDF.foreach { println }
            
           /* updateDF
           .write
           .format("org.apache.spark.sql.cassandra")
           .option("keyspace", "wip_warehouse_data")
           .option("table", "file_streaming_tracker")
           .mode("append")
           .save()*/
           
           logger.debug( " - Hourly Cases Status Updated" )
      
      }
    catch {
      case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing updateFileIngestion at : " + e.getMessage + " "  +  e.printStackTrace() )
    }
  }    
         
}