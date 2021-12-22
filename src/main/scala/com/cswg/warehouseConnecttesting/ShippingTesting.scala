package com.cswg.testing

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

import com.norbitltd.spoiwo.model.{Row => XRow, Sheet => Xsheet}
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.norbitltd.spoiwo.model._
import com.norbitltd.spoiwo._
import com.norbitltd.spoiwo.model.enums.CellFill

object Test {
  var logger = LoggerFactory.getLogger("Examples");
  var extDateTime = getCurrentTime()
  var extDateTimeSt = "" 
  var filePath = "C:/opt/cassandra/default/dataFiles/LoadDetails/"
  
  def getCurrentTime() : java.sql.Timestamp = {
     val now = Calendar.getInstance()
     var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)

  }
  
  def getCurrentTimeWOSpaces() : java.sql.Timestamp = {
     val now = Calendar.getInstance()
     var sdf = new SimpleDateFormat("yyyyMMddHHmmss")     
     return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)

  }
  
  def getCurrentDate(): String = {
    val now = Calendar.getInstance()
    var sdf = new SimpleDateFormat("yyyy-MM-dd")     
    return sdf.format(now.getTime)
  }
  
   def getStringDateTimestamp(date: String): java.sql.Timestamp = {   //getCurrentDate name is changed
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     return new java.sql.Timestamp(sdf.parse(date).getTime())
   }
   
   // 0 -> Job Id
   // 1 -> Environment Name   
  def main(args: Array[String]) {    
     logger.debug("Main for Shipping Testing")
     val environment = if ( args(1) == null ) "dev" else args(1).toString 
     logger.info( " - environment - " + environment )
     
     val (hostName, userName, password) = getCassandraConnectionProperties(environment)
     logger.info( " Host Details got" )
     
     val conf = new SparkConf()
      .setAppName("WIPShipping")
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
     val currentTimeFile = getCurrentTimeWOSpaces()
     val currentDate = getCurrentDate()
     val previousDate = addDaystoCurrentDatePv(-7) 
     val jobId = if ( args(0) == null ) 3 else args(0).toString() 
     /*val extDateTimeS = if ( args(1) == null ) currentTime.toString() else  args(1).toString()
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     extDateTime = new java.sql.Timestamp( ( sdf.parse( extDateTimeS ).getTime ) )
     extDateTimeSt = sdf.format(extDateTime)
     //val extDateTimeTS = new java.sql.Timestamp( extDateTimeS.toLong ) 
     val extDateTimeTM = sdf.parse( extDateTimeS ).getTime*/
     
     /*val currentTimeQuery = s""" SELECT to_date( '$currentTime', 'yyyyMMddHHmmss' ) """
     logger.debug( " - currentTimeQuery - " + currentTimeQuery )
     val currentTimeVal = sqlContext.sql(currentTimeQuery)
     currentTimeVal.show()*/
     /*logger.info( " - extDateTimeS - " + extDateTimeS + "\n" + " - extDateTime - " + extDateTime 
                // + "\n" + " - parse - " + sdf.parse( extDateTimeS )
                 + "\n" + " - extDateTimeTM - " + extDateTimeTM
                // + "\n" + " - TS - " + extDateTimeTS 
                 + "\n" + " - extDateTimeSt - " + extDateTimeSt
                 ) */
     logger.info( " - jobId - " + jobId + " - previousDate - " + previousDate + " - currentDate - " + currentDate 
                 + " - currentTime - " + currentTime + "\n"
                 + " - currentTimeFile - "  + currentTimeFile) 
                 
      
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
                         //.filter(s"warehouse_id IN ( $warehouseId ) ")
                         //.filter(s"warehouse_id IN ( $warehouseId )  ") //closed_timestamp IS NULL or //AND ( to_date(closed_timestamp) = '$previousDate' )
                         
         val dfStopDetails =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "stop_details_by_warehouse"
                                      , "keyspace" -> "wip_shipping_prs") )
                         .load()
                         //.filter(s"warehouse_id IN ( $warehouseId ) ")
         
         val dfAssignmentDetails = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "assignment_details_by_warehouse"
                                        ,"keyspace" -> "wip_shipping_prs" ) )
                          .load()
                          //.filter(s"warehouse_id IN ( $warehouseId ) ")
         
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
                          //.filter(s"warehouse_id IN ( $warehouseId ) ")
                          
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
                         //.filter(s"whse_nbr IN ( $warehouseId ) AND ext_timestamp >= '$previousDate'  ")
                         
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
                          
         /*val dfLoadDetailsEntDt = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "load_details_by_entry_date"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()*/
         
         val dfFileStreamin = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "file_streaming_tracker"
                                        ,"keyspace" -> "wip_configurations" ) )
                          .load()     
                          //.filter( " load_status = 'Load_complete' " )
                          //.filter( " creation_time < '2017-04-22 23:57:44' ")
                          //.filter(s" validation_status IN ( 'Ingested' ) ")  //,'Fully_Complete' AND creation_time BETWEEN '$currentDate' AND '$currentTime'
                                      
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
                          
          val dfLoadDetailsStreaming = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "load_details_by_warehouse_stream"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load()                           
                          //.filter(s"warehouse_id IN ( $warehouseId ) ")
                          
          val dfStopDetailsStreaming = sqlContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "stop_details_by_warehouse_stream"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load()                           
                          .filter(s"warehouse_id IN ( $warehouseId ) ")
                          
         logger.debug(" - DataFrame Declared")
         
         dfWarehouseShipping.registerTempTable("warehouse_shipping_data")
         dfWarehouseShippingHist.registerTempTable("warehouse_shipping_data_history")
         //dfLoadDetailsEntDt.registerTempTable("load_details_by_entry_date")
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
         dfLoadDetailsStreaming.registerTempTable("load_details_by_warehouse_stream")
         dfStopDetailsStreaming.registerTempTable("stop_details_by_warehouse_stream")
         logger.debug(" - Temp Tables Registered")
         
         //val extTimestamp = getStringDateTimestamp("2017-06-13 13:22:00")
         //logger.debug(" Converted Timestamp - " + extTimestamp)
         /*val factor = 3
         val multiplier = (i: Int) => { val factor = 3 
                                         i * factor }
         logger.debug( "Multiplier - " +  multiplier(1) + multiplier(2) )         
         */
         
         /*var employeesImages = dfEmployeeImages.filter(" CAST( warehouse_id as Int) in (" + warehouseId + ") ")
                                               .select(dfEmployeeImages("warehouse_id").cast("Int").alias("warehouse_id"), dfEmployeeImages("employee_id"), dfEmployeeImages("first_name"), dfEmployeeImages("last_name"))
                                               .cache()*/
         
          //testShippingCasting(sqlContext)
         //employeesImages.show(50)
         StreamingShipping(sqlContext)    //Check TTL
         //loadClosedStreaming(sqlContext)
         //tombstoneShipping(sqlContext)
         //adhocQuery(sqlContext, warehouseId)
         //trimZeroesIndexes(sqlContext, warehouseId)
         //execRandomQuery(sqlContext)
         //prevDayMergedAssignments(sqlContext)
         //getData(sqlContext)
         //deleteAssignmentsCheck(sqlContext)
         //getABACapacity(sqlContext: SQLContext)
         //validateExtTime(sqlContext, dfWarehouseShipping)
         //validateCustomerHourly(sqlContext, dfAssignmentDetails)
         //getCounts(sqlContext)
         
         //File Streaming
         //updateData(sqlContext, sc, dfFileStreamin)
         //fileStreaming(sqlContext)
         //updateLoadStreaming(sqlContext, dfLoadDetailsStreaming)
         
         
         //Snapshots
         //verifyData(sqlContext, sc)
         //getSnapshot(sqlContext, sc)
         //getData(sqlContext, sc)
         //mergedDiffTime(sqlContext)
         //getMissingMergedAssgn(sqlContext)
         //missingLoadsAssignments(sqlContext)
         
         //extTimeStampCheck(sqlContext, dfFileStreamin)
         //extTimeStampTrimMS(sqlContext, dfFileStreamin)
         //val reverse = reverseString("Pushkar Goyal")
          //logger.debug("reverse - " + reverse)
         //verifyLoadLoaderPercent(sqlContext)
         //copyData(sqlContext)
         
         //Analytics
         //selectorsWithDelayedLoads(sqlContext)
         //hoursAnalysis(sqlContext, sc)
         //trailerNeeds(sqlContext, sc)
         //trailerAvailability(sqlContext, sc)
         //validateAnalysis(sqlContext, sc)
         
         //loadRemaining(sqlContext) 
         
         //var parameter: Long = 0L
         /*val qtyQuery = s""" SELECT load_number FROM load_details_by_warehouse WHERE load_number = '000000476014' """
         val qty = sqlContext.sql(qtyQuery)
         val qtyValue = qty.select(qty("load_number")).collect.map { x => x.get(0) }.apply(0)
         logger.debug(" - qtyValue - "   + qtyValue )
         if ( qtyValue == null ) {
             parameter = 0L
         }  else{
           parameter = qtyValue.toString().toLong
         }
         */
         
         //logger.debug(" - parameter - "   + parameter )
         
         //qty = null
         //val result = checkNull(parameter.toLong)
         //logger.debug( " - result - " + result )
         
         logger.debug(" - Process Completed")
         
         
     }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def closureTest(i: Int): Int = {
     return i
  }

   def getCounts(sqlContext: SQLContext) = {
      val loadsCountQuery  = """ select count(*) from load_details_by_warehouse
                              """
           
      val stopsCountQuery  = """ select count(*) from stop_details_by_warehouse
                              """
            
      val asgnDetailsQuery  = """ select count(*) from assignment_details_by_warehouse
                              """
      
     logger.debug( "loadsCountQuery - " + loadsCountQuery)
     val loadsCount = sqlContext.sql(loadsCountQuery)
     loadsCount.show(50)
     
      logger.debug( "stopsCountQuery - " + stopsCountQuery)
     val stopsCount = sqlContext.sql(stopsCountQuery)
     stopsCount.show(50)
     
     logger.debug( "asgnDetailsQuery - " + asgnDetailsQuery)
     val asgnDetails = sqlContext.sql(asgnDetailsQuery)
     asgnDetails.show(50)
         
     /*asgnDetails.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "TombstoneLoadDetails/") */
  }  
   
  def testShippingCasting(sqlContext: SQLContext) = {
      val asgnDetailsQuery  = """ SELECT whse_nbr, snapshot_id, ent_date,  load_nbr, stop_nbr, assgn_nbr
                                        ,cube, sel_cube, sel_qty, scr_qty, bill_qty
                                        ,CAST( cube as Int) number_of_cubes
                                        ,FLOOR( CAST( cube as float) ) floor_number_of_cubes 
                                    FROM warehouse_shipping_data sd
                                   WHERE whse_nbr = '29'   
                                   AND snapshot_id = 'a0c0f030-a2ec-11e7-b070-d186589e3899'
                                   AND ent_date = '20170925' 
                                   AND load_nbr = '39740'  
                                   AND stop_nbr = '1' 
                                   AND assgn_nbr = '32749'
                              """
     logger.debug( "asgnDetailsQuery - " + asgnDetailsQuery)
     val asgnDetails = sqlContext.sql(asgnDetailsQuery)
     asgnDetails.show(50)
         
     /*asgnDetails.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "TombstoneLoadDetails/")*/       
  }  
  
  def tombstoneShipping(sqlContext: SQLContext) = {
      val loadDetailsQuery  = """ SELECT * 
                                    FROM load_details_by_warehouse
                                   WHERE token(warehouse_id, ops_date) > -4613431209607112644 
                                     AND token(warehouse_id, ops_date) <= -4515190335915455083 
                                   LIMIT 1000
                              """
     logger.debug( "loadDetailsQuery - " + loadDetailsQuery)
     val loadDetails = sqlContext.sql(loadDetailsQuery)
     loadDetails.show(50)
         
     loadDetails.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "TombstoneLoadDetails/")       
  }
  
  def updateLoadStreaming(sqlContext: SQLContext, dfLoadDetailsStreaming: DataFrame) = {
      val loadDetails  = dfLoadDetailsStreaming.select("warehouse_id", "ops_date", "load_number")
                              .filter(" warehouse_id = '29' AND ops_date = '2017-11-26' ")
                              .select("warehouse_id", "ops_date", "load_number")
                              .withColumn("loaded_pct", lit("0") )
                              
                              
     //logger.debug( "loadDetailsQuery - " + loadDetailsQuery)
     //val loadDetails = sqlContext.sql(loadDetailsQuery)
     loadDetails.show(50)
         
     loadDetails.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "wip_shipping_ing")
        .option("table", "load_details_by_warehouse_stream")
        .mode("append").save()      
  }
  
   def StreamingShipping (sqlContext: SQLContext) {
     
     val previousDate = addDaystoCurrentDatePv(-3)
     val currenDate = getCurrentDate()
    
      val StreamingDataQuery = s"""SELECT warehouse_id, load_number, chain_name, chain_number, closed_date, closed_time, closed_timestamp
                                     ,ops_date, loader_name, loader_employee_id, loaded_pct, selected_pct
                                     ,bill_cube, selected_cube, bill_qty, selected_qty, scr_qty, expected_pallets, actual_pallets
                                     ,door_number, trailer_number, trailer_type, shift_id, is_load_deferred, number_of_stops
                                     ,dispatch_timestamp, entry_timestamp, wms_update_timestamp, orig_dispatch_timestamp, selection_start_timestamp     
                                     ,update_time 
                                  FROM load_details_by_warehouse_stream
                                 WHERE ( orig_dispatch_timestamp IS NULL OR dispatch_timestamp IS NULL )
                                """
      
      val ShippingDataQuery = s"""SELECT warehouse_id, load_number, customer_number, customer_name,  closed_time, closed_timestamp
                                   ,ops_date, loader_name, loader_employee_id, loaded_pct, selected_pct
                                   ,cube_qty, total_cases, expected_pallets, actual_pallets
                                   ,door_number, trailer_type, shift_id, is_load_deferred, number_of_stops
                                   ,dispatch_timestamp, entry_date, set_back_timestamp    
                                   ,update_time 
                                FROM load_details_by_warehouse
                               WHERE warehouse_id = '29'
                                 AND ops_date = '2017-07-25'
                                 AND load_number IN ('49608') 
                              """  
      
      //'69808'
      val loadPercentQuery = s""" SELECT *
                             FROM load_percent_by_warehouse
                             WHERE load_number IN ('39411')
                         ORDER BY warehouse_id, gate_datetime, load_number
                        """           

      val loadLoadersQuery = s"""SELECT DISTINCT warehouse_id, gate_datetime, load_number, loader_id, loader_name, update_status, update_time 
                                FROM load_loaders_by_warehouse
                               WHERE warehouse_id = '29'
                                 AND load_number IN ('39411')
                               ORDER BY warehouse_id, gate_datetime, load_number
                              """    

       /* logger.debug( "loadPercentQuery - " + loadPercentQuery)
          val loadPercent = sqlContext.sql(loadPercentQuery)
          loadPercent.show(20)
          
       logger.debug( "loadLoadersQuery - " + loadLoadersQuery)
          val loadLoaders = sqlContext.sql(loadLoadersQuery)
          loadLoaders.show(20)    
         */ 
        logger.debug( "StreamingDataQuery - " + StreamingDataQuery)
          val StreamingData = sqlContext.sql(StreamingDataQuery)
          StreamingData.show(20)
         
      /*val loadShippingDetailsQuery = s""" SELECT *
                                           FROM warehouse_shipping_data_history wsdh
                                          WHERE wsdh.load_nbr = '69740' 
                                          """
      */
       /*  logger.debug( "loadPercentQuery - " + loadPercentQuery)
          val loadShippingDetails = sqlContext.sql(loadPercentQuery)
          loadShippingDetails.show(50)
         
         /* loadShippingDetails.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingDataQuery/") */
             */
    /*val loadDetailsQuery = s""" SELECT DISTINCT ldws.warehouse_id, ldws.load_number, llw.gate_datetime, chain_name, chain_number, closed_date, closed_time, closed_timestamp
                                       ,ops_date, ldws.loader_name, ldws.loader_employee_id, ldws.loaded_pct, ldws.selected_pct
                                       ,bill_cube, selected_cube, bill_qty, selected_qty, scr_qty, expected_pallets, actual_pallets
                                       ,door_number, trailer_number, trailer_type, shift_id, is_load_deferred, number_of_stops
                                       ,dispatch_timestamp, entry_timestamp, wms_update_timestamp, orig_dispatch_timestamp, selection_start_timestamp     
                                       ,ldws.update_time 
                             FROM load_details_by_warehouse_stream ldws
                             LEFT OUTER JOIN load_loaders_by_warehouse llw
                                     ON ldws.warehouse_id = llw.warehouse_id
                                    AND ( ldws.load_number = llw.load_number
                                          OR ldws.load_number = llw.pallet_ovrld_to_load
                                        )
                            WHERE chain_name IS null
                         ORDER BY ldws.warehouse_id, ldws.closed_timestamp, ldws.load_number 
                        """
                     */
          
   
  }
    
    
  def loadClosedStreaming(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  loadClosedStreaming               " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )    
    val warehouseId = "'44'"
    val startTime = getCurrentDate   //addDaystoCurrentDatePv(-1) 
    val endTime =  addDaystoCurrentDatePv(-1)    //getCurrentDate    
    val previousDate = addDaystoCurrentDatePv(-1)
    val previousLoadsDate = addDaystoCurrentDatePv(-7)
    
    
    /*val loadDetailByWarehouseQuery = s"""
                SELECT * FROM load_details_by_warehouse
                WHERE warehouse_id  = '29'
                   AND load_number LIKE '%49425%'
                """    */
    //                            WHERE closed_timestamp IS NULL OR closed_timestamp >= '$endTime'
    //                                    AND ldws.ops_date = ldbw.ops_date
     val loadDetailsMissingLoadsQuery = s""" SELECT DISTINCT ldbw.warehouse_id, ldbw.ops_date, ldbw.load_number
                                       ,ldws.warehouse_id, ldws.ops_date, ldws.load_number
                                       ,ldbw.customer_name, ldbw.customer_number, ldbw.closed_time, ldbw.closed_timestamp
                                       ,ldws.chain_name, ldws.chain_number, ldws.closed_time, ldws.closed_timestamp
                                       ,ldbw.loader_name, ldbw.loader_employee_id, ldbw.loaded_pct, ldbw.selected_pct
                                       ,ldws.loader_name, ldws.loader_employee_id, ldws.loaded_pct, ldws.selected_pct
                             FROM load_details_by_warehouse ldbw 
                             LEFT OUTER JOIN load_details_by_warehouse_stream ldws
                                     ON ldws.warehouse_id = ldbw.warehouse_id
                                    AND ldws.load_number = ldbw.load_number
                            WHERE ldbw.ops_date >= '$previousDate'
                         ORDER BY ldws.warehouse_id, ldbw.ops_date, ldws.load_number, ldws.closed_timestamp
                        """    
 
         logger.debug( "loadDetailsMissingLoadsQuery - " + loadDetailsMissingLoadsQuery)
          val loadDetailsMissingLoads = sqlContext.sql(loadDetailsMissingLoadsQuery)
          //loadDetailsMissingLoads.show(50)
         
          loadDetailsMissingLoads.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingLoadDetailsMissing/") 
             
     val loadDetailsStreamQuery = s""" SELECT DISTINCT ldws.warehouse_id, ldws.load_number, llw.gate_datetime, ldws.chain_name, ldws.chain_number, closed_date, closed_time, closed_timestamp
                                       ,ldws.ops_date, ldws.loader_name, ldws.loader_employee_id, ldws.loaded_pct, ldws.selected_pct
                                       ,ldws.bill_cube, ldws.selected_cube, ldws.bill_qty, ldws.selected_qty, ldws.scr_qty, ldws.expected_pallets, ldws.actual_pallets
                                       ,ldws.door_number, trailer_number, trailer_type, shift_id, is_load_deferred, number_of_stops
                                       ,dispatch_timestamp, entry_timestamp, wms_update_timestamp, orig_dispatch_timestamp, selection_start_timestamp     
                                       ,ldws.update_time
                             FROM load_details_by_warehouse_stream ldws
                             LEFT OUTER JOIN load_loaders_by_warehouse llw
                                     ON ldws.warehouse_id = llw.warehouse_id
                                    AND ( ldws.load_number = llw.load_number
                                          OR ldws.load_number = llw.pallet_ovrld_to_load
                                        )
                         ORDER BY ldws.warehouse_id, closed_timestamp, ldws.load_number 
                        """
     
         logger.debug( "loadDetailsStreamQuery - " + loadDetailsStreamQuery)
          val loadDetailsStream = sqlContext.sql(loadDetailsStreamQuery)
          //loadDetailsStream.show(50)
         
          loadDetailsStream.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingLoadDetails/") 
             
    //                                  AND ldbw.ops_date = ldbws.ops_date 
    //WHERE ldbws.closed_timestamp IS NULL OR ldbws.closed_timestamp >= '$endTime'
    val loadStreamCloseddateQuery = s""" SELECT ldbw.warehouse_id
                                  ,ldbw.load_number
                                  ,ldbws.load_number s_load_number
                                  ,from_unixtime(unix_timestamp( ldbw.closed_timestamp, 'yyyyMMdd HHmmss' ), 'yyyyMMdd')  closed_date
                                  ,ldbw.closed_time
                                  ,ldbw.closed_timestamp
                                  ,ldbws.closed_date s_closed_date
                                  ,ldbws.closed_time s_closed_time
                                  ,ldbws.closed_timestamp s_closed_timestamp
                                  ,ldbw.ops_date
                                  ,ldbws.ops_date s_ops_date
                                  ,ldbws.update_time 
                             FROM load_details_by_warehouse_stream ldbws
                             LEFT OUTER JOIN load_details_by_warehouse ldbw
                                 ON ldbw.warehouse_id = ldbws.warehouse_id
                                  AND ldbw.load_number = ldbws.load_number
                         ORDER BY ldbws.warehouse_id, ldbws.load_number, ldbws.closed_timestamp 
                        """
         logger.debug( "loadStreamCloseddateQuery - " + loadStreamCloseddateQuery)
          val loadStreamCloseddate = sqlContext.sql(loadStreamCloseddateQuery)
          loadStreamCloseddate.show(50)
         
          loadStreamCloseddate.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingLoadClosedDateDetails/") 
             
                            //                            WHERE closed_timestamp IS NULL OR closed_timestamp >= '$endTime'      
                            
    val loadLoadersQuery = s""" SELECT *
                                  FROM load_loaders_by_warehouse
                                 WHERE gate_datetime >= '$previousLoadsDate'
                         ORDER BY warehouse_id, gate_datetime, load_number
                        """   
     logger.debug( "loadLoadersQuery - " + loadLoadersQuery)
     val loadLoaders = sqlContext.sql(loadLoadersQuery)
          //loadLoaders.show(20)
          //logger.debug( "Adhoc Schema - " +  adhoc.schema )
         
    loadLoaders.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingLoadLoaders/") 
             
    val loadPercentQuery = s""" SELECT *
                             FROM load_percent_by_warehouse
                             WHERE gate_datetime >= '$previousLoadsDate'
                         ORDER BY warehouse_id, gate_datetime, load_number
                        """       
                        
     logger.debug( "loadPercentQuery - " + loadPercentQuery)
     val loadPercent = sqlContext.sql(loadPercentQuery)
     //loadPercent.show(20)
          //logger.debug( "Adhoc Schema - " +  adhoc.schema )
         
     loadPercent.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingLoadPercent/")   
             
      /*val stopDetailsQuery = s""" SELECT warehouse_id
                                  ,load_number
                                  ,stop_number
                                  ,ops_date
                                  ,update_time
                             FROM stop_details_by_warehouse_stream
                         ORDER BY warehouse_id, load_number, stop_number
                        """               
             
         logger.debug( "stopDetailsQuery - " + stopDetailsQuery)
          val stopDetails = sqlContext.sql(stopDetailsQuery)
          stopDetails.show(20)
          //logger.debug( "Adhoc Schema - " +  adhoc.schema )
         
          stopDetails.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "StreamingStopDetails/")        
					*/

               
  }
  
  /*def time_delta(startTime: Timestamp, endTime: Timestamp): Integer = {
    val end = datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S.%f")
    val start = datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%S.%f")
    delta = (end-start).total_seconds()
    return delta
  }*/
  
  def adhocQuery(sqlContext: SQLContext, warehouseId: String) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  adhocQuery                        " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )    
    val warehouseId = "'29'"
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate    
    
          /*val adhocQuery = s""" SELECT LPAD( hour(ext_timestamp) , 2, '0' ) hour_exttimt
                                     ,hour('2017-05-08 23:52:00') hour_for_da
                                     ,CASE WHEN hour('2017-05-08 23:52:00') < CASE WHEN LPAD( hour(ext_timestamp), 2, '0' ) = '00' THEN '24' ELSE hour(ext_timestamp) END THEN 'Y' ELSE 'N' END worked
                                 FROM file_streaming_tracker 
                                WHERE file_name = 'WIP_3_ASGN.201705090007.CSV'"""*/
          /*val adhocQuery = s""" SELECT invoice_site, MAX(ops_date), MAX(ext_timestamp) 
                                  FROM assignments_merged
                                 WHERE invoice_site IN ($warehouseId)
                               GROUP BY invoice_site
                            """ */
                                 
                //AND ext_timestamp IS NULL
          val adhocQuery = s""" SELECT file_id, file_name, snapshot_id, ext_timestamp
                                      ,error, hourly_cases_status, load_status, trailer_need_status
                                     ,update_time, warehouses
                                  FROM file_streaming_tracker_status
                                 WHERE file_id = 1
                                   AND snapshot_id = '55acfb40-3a3f-11e7-ac92-91704e58b910'
                               ORDER BY file_name DESC
                            """ 
          //fst.file_name, fst.ext_timestamp file_ext_timestamp, fst.creation_time, fst.update_time, fst.validation_status                                 
          //,file_streaming_tracker fst
          //fst.snapshot_id = wsdh.snapshot_id  AND
          // assgn_nbr IN ( '098141','098142' )
          /*
           *                                      ,wsdh.stop_nbr
                                     ,wsdh.assgn_nbr
                                     ,wsdh.com_code
                                     
           */
          val shippingHistoryAssgn = s""" SELECT DISTINCT wsdh.whse_nbr warehouse_id
                                     ,wsdh.load_nbr 
                                     ,wsdh.ext_date
                                     ,wsdh.ext_time
                                     ,wsdh.ext_timestamp
                                     ,SUM(bill_qty) OVER ( PARTITION BY wsdh.whse_nbr
                                                                       ,wsdh.load_nbr
                                                                       ,wsdh.ext_timestamp
                                                                        ) bill_qty
                                     ,SUM(sel_cube) OVER ( PARTITION BY wsdh.whse_nbr
                                                                       ,wsdh.load_nbr
                                                                       ,wsdh.ext_timestamp 
                                                                        ) cases_selected
                                     ,SUM(scr_qty) OVER ( PARTITION BY wsdh.whse_nbr
                                                                       ,wsdh.load_nbr
                                                                       ,wsdh.ext_timestamp 
                                                                        ) scr_qty
                                 FROM warehouse_shipping_data_history wsdh
                                WHERE wsdh.load_nbr = '000000029104'
                                  AND whse_nbr = $warehouseId
                               ORDER BY wsdh.ext_timestamp, load_nbr
                       """ 
                  
          val assgnPres = s""" SELECT DISTINCT wsdh.warehouse_id
                                     ,wsdh.ops_date
                                     ,wsdh.load_number
                                     ,wsdh.ext_timestamp
                                     ,wsdh.merged_status
                                     ,SUM(bill_qty) OVER ( PARTITION BY wsdh.warehouse_id
                                                                       ,wsdh.ops_date
                                                                       ,wsdh.load_number
                                                                       ,wsdh.ext_timestamp
                                                                       ,wsdh.merged_status ) bill_qty
                                     ,SUM(cases_selected) OVER ( PARTITION BY wsdh.warehouse_id
                                                                       ,wsdh.ops_date
                                                                       ,wsdh.load_number
                                                                       ,wsdh.ext_timestamp 
                                                                       ,wsdh.merged_status ) cases_selected
                                     ,SUM(scr_qty) OVER ( PARTITION BY wsdh.warehouse_id
                                                                       ,wsdh.ops_date
                                                                       ,wsdh.load_number
                                                                       ,wsdh.ext_timestamp 
                                                                       ,wsdh.merged_status ) scr_qty
                                 FROM assignment_details_by_warehouse_history wsdh
                                WHERE wsdh.load_number = '000000029104'
                                  AND warehouse_id = $warehouseId
                               ORDER BY wsdh.ext_timestamp, load_number 
                       """ 
                                  
          val shiftLoadsQuery = """ SELECT * 
                                 FROM warehouse_shipping_data_history 
                                WHERE whse_nbr = '29'
                                  AND load_nbr IN ( '000000039367','000000039368','000000039369','000000039370','000000039371','000000039372','000000039373','000000039374','000000039375','000000039376' )
                             ORDER BY whse_nbr, load_nbr, shift, snapshot_id 
                           """
          
          //ORDER BY warehouse_id, ops_date, load_number, closed_timestamp DESC
          val loadClosedQuery = """ SELECT DISTINCT warehouse_id, ops_date, load_number
                                         , MAX( closed_timestamp ) OVER ( PARTITION BY warehouse_id, ops_date, load_number 
                                                                               )
                                                           closed_timestamp
                                 FROM assignment_details_by_warehouse 
                                WHERE warehouse_id = '29'
                                  AND ops_date = '2017-06-26'
                                  AND load_number IN ('000000039331', '000000049142' )
                             ORDER BY warehouse_id, ops_date, load_number
                           """
          //TO_CHAR( TRIM( LEADING 0 FROM load_number) ) 
          val trimLeading0s = """ SELECT cast (load_number as int) load_number_int
                                        ,load_number
                                    FROM load_details_by_warehouse
                                  WHERE warehouse_id = '29'
                                    AND ops_date = '2017-07-11'
                              """
          logger.debug( "adhocQuery - " + trimLeading0s)
          val adhoc = sqlContext.sql(trimLeading0s)
          adhoc.show(20)
          //logger.debug( "Adhoc Schema - " +  adhoc.schema )
         
          /*adhoc.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "Adhoc/") */
             
          /*logger.debug( "adhocQuery - " + assgnPres)
          val adhoc1 = sqlContext.sql(assgnPres)
          adhoc1.show(20)
          //logger.debug( "Adhoc Schema - " +  adhoc.schema )
         
          adhoc1.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "Adhoc1/")     */         
  }
  
  def trimZeroesIndexes(sqlContext: SQLContext, warehouseId: String) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  trimZeroesIndexes                 " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )    
    val warehouseId = "'29'"
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate    
    
          val trimLeading0s = """ SELECT trim_zeroes_columns
                                    FROM file_ingestion_controller
                                   WHERE job_id = 1
                                     AND file_id = 1
                              """
          logger.debug( "adhocQuery - " + trimLeading0s)
          val adhoc = sqlContext.sql(trimLeading0s)
          adhoc.show(20)
          //logger.debug( "Adhoc Schema - " +  adhoc.schema )
         
          val trimZeroes = adhoc.select("trim_zeroes_columns").first().getString(0)
          val indexes = trimZeroes.split(",").map( x => x.split(":").apply(1) )
          val indexesString = indexes.mkString(",")
          
          if ( indexesString.contains(5.toString()) ){
              logger.debug( "String Value exists")
          }else{
              logger.debug( "String Value does not exists")
          }  
          
         if ( indexes.contains(5.toString()) ){
              logger.debug( "Array Value exists")
          }else{
              logger.debug( "Array Value does not exists")
          }
    
          logger.debug("trimZeroes - " + trimZeroes + "\n" + 
                       "indexes - "  + indexes + "\n" +
                       "indexesString - " + indexesString
                       )
          /*adhoc.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "Adhoc/") */
                       
          val assignmentsMergedQuery = """ SELECT load_nbr, assgn_nbr 
                                        FROM assignments_merged 
                                       WHERE load_nbr IN ( '000000000000','000000039485'
   		                                                   )
                                  """
           logger.debug( "assignmentsMerged - " + assignmentsMergedQuery)
           val assignmentsMerged = sqlContext.sql(assignmentsMergedQuery)
           assignmentsMerged.show(20)       
           
           val convertedValues = assignmentsMerged.select("load_nbr", "assgn_nbr").collect().map { x => Row( x.getString(0).toInt.toString(), x.getString(1) ) }
           convertedValues.foreach(println)
  }
  
  def fileStreaming(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  fileStreaming                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      //sqlContext.udf.register("timeDiff", time_delta _)      
      val startTime = addDaystoCurrentDatePv(-2)
       try{
             //                                             AND validation_status IN ( 'Fully_Complete' )
         //,datediff( from_unixtime(unix_timestamp( update_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss'), from_unixtime(unix_timestamp( creation_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss')  ) * (24*60*60) update_time_diff_sec
             val fileStreamingQuery = s""" SELECT fst.file_id
                                             ,fst.file_name
                                             ,fst.ext_timestamp
                                             ,fst.creation_time
                                             ,fst.update_time file_update_time
                                             ,fsts.update_time tracker_update_time
                                             ,( unix_timestamp( fst.creation_time ) - unix_timestamp( fst.ext_timestamp ) ) / 60 creation_ext_diff_inmin
                                             ,( unix_timestamp( fst.update_time ) - unix_timestamp( fst.creation_time ) ) / 60 file_update_diff_inmin
                                             ,( unix_timestamp( fsts.update_time ) - unix_timestamp( fst.update_time ) ) / 60 tracker_update_diff_inmin
                                             ,( unix_timestamp( fst.update_time ) - unix_timestamp( fst.creation_time ) ) file_update_diff_insec
                                             ,( unix_timestamp( fst.update_time ) - unix_timestamp( fst.creation_time ) ) / 3600 file_update_diff_inhr
                                             ,fsts.hourly_cases_status
                                             ,fsts.load_status
                                             ,fst.validation_status
                                             ,fsts.trailer_need_status
                                             ,fst.snapshot_id
                                             ,fst.file_moved_status
                                             ,SUBSTR( fst.error,1, 500) error
                                             ,fst.rejected_rows
                                            FROM file_streaming_tracker fst
                                            LEFT JOIN file_streaming_tracker_status fsts
                                                   ON fst.file_id = fsts.file_id 
                                                  AND fst.file_name = fsts.file_name
                                                  AND fst.snapshot_id = fsts.snapshot_id
                                           WHERE fst.file_id = 1
                                             AND fst.creation_time >= '$startTime'
                                        ORDER BY fst.ext_timestamp DESC, fst.creation_time DESC, fst.update_time DESC, fsts.update_time DESC
                                          """
             logger.debug( " - Query - " + fileStreamingQuery );
             val fileStreaming = sqlContext.sql(fileStreamingQuery)     
             logger.debug( " - Query fired " ) 
            //fileStreaming.show(40)
             
             fileStreaming.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "fileStreaming/") 
             
             val fileStreamingOnlyQuery = s""" SELECT fst.file_id
                                             ,fst.file_name
                                             ,fst.ext_timestamp
                                             ,fst.creation_time
                                             ,fst.update_time file_update_time
                                             ,( unix_timestamp( fst.creation_time ) - unix_timestamp( fst.ext_timestamp ) ) / 60 creation_ext_diff_inmin
                                             ,( unix_timestamp( fst.update_time ) - unix_timestamp( fst.creation_time ) ) / 60 file_update_diff_inmin
                                             ,( unix_timestamp( fst.update_time ) - unix_timestamp( fst.creation_time ) ) file_update_diff_insec
                                             ,( unix_timestamp( fst.update_time ) - unix_timestamp( fst.creation_time ) ) / 3600 file_update_diff_inhr
                                             ,fst.validation_status
                                             ,fst.snapshot_id
                                             ,fst.file_moved_status
                                             ,SUBSTR( fst.error,1, 500) error
                                             ,fst.rejected_rows
                                            FROM file_streaming_tracker fst
                                           WHERE fst.file_id = 1
                                             AND fst.creation_time >= '$startTime'
                                        ORDER BY fst.ext_timestamp DESC, fst.creation_time DESC, fst.update_time DESC
                                          """
             logger.debug( " - Query - " + fileStreamingOnlyQuery );
             val fileStreamingOnly = sqlContext.sql(fileStreamingOnlyQuery)     
             logger.debug( " - Query fired " ) 
            //fileStreaming.show(40)
             
             fileStreamingOnly.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "fileStreamingOnly/")              
       }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def loadRemaining(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  loadRemaining                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )  
       try{
             //            
         
           val warehouseOpsDayQuery = """ SELECT facility warehouse_id
                                              ,ops_date
                                              ,'PH' date_type
                                              ,oracle_start_time
                                          FROM warehouse_operation_time
                                         WHERE facility = '29' 
                                           AND ops_date = '2017-05-10'
                                    """
           
           logger.debug(" - warehouseOpsDayQuery - " + warehouseOpsDayQuery )
            //val openLoad = Future{ sqlContext.sql(openLoadQuery) }
            val warehouseOpsDay = sqlContext.sql(warehouseOpsDayQuery)
            warehouseOpsDay.show(30) 
            warehouseOpsDay.registerTempTable("warehouse_ops_days")
          
              //,adbw.closed_timestamp
            val openWCLoadPrevQuery = s""" SELECT DISTINCT adbw.warehouse_id
                                          ,adbw.ops_date
                                          ,adbw.customer_number
                                          ,adbw.customer_name
                                          ,adbw.load_number
                                          ,adbw.is_load_deferred
                                          ,wot.date_type
                                      FROM assignment_details_by_warehouse_prev adbw
                                          ,load_details_by_warehouse ldbw
                                          ,warehouse_ops_days wot
                                     WHERE wot.warehouse_id = adbw.warehouse_id
                                       AND wot.ops_date = adbw.ops_date 
                                       AND wot.date_type = 'PH'
                                       AND adbw.customer_number IS NOT NULL
                                       AND ldbw.warehouse_id = adbw.warehouse_id
                                       AND ldbw.ops_date = adbw.ops_date
                                       AND ldbw.load_number = adbw.load_number
                                       AND ldbw.load_number = '000000059401'
                                       AND ( adbw.is_load_deferred = 'N' OR adbw.is_load_deferred IS NULL )
                                       AND adbw.closed_timestamp IS NULL 
                                          """
          logger.debug(" - openWCLoadPrevQuery - " + openWCLoadPrevQuery )
          //val openLoad = Future{ sqlContext.sql(openLoadQuery) }
          val openWCLoadPrev = sqlContext.sql(openWCLoadPrevQuery)
          openWCLoadPrev.show(30)     //Commented on 03/09/2017 to improve performance
           // Ended on 02/15/2017 to handle Open Loads Issue
          //openLoadPrev.persist()   // Added on 02/17/2017 as Processing is not happening for Open Loads Calculation          
          
          //Modification started on 02/23/2017
          //,adbw.closed_timestamp
          val closedLoadPrevQuery = s""" SELECT DISTINCT adbw.warehouse_id
                                          ,adbw.ops_date
                                          ,adbw.customer_number
                                          ,adbw.customer_name
                                          ,adbw.load_number
                                          ,adbw.is_load_deferred
                                          ,wot.date_type
                                      FROM assignment_details_by_warehouse_prev adbw
                                          ,load_details_by_warehouse ldbw
                                          ,warehouse_ops_days wot
                                     WHERE wot.warehouse_id = adbw.warehouse_id
                                       AND wot.ops_date = adbw.ops_date 
                                       AND wot.date_type = 'PH'
                                       AND adbw.customer_number IS NOT NULL
                                       AND ldbw.warehouse_id = adbw.warehouse_id
                                       AND ldbw.ops_date = adbw.ops_date
                                       AND ldbw.load_number = adbw.load_number
                                       AND ldbw.load_number = '000000059401'
                                       AND ( adbw.is_load_deferred = 'N' OR adbw.is_load_deferred IS NULL )
                                       AND adbw.closed_timestamp IS NOT NULL 
                                          """
          logger.debug(" - closedLoadPrevQuery - " + closedLoadPrevQuery )
          //val openLoad = Future{ sqlContext.sql(openLoadQuery) }
          val closedLoadPrev = sqlContext.sql(closedLoadPrevQuery)
          closedLoadPrev.show(40)
          
          val openLoadPrev = openWCLoadPrev.except(closedLoadPrev)
          openLoadPrev.show()   //Commented on 03/09/2017 to improve performance
          
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing loadRemaining at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def mergedDiffTime(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  mergedDiffTime                    " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      //sqlContext.udf.register("timeDiff", time_delta _)      
       try{
             //                                             AND validation_status IN ( 'Fully_Complete' )
         //,datediff( from_unixtime(unix_timestamp( update_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss'), from_unixtime(unix_timestamp( creation_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss')  ) * (24*60*60) update_time_diff_sec
             val lateMergePrevQuery = s""" SELECT adbw.warehouse_id
                                                  ,adbw.ops_date
                                                  ,adbw.load_number
                                                  ,adbw.stop_number
                                                  ,adbw.assignment_id
                                                  ,adbw.ext_timestamp assgn_ext_timestamp
                                                  ,hour(adbw.ext_timestamp) assgn_ext_timestamp_hr
                                                  ,am.ext_timestamp merged_ext_timestamp
                                                  ,hour(am.ext_timestamp) merged_ext_timestamp_hr
                                                  ,adbw.snapshot_id assgn_snapshot_id
                                                  ,am.snapshot_id merged_snapshot_id
                                                  ,adbw.bill_qty
                                                  ,adbw.cases_selected
                                                  ,adbw.scr_qty
                                            FROM assignment_details_by_warehouse_prev adbw
                                                ,assignments_merged am
                                           WHERE adbw.warehouse_id = am.invoice_site
                                             AND adbw.ops_date = am.ops_date
                                             AND adbw.assignment_id = am.assgn_nbr
                                             AND adbw.warehouse_id = '29'
                                             AND adbw.ops_date = '2017-05-10'
                                             AND hour(adbw.ext_timestamp) <> hour(am.ext_timestamp)
                                             AND merged_status = 'N'
                                        ORDER BY warehouse_id, ops_date, load_number, stop_number, assignment_id
                                          """
             logger.debug( " - Query - " + lateMergePrevQuery );
             val lateMergePrev = sqlContext.sql(lateMergePrevQuery)     
             logger.debug( " - Query fired " ) 
             lateMergePrev.show(40)
             
             lateMergePrev.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "LateMergePrev/") 
             
             val lateMergeQuery = s""" SELECT adbw.warehouse_id
                                                  ,adbw.ops_date
                                                  ,adbw.load_number
                                                  ,adbw.stop_number
                                                  ,adbw.assignment_id
                                                  ,adbw.ext_timestamp assgn_ext_timestamp
                                                  ,hour(adbw.ext_timestamp) assgn_ext_timestamp_hr
                                                  ,am.ext_timestamp merged_ext_timestamp
                                                  ,hour(am.ext_timestamp) merged_ext_timestamp_hr
                                                  ,adbw.snapshot_id assgn_snapshot_id
                                                  ,am.snapshot_id merged_snapshot_id
                                                  ,adbw.bill_qty
                                                  ,adbw.cases_selected
                                                  ,adbw.scr_qty
                                            FROM assignment_details_by_warehouse adbw
                                                ,assignments_merged am
                                           WHERE adbw.warehouse_id = am.invoice_site
                                             AND adbw.ops_date = am.ops_date
                                             AND adbw.assignment_id = am.assgn_nbr
                                             AND adbw.warehouse_id = '29'
                                             AND adbw.ops_date = '2017-05-10'
                                             AND hour(adbw.ext_timestamp) <> hour(am.ext_timestamp)
                                             AND merged_status = 'N'
                                        ORDER BY warehouse_id, ops_date, load_number, stop_number, assignment_id
                                          """
             logger.debug( " - Query - " + lateMergeQuery );
             val lateMerge = sqlContext.sql(lateMergeQuery)     
             logger.debug( " - Query fired " ) 
             lateMerge.show(40)
             
             lateMerge.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "LateMerge/")              
       }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing mergedDiffTime at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def getMissingMergedAssgn(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "               getMissingMergedAssgn                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      //sqlContext.udf.register("timeDiff", time_delta _)      
      val warehouseId = "'29','76'" 
      val opsDate = addDaystoCurrentDatePv(-2)
      val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
      val endTime =  getCurrentDate       
      
       try{
             //                                             AND validation_status IN ( 'Fully_Complete' )
         //,datediff( from_unixtime(unix_timestamp( update_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss'), from_unixtime(unix_timestamp( creation_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss')  ) * (24*60*60) update_time_diff_sec
         
         
             val missingMergedAssgnQuery = s""" SELECT adwh.warehouse_id
                                             ,adwh.ops_date
                                             ,adwh.load_number
                                             ,adwh.stop_number
                                             ,adwh.assignment_id
                                             ,adwh.merged_status
                                             ,adwh.snapshot_id
                                             ,adwh.ext_timestamp
                                             ,adwh.update_time
                                             ,adwh.closed_timestamp
                                             ,adwh.assignment_type
                                             ,adwh.bill_qty
                                             ,adwh.cases_selected
                                             ,adwh.scr_qty
                                             ,adwh.door_number
                                             ,adwh.customer_number
                                             ,adwh.customer_name
                                             ,am.ext_timestamp merge_ext_timestamp
                                             ,am.snapshot_id merge_snapshot_id
                                            FROM assignment_details_by_warehouse adwh
                                           LEFT OUTER JOIN assignments_merged am
                                                        ON am.invoice_site = adwh.warehouse_id
                                                           AND am.ops_date = adwh.ops_date
                                                           AND am.assgn_nbr = adwh.assignment_id
                                           WHERE adwh.warehouse_id IN ( $warehouseId )
                                             AND adwh.ops_date >= '$startTime'
                                             AND adwh.ops_date <= '$endTime'
                                             AND adwh.update_time < '2017-06-09 10:25:00' 
                                             AND adwh.merged_status = 'N'
                                        ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.load_number, adwh.stop_number, adwh.assignment_id
                                          """
             
             logger.debug( " - Query - " + missingMergedAssgnQuery );
             val missingMergedAssgn = sqlContext.sql(missingMergedAssgnQuery)     
             logger.debug( " - Query fired " ) 
             missingMergedAssgn.show(40)
             
             missingMergedAssgn.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "MissingMergedAssgn/") 
             
             /*val missingMergedAssgnPrevQuery = s""" SELECT adwh.warehouse_id
                                             ,adwh.ops_date
                                             ,adwh.load_number
                                             ,adwh.stop_number
                                             ,adwh.assignment_id
                                             ,adwh.merged_status
                                             ,adwh.snapshot_id
                                             ,adwh.ext_timestamp
                                             ,adwh.update_time
                                             ,adwh.closed_timestamp
                                             ,adwh.assignment_type
                                             ,adwh.bill_qty
                                             ,adwh.cases_selected
                                             ,adwh.scr_qty
                                             ,adwh.door_number
                                             ,adwh.customer_number
                                             ,adwh.customer_name
                                             ,am.ext_timestamp merge_ext_timestamp
                                             ,am.snapshot_id merge_snapshot_id
                                            FROM assignment_details_by_warehouse_prev adwh
                                           LEFT OUTER JOIN assignments_merged am
                                                        ON am.invoice_site = adwh.warehouse_id
                                                           AND am.ops_date = adwh.ops_date
                                                           AND am.assgn_nbr = adwh.assignment_id
                                           WHERE adwh.warehouse_id = $warehouseId
                                             AND adwh.ops_date >= $startTime
                                             AND adwh.ops_date <= $endTime
                                             AND adwh.update_time < '2017-05-11 09:00:00' 
                                             AND adwh.merged_status = 'N'
                                        ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.load_number, adwh.stop_number, adwh.assignment_id
                                          """
             
             logger.debug( " - Query - " + missingMergedAssgnPrevQuery );
             val missingMergedAssgnPrev = sqlContext.sql(missingMergedAssgnPrevQuery)     
             logger.debug( " - Query fired " ) 
             missingMergedAssgnPrev.show(40)
             
             missingMergedAssgnPrev.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "MissingMergedAssgnPrev/") */
             //                                             ,adwh.update_time
             val assignmentWronglyMarkedMergedQuery = s""" SELECT DISTINCT adwh.warehouse_id
                                                                 ,adwh.ops_date
                                                                 ,adwh.load_number
                                                                 ,adwh.stop_number
                                                                 ,adwh.assignment_id
                                                                 ,adwh.merged_status
                                                                 ,adwh.snapshot_id
                                                                 ,adwh.ext_timestamp
                                                                 ,adwh.closed_timestamp
                                                                 ,adwh.assignment_type
                                                                 ,adwh.bill_qty
                                                                 ,adwh.cases_selected
                                                                 ,adwh.scr_qty
                                                                 ,adwh.door_number
                                                                 ,adwh.customer_number
                                                                 ,adwh.customer_name 
                                                             FROM assignment_details_by_warehouse_history adwh
                                                             LEFT OUTER JOIN assignments_merged am
                                                                    ON am.invoice_site = adwh.warehouse_id
                                                                       AND am.ops_date = adwh.ops_date
                                                                       AND am.assgn_nbr = adwh.assignment_id
                                                             WHERE adwh.warehouse_id IN ( $warehouseId )
                                                               AND adwh.ops_date >= '$startTime'
                                                               AND adwh.ops_date <= '$endTime'
                                                               AND adwh.merged_status = 'Y'
                                                               AND am.assgn_nbr IS NULL
                                                          ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.load_number, adwh.stop_number, adwh.assignment_id
                                                  """
             
             /*logger.debug( " - Query - " + assignmentWronglyMarkedMergedQuery );
             val assignmentWronglyMarkedMerged = sqlContext.sql(assignmentWronglyMarkedMergedQuery)     
             logger.debug( " - Query fired " ) 
             assignmentWronglyMarkedMerged.show(40)
             
             assignmentWronglyMarkedMerged.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "WronglyMarkedMerged/")     */                                                     
             /*val missingMergedAssgnQuery1 = s""" SELECT fst.file_name
                                             ,fst.ext_timestamp
                                             ,fst.snapshot_id
                                             ,adwh.warehouse_id
                                             ,adwh.ops_date
                                             ,adwh.load_number
                                             ,adwh.stop_number
                                             ,adwh.assignment_id
                                             ,adwh.merged_status
                                             ,adwh.snapshot_id
                                             ,adwh.ext_timestamp
                                             ,adwh.update_time
                                             ,adwh.closed_timestamp
                                             ,adwh.assignment_type
                                             ,adwh.bill_qty
                                             ,adwh.cases_selected
                                             ,adwh.scr_qty
                                             ,adwh.door_number
                                             ,adwh.customer_number
                                             ,adwh.customer_name
                                             ,LEAD(assignment_id, 1, 0 ) OVER ( PARTITION BY adwh.warehouse_id, adwh.ops_date, adwh.assignment_id 
                                                                                    ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.assignment_id, fst.ext_timestamp
                                                                              ) next_assignment_id
                                            FROM file_streaming_tracker fst
                                          LEFT OUTER JOIN assignment_details_by_warehouse_history adwh
                                                       ON fst.snapshot_id = adwh.snapshot_id
                                                      AND fst.ext_timestamp = adwh.ext_timestamp
                                           WHERE fst.file_id = 1
                                             AND adwh.warehouse_id = '44'
                                             AND adwh.ops_date >= '2017-05-03'
                                             AND adwh.ops_date <= '2017-05-04'
                                             AND adwh.merged_status = 'N'
                                        ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.assignment_id, fst.file_name, fst.ext_timestamp
                                          """
             
             logger.debug( " - Query - " + missingMergedAssgnQuery1 );
             val missingMergedAssgn1 = sqlContext.sql(missingMergedAssgnQuery1)     
             logger.debug( " - Query fired " ) 
             missingMergedAssgn1.show(40)
             
             missingMergedAssgn.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "MissingMergedAssgn/") */             
             
       }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getMissingMergedAssgn at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def missingLoadsAssignments(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "               missingLoadsAssignments              " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      //sqlContext.udf.register("timeDiff", time_delta _)      
       try{
             //                                             AND validation_status IN ( 'Fully_Complete' )
         //,datediff( from_unixtime(unix_timestamp( update_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss'), from_unixtime(unix_timestamp( creation_time, 'yyyyMMdd HHmmss' ), 'yyyyMMdd HHmmss')  ) * (24*60*60) update_time_diff_sec
         
         
             val missingMergedAssgnQuery = s""" SELECT ldbw.warehouse_id
                                             ,adwh.ops_date
                                             ,adwh.load_number
                                             ,adwh.stop_number
                                             ,adwh.assignment_id
                                             ,adwh.merged_status
                                             ,adwh.snapshot_id
                                             ,adwh.ext_timestamp
                                             ,adwh.update_time
                                             ,ldbw.closed_timestamp load_closed_timestamp
                                             ,adwh.closed_timestamp assgn_closed_timestamp
                                             ,am.ext_timestamp merge_ext_timestamp
                                             ,adwh.assignment_type
                                             ,adwh.bill_qty
                                             ,adwh.cases_selected
                                             ,adwh.scr_qty
                                             ,adwh.door_number
                                             ,adwh.customer_number
                                             ,adwh.customer_name
                                             ,am.snapshot_id merge_snapshot_id
                                            FROM assignment_details_by_warehouse adwh
                                            JOIN load_details_by_warehouse ldbw
                                                 ON ldbw.warehouse_id = adwh.warehouse_id
                                                 AND ldbw.ops_date = adwh.ops_date
                                                 AND ldbw.load_number = adwh.load_number
                                           LEFT OUTER JOIN assignments_merged am
                                                        ON am.invoice_site = adwh.warehouse_id
                                                           AND am.ops_date = adwh.ops_date
                                                           AND am.assgn_nbr = adwh.assignment_id
                                           WHERE adwh.warehouse_id = '29'
                                             AND adwh.ops_date = '2017-05-10'
                                             AND ldbw.load_number IN ( '000000059401','000000059409','000000059410','000000059418','000000059433','000000059438')
                                        ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.load_number, adwh.stop_number, adwh.assignment_id
                                          """
             //AND adwh.merged_status = 'N'
             logger.debug( " - Query - " + missingMergedAssgnQuery );
             val missingMergedAssgn = sqlContext.sql(missingMergedAssgnQuery)     
             logger.debug( " - Query fired " ) 
             missingMergedAssgn.show(40)
             
             missingMergedAssgn.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "MissingMergedAssgnLoad/") 
             
             val missingMergedAssgnPrevQuery = s""" SELECT ldbw.warehouse_id
                                             ,adwh.ops_date
                                             ,adwh.load_number
                                             ,adwh.stop_number
                                             ,adwh.assignment_id
                                             ,adwh.merged_status
                                             ,adwh.snapshot_id
                                             ,adwh.ext_timestamp
                                             ,adwh.update_time
                                             ,ldbw.closed_timestamp load_closed_timestamp
                                             ,adwh.closed_timestamp assgn_closed_timestamp
                                             ,am.ext_timestamp merge_ext_timestamp
                                             ,adwh.assignment_type
                                             ,adwh.bill_qty
                                             ,adwh.cases_selected
                                             ,adwh.scr_qty
                                             ,adwh.door_number
                                             ,adwh.customer_number
                                             ,adwh.customer_name
                                             ,am.snapshot_id merge_snapshot_id
                                            FROM assignment_details_by_warehouse_prev adwh
                                            JOIN load_details_by_warehouse ldbw
                                                 ON ldbw.warehouse_id = adwh.warehouse_id
                                                 AND ldbw.ops_date = adwh.ops_date
                                                 AND ldbw.load_number = adwh.load_number
                                           LEFT OUTER JOIN assignments_merged am
                                                        ON am.invoice_site = adwh.warehouse_id
                                                           AND am.ops_date = adwh.ops_date
                                                           AND am.assgn_nbr = adwh.assignment_id
                                           WHERE adwh.warehouse_id = '29'
                                             AND adwh.ops_date = '2017-05-10'
                                             AND ldbw.load_number IN ( '000000059401','000000059409','000000059410','000000059418','000000059433','000000059438')
                                        ORDER BY adwh.warehouse_id, adwh.ops_date, adwh.load_number, adwh.stop_number, adwh.assignment_id
                                          """
             
             logger.debug( " - Query - " + missingMergedAssgnPrevQuery );
             val missingMergedAssgnPrev = sqlContext.sql(missingMergedAssgnPrevQuery)     
             logger.debug( " - Query fired " ) 
             missingMergedAssgnPrev.show(40)
             
             missingMergedAssgnPrev.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
             .mode("overwrite")
             .save(filePath + "MissingMergedAssgnLoadPrev/") 
                    
             
       }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing missingLoadsAssignments at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
   def extTimeStampCheck(sqlContext: SQLContext, dfFileStreamin: DataFrame) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  extTimeStampCheck                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
    
       val currentTime = getCurrentTime()
       try{
             val pendingFilesExtTimes = dfFileStreamin
                                    .select( dfFileStreamin("creation_time"), dfFileStreamin("file_name"), dfFileStreamin("file_id"), dfFileStreamin("validation_status") )
                                    .filter(s"file_id = 1 AND validation_status = 'Ingested' ")
                                    .orderBy( dfFileStreamin("creation_time") )
                                    .limit(1)
                                    .select( "file_name" , "creation_time" )
                                    .withColumn("ext_timestamp", expr(" SUBSTR(file_name, INSTR(file_name,'.') + 1, 12 ) ") )
                                    .withColumn("unix_timestamp", expr(" unix_timestamp( SUBSTR(file_name, INSTR(file_name,'.') + 1, 12 ), 'yyyyMMddHHmm' ) ") )
                                    .withColumn("from_unix_timestamp", expr(" from_unixtime( unix_timestamp( SUBSTR(file_name, INSTR(file_name,'.') + 1, 12 ), 'yyyyMMddHHmm' ) )") )
                                    .withColumn("ext_timestamp", expr(" SUBSTR(file_name, INSTR(file_name,'.') + 1, 12 ) ") )
                                    //.withColumn("dot_position", expr( " INSTR(file_name,'.') " ) )
                                    //.withColumn("csv_position", expr( " INSTR(file_name,'CSV') " ) )
                                    //.withColumn("length_filename", expr( " LENGTH(file_name) " ) )
                                   .collect()
         
         //pendingFilesExtTimes.show()
         pendingFilesExtTimes.foreach { println }
         
         val extDateTime = pendingFilesExtTimes.map { x => x.getString(4) }
                           .mkString
                       
         logger.debug( "\n" + "extDateTime - " + extDateTime + "\n" + " - currentTime - " + currentTime )
         val fileIngestionQuery = s""" SELECT CASE WHEN '$currentTime' > '$extDateTime' THEN
                                                        'Current Time is greater'
                                                   ELSE 'Current Time is smaller' 
                                              END time_comnparison
                                         FROM file_ingestion_controller
                                   """
         
         logger.debug( " - Query - " + fileIngestionQuery );
         val fileIngestion = sqlContext.sql(fileIngestionQuery)     
         logger.debug( " - Query fired " ) 
         fileIngestion.show(40)
             
       }catch{
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
            logger.error("Error while executing extTimeStampCheck at : " +  e.getMessage + " " + e.printStackTrace() )
       }
   }
   
   def extTimeStampTrimMS(sqlContext: SQLContext, dfFileStreaming: DataFrame) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  extTimeStampTrimMS                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
    
       val currentTime = getCurrentTime()
       try{
              val maxFilesExtTimes = dfFileStreaming.select( dfFileStreaming("ext_timestamp"), dfFileStreaming("file_name"), dfFileStreaming("snapshot_id") )
                       .orderBy( dfFileStreaming("ext_timestamp").desc )
                       .limit(1)
                       .map { x => Row( if ( x.get(0) == null ) null else x.getTimestamp(0).toString().split("\\.")(0), x.getString(1), x.getString(2), convertToLong(x.getTimestamp(0).toString()) ) }   //
                       //.collect()
         
             maxFilesExtTimes.foreach { println }
             val extDateTime = maxFilesExtTimes.first().getString(0)
             val extDateTimeLng = maxFilesExtTimes.first().getLong(3)
             logger.debug( "extDateTime - " + extDateTime + "\n"
                          + " - extDateTimeLng - " + extDateTimeLng
                 )
             
       }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing extTimeStampTrimMS at : " +  e.getMessage + " " + e.printStackTrace() )
       }
   }   
   
 def checkNull(qty: Long): String = {
      /*var qtyLong = None: Option[Long]
      qtyLong = Some(qty)
      val result = qtyLong.getOrElse(-999)
      if ( result == -999 )  
         return "1"
      else "0"*/
    
    logger.debug( " Inside checkNull for - " + qty.toString() )
    try {
        val result = qty.toString()
        if ( result.equals(null) || result.equals("") ) {
          return "1"
        }else{
          return "0"
        }
    }catch {
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException | _:java.lang.NullPointerException  ) => 
         logger.debug("qty is null - " + qty )
         "1"
    }
  }
    
  def getPreviousOpsDate(opsDate: Timestamp, numberOfDays: Int) : java.sql.Timestamp = {
    //val now = Calendar.getInstance()
    //dateAdd.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")   
    val secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    return new java.sql.Timestamp(sdf.parse(sdf.format(opsDate.getTime + secondsAdd)).getTime)
  }
  
  def prevDayMergedAssignments(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "             prevDayMergedAssignments               " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    sqlContext.udf.register( "getPreviousOpsDate", getPreviousOpsDate _)
    try{ 

         val warehouseOperationDateQuery = s""" SELECT * 
                                                  FROM warehouse_operation_time wot
                                                 WHERE '$currentTime' BETWEEN start_time AND end_time
                                              """
         logger.debug( " - warehouseOperationDateQuery - " + warehouseOperationDateQuery )
         val warehouseOperationDate = sqlContext.sql(warehouseOperationDateQuery)
         warehouseOperationDate.show()   
         
         warehouseOperationDate.registerTempTable("current_ops_date")
         
         val assgnMergedSnapshot = s""" SELECT am.invoice_site warehouse_id  
                                              ,am.ops_date merged_ops_date
                                              ,adbw.ops_date asgn_ops_date
                                              ,adbw.load_number, adbw.stop_number
                                              ,am.load_nbr, adbw.assignment_id
                                              ,adbw.merged_status
                                              ,adbw.customer_name, adbw.customer_number
                                              ,adbw.bill_qty, adbw.cases_selected, adbw.scr_qty
                                              ,adbw.number_of_cubes
                                              ,am.snapshot_id merged_snapshot_id
                                              ,adbw.snapshot_id asgn_snapshot_id
                                              ,am.extr_date, am.extr_time, am.print_site
                                              ,am.ext_timestamp, am.creation_time
                                       FROM assignment_details_by_warehouse adbw  
                                       JOIN assignments_merged am
                                         ON adbw.ops_date <> am.ops_date
                                            AND adbw.warehouse_id = am.invoice_site
                                            AND adbw.assignment_id = am.assgn_nbr
                                       JOIN current_ops_date cwod
                                         ON adbw.warehouse_id = cwod.facility           
                                            AND adbw.ops_date BETWEEN getPreviousOpsDate( cwod.ops_date, -6 ) AND cwod.ops_date
                                   ORDER BY warehouse_id, adbw.ops_date, am.ops_date, adbw.load_number, adbw.stop_number, adbw.assignment_id, adbw.update_time
                                      """   
         /*
          *                                       WHERE adbw.warehouse_id IN ('76')
                                        AND am.ops_date >= '2017-03-17'
                                        AND am.ops_date <= '2017-03-25'
          */
         logger.debug( " - Query - " + assgnMergedSnapshot );
         val queryData = sqlContext.sql(assgnMergedSnapshot)     
         logger.debug( " - Query fired " ) 
         queryData.show(40)
         
         queryData.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "MergedAssignments") 

    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing prevDayMergedAssignments at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def execRandomQuery(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  execRandomQuery                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    sqlContext.udf.register( "getPreviousOpsDate", getPreviousOpsDate _)
    try{ 
         /*val randomQuery = s""" SELECT adw.warehouse_id, adw.ops_date, adw.load_number
                                      ,adw.stop_number, adw.assignment_id, adw.merged_status
                                      ,am.load_nbr, am.ops_date, am.ext_timestamp, am.snapshot_id
                                      ,getPreviousOpsDate(am.ops_date, -2) previous_ops_date
                                  FROM assignment_details_by_warehouse adw
                                      ,assignments_merged am
                                  WHERE adw.warehouse_id = '44'
                                   AND adw.ops_date = '2017-03-20'
                                   AND adw.load_number = '000000344591' --''000000069060'
                                   AND adw.stop_number = '002'
                                   AND adw.assignment_id IN ( '077551')
                                   AND adw.warehouse_id = am.invoice_site
                                   AND adw.assignment_id = am.assgn_nbr
                                   AND adw.ops_date BETWEEN getPreviousOpsDate(am.ops_date, -2) AND am.ops_date
                            """*/
         
         val randomQuery = s"""SELECT *
                                 FROM assignment_details_by_warehouse
                                WHERE warehouse_id = '29'
                                  AND assignment_id = '896124' """
         logger.debug( " - randomQuery - " + randomQuery );
         val randomExec = sqlContext.sql(randomQuery)     
         logger.debug( " - Query fired " ) 
         randomExec.show(40)
         
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing execRandomQuery at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def updateData(sqlContext: SQLContext, sc: SparkContext, dfUpdateDF: DataFrame) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      updateData                    " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{ 
         val fileStreamingRDD = dfUpdateDF
                               //.filter("file_id = 1 AND ext_timestamp > '2017-08-06 10:58:01' AND validation_status = 'Ingested-Hold'")  //
                               .filter("file_id IN (1,2) AND creation_time < '2017-11-13 00:04:52' AND validation_status = 'Ingested'")
                               .select("file_id", "file_name", "validation_status")
                               .map { x => Row( x.getInt(0), x.getString(1), "Ingested-Hold" ) }
         //fileStreamingRDD.foreach { println }
         
         val fileStreamingDS = new StructType( Array( StructField("file_id", IntegerType, nullable=false), 
                                                      StructField("file_name", StringType, nullable=false),
                                                      StructField("validation_status", StringType, nullable=true)
                                                 )
                                         )
         val fileStreaming = sqlContext.createDataFrame(fileStreamingRDD, fileStreamingDS )
         //fileStreaming.foreach { println }
         
         fileStreaming
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "wip_configurations")
        .option("table", "file_streaming_tracker")
        .mode("append").save()
        
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing updateData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def deleteAssignmentsCheck(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   deleteAssignmentsCheck           " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{ 
         val loadsOpenCloseQuery = s"""SELECT DISTINCT adbw.warehouse_id, adbw.ops_date ops_date_format_unix
                                          ,adbw.load_number,customer_number
                                          ,customer_name, closed_timestamp
                             FROM assignment_details_by_warehouse_prev adbw
                            WHERE ops_date = '2017-02-22'
                              AND warehouse_id ='29'
                          ORDER BY warehouse_id, ops_date_format_unix DESC, load_number"""
           
         logger.debug( " - loadsOpenCloseQuery - " + loadsOpenCloseQuery );
         val loadsOpenClose = sqlContext.sql(loadsOpenCloseQuery)     
         logger.debug( " - Query fired " ) 
         loadsOpenClose.show(40)
                  
         loadsOpenClose.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "LoadOpenClose/Loads") 
         
         val assgnOpenCloseQuery = s""" WITH base_row AS (
                                         SELECT  DISTINCT adbw.warehouse_id, adbw.ops_date
                                                        ,adbw.load_number,customer_number
                                                        ,customer_name
                                               ,COUNT( DISTINCT ( CASE WHEN closed_timestamp IS NULL THEN 1 else 0 END ) ) distinct_closed_timestamp
                                           FROM assignment_details_by_warehouse_prev adbw
                                          WHERE ops_date IN ('2017-02-14 00:00:00','2017-02-13 00:00:00')
                                          GROUP BY adbw.warehouse_id, adbw.ops_date
                                                        ,adbw.load_number,customer_number
                                                        ,customer_name
                                                  ) 
                                        SELECT DISTINCT adbw.*
                                          FROM base_row br
                                              ,assignment_details_by_warehouse_prev adbw
                                         WHERE br.warehouse_id = adbw.warehouse_id
                                           AND br.ops_date = adbw.ops_date
                                           AND br.load_number = adbw.load_number
                                           AND distinct_closed_timestamp > 1
                                    ORDER BY adbw.load_number, customer_number, customer_name
                                                """
         logger.debug( " - assgnOpenCloseQuery - " + assgnOpenCloseQuery );
         val assgnOpenClose = sqlContext.sql(assgnOpenCloseQuery)     
         logger.debug( " - Query fired 1 " ) 
         assgnOpenClose.show(40)
         assgnOpenClose.registerTempTable("assgn_open_close")
                  
         assgnOpenClose.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "LoadOpenClose/Assignments")    
         
         val mergedAssignmentsQuery = s"""SELECT aoc.warehouse_id
                                                ,aoc.ops_date
                                                ,aoc.load_number
                                                ,aoc.stop_number
                                                ,aoc.assignment_id
                                                ,aoc.assgn_start_time
                                                ,aoc.assgn_end_time
                                                ,aoc.closed_timestamp
                                                ,aoc.ext_timestamp
                                                ,aoc.assignment_type
                                                ,aoc.bill_qty
                                                ,aoc.cases_selected
                                                ,aoc.scr_qty
                                                ,aoc.number_of_cubes
                                                ,aoc.customer_number
                                                ,aoc.customer_name
                                                ,aoc.merged_status
                                                ,aoc.is_load_deferred
                                                ,aoc.snapshot_id
                                                ,aoc.update_time
                                                ,am.load_nbr merged_load_number
                                                ,am.assgn_nbr merged_assgn_nbr
                                                ,am.ext_timestamp merged_ext_timestamp
                                                ,am.snapshot_id merged_snapshot_id
                                                ,am.update_time merged_update_time
                                            FROM assgn_open_close aoc 
                                            LEFT OUTER JOIN assignments_merged am
                                             ON aoc.warehouse_id = am.invoice_site
                                              AND aoc.ops_date = am.ops_date
                                              AND aoc.assignment_id = am.assgn_nbr
                                         """
         logger.debug( " - mergedAssignmentsQuery - " + mergedAssignmentsQuery );
         val mergedAssignments = sqlContext.sql(mergedAssignmentsQuery)     
         logger.debug( " - Query fired 2 " ) 
         mergedAssignments.show(40)
                  
         mergedAssignments.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "LoadOpenClose/MergeAssignments")      
         
         val openLoadsQuery =  s""" SELECT DISTINCT adbw.warehouse_id
                                          ,adbw.ops_date
                                          ,adbw.customer_number
                                          ,adbw.customer_name
                                          ,adbw.load_number
                                          ,adbw.closed_timestamp
                                          ,adbw.is_load_deferred
                                          ,adbw1.load_number notnull_load_number
                                      FROM assignment_details_by_warehouse_prev adbw
                                      JOIN load_details_by_warehouse ldbw
                                        ON ldbw.warehouse_id = adbw.warehouse_id
                                          AND ldbw.ops_date = adbw.ops_date
                                          AND ldbw.load_number = adbw.load_number
                                      LEFT OUTER JOIN assignment_details_by_warehouse_prev adbw1
                                        ON adbw1.warehouse_id = adbw.warehouse_id
                                          AND adbw1.ops_date = adbw.ops_date
                                          AND adbw1.load_number = adbw.load_number
                                          AND adbw1.closed_timestamp IS NOT NULL
                                     WHERE adbw.customer_number IS NOT NULL
                                       AND ( adbw.is_load_deferred = 'N' OR adbw.is_load_deferred IS NULL )
                                       AND adbw.closed_timestamp IS NULL 
                                       AND adbw1.load_number IS NULL
                                       AND adbw.warehouse_id = '29'
                                       AND adbw.ops_date = '2017-02-22'
                                """         
         logger.debug( " - openLoadsQuery - " + openLoadsQuery );
         val openLoads = sqlContext.sql(openLoadsQuery)     
         logger.debug( " - Query fired 2 " ) 
         openLoads.show(40)
                  
         openLoads.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "LoadOpenClose/OpenLoads")      
         
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing deleteAssignmentsCheck at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
    
  def getABACapacity(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      getABACapacity                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{
        val abaCapacityQuery = s""" SELECT acb.warehouse_id as whse_nbr
                                           ,acb.ops_date
                                           ,acb.interval_hour
                                           ,acb.aba_capacity
                                           ,wot.start_hour  
                                           ,CASE WHEN wot.start_hour = interval_hour THEN 
                                                      -1
                                                 WHEN ( wot.start_hour > interval_hour 
                                                       OR interval_hour == 0 ) THEN
                                                       24 + interval_hour
                                                 ELSE interval_hour
                                             END orderby_col
                                      FROM aba_capacity_by_opsday acb
                                          ,warehouse_operation_time wot
                                     WHERE acb.warehouse_id = wot.warehouse_id
                                       AND acb.ops_date = wot.ops_date 
                                       AND '$currentTime' BETWEEN wot.start_time AND wot.end_time
                                  ORDER BY orderby_col
                                """
         logger.debug(" - abaCapacityQuery - " + abaCapacityQuery )
         val abaCapacity = sqlContext.sql(abaCapacityQuery) 
         abaCapacity.show()
                  
         
         val abaCapacityGrouped = 
           abaCapacity
         .map( x => Row( x.getString(0), x.getTimestamp(1), x.getInt(2), x.getDouble(3), x.getInt(4), x.getInt(5) ) )
         .groupBy(x => ( x.getString(0), x.getTimestamp(1) ) )
         abaCapacityGrouped.foreach(println)
         
         val ABACapacityRowMap = abaCapacityGrouped.map(f => Row( f._1._1
                                                       ,f._1._2
                                                       ,f._2.map { x => Map( x.getInt(5) -> (x.getInt(2)) )
                                                                 }
                                                       ,f._2.map { x => Map( x.getInt(5) -> (x.getDouble(3)) )
                                                                 }
                                                      )
                                            )
                                            
        /*val ABACapacityRowMap = abaCapacityGrouped.map(f => Row( f._1._1
                                                       ,f._1._2
                                                       ,f._2.map { x => Map( x.getInt(5) -> (x.getInt(2)) )
                                                                       .toSeq.sortBy(_._1)
                                                                       .flatMap( f => Seq(f._2.toString() ) )// Array(Row( (f._2._1), f._2._2 ) ))
                                                                 }.flatten
                                                       ,f._2.map { x => Map( x.getInt(5) -> (x.getDouble(3)) )
                                                                       .toSeq.sortBy(_._1)
                                                                       .flatMap( f =>  Seq(f._2.toString() ) )
                                                                 }.flatten
                                                      )
                                            )*/
      logger.debug(" - ABACapacityRowMap - ")                      
      ABACapacityRowMap.foreach { println }
      
      val ABACapacityFinal = ABACapacityRowMap.map { x => Row( x.getString(0)
                                                              ,x.getTimestamp(1)
                                                              ,x.getList(2).asScala.toList
                                                              ,x.getList(3).asScala.toList
                                                              )}      
        
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getABACapacity at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
                                            //
                                            //AND update_time < '2017-02-22 03:00:00'
         /*
          * AND snapshot_id IN ('be18fcb0-f88b-11e6-ac16-9dc40a17569e'
                                                                ,'014e8140-f890-11e6-ac16-9dc40a17569e'
                                                                ,'14c9b330-f894-11e6-ac16-9dc40a17569e'
                                                                ,'5ff136e0-f898-11e6-ac16-9dc40a17569e'
                                                                ,'59a09970-f89d-11e6-ac16-9dc40a17569e'
                                                                )
          */  
  
  def getData(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      getData                       " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'29'"
    val opsDate = addDaystoCurrentDatePv(-2)
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate  //getPreviousDate(1)
    val loadNumber = "'000000059617'"  //,'000000059618'
    
    logger.debug( "warehouseId - " + warehouseId + "\n"
               + "opsDate - " + opsDate + "\n"
               + "startTime - " + startTime + "\n"
               + "endTime - " + endTime + "\n"
               + "loadNumber - " + loadNumber
               ) 
    try{     
      /*
       *                                     WHERE warehouse_id IN ($warehouseId)
                                      AND ops_date = '$opsDate'
                                      AND load_number = $loadNumber
       */
       val loadDetailsQuery = s""" SELECT * 
                                     FROM load_details_by_warehouse 
                                """
         
       val stopDetailsQuery = s""" SELECT * 
                                     FROM stop_details_by_warehouse 
                               """
                                      
       val assignmentDetailsQuery = s""" SELECT * 
                                     FROM assignment_details_by_warehouse 
                                """                                      
       
                                      //AND ops_date = '$opsDate'
       val assignmentMergedQuery = s""" SELECT * 
                                           FROM assignments_merged 
                                          WHERE snapshot_id = '0915e080-47b9-11e7-98ae-bddb3bd318f0'
                               """     
       
       val completeDetailsQuery = s""" SELECT ldbw.warehouse_id
                                        ,ldbw.ops_date
                                        ,ldbw.customer_number
                                        ,ldbw.customer_name
                                        ,ldbw.load_number
                                        ,ldbw.cube_qty
                                        ,ldbw.total_cases
                                        ,ldbw.actual_pallets
                                        ,ldbw.expected_pallets
                                        ,ldbw.selected_pct
                                        ,ldbw.loaded_pct
                                        ,ldbw.closed_timestamp
                                        ,ldbw.set_back_timestamp
                                        ,ldbw.dispatch_timestamp
                                        ,ldbw.ext_timestamp
                                        ,ldbw.gate_status
                                        ,ldbw.loader_employee_id
                                        ,ldbw.loader_name
                                        ,ldbw.trailer_type
                                        ,ldbw.store_stops
                                        ,sdbw.stop_number
                                        ,sdbw.store_number
                                        ,sdbw.store_name
                                        ,sdbw.cs_customer_number
                                        ,sdbw.cs_customer_name
                                        ,sdbw.cases_selected
                                        ,sdbw.actual_pallets
                                        ,sdbw.expected_pallets
                                        ,sdbw.selected_pct
                                        ,sdbw.loaded_pct
                                        ,concat( sdbw.store_address, ' ', sdbw.store_city, ' ' , sdbw.store_state, ' ', sdbw.store_zip ) store_address
                                        ,adbw.assignment_id
                                        ,adbw.assgn_start_time
                                        ,adbw.assgn_end_time
                                        ,adbw.bill_qty
                                        ,adbw.cases_selected
                                        ,adbw.scr_qty
                                        ,adbw.number_of_cubes
                                        ,adbw.employee_id
                                        ,adbw.employee_name
                                        ,adbw.is_load_deferred
                                        ,adbw.selected_pct
                                        ,adbw.snapshot_id
                                    FROM load_details_by_warehouse ldbw
                                        ,stop_details_by_warehouse sdbw
                                        ,assignment_details_by_warehouse adbw
                                  WHERE ldbw.warehouse_id = sdbw.warehouse_id
                                    AND ldbw.ops_date = sdbw.ops_date
                                    AND ldbw.load_number = sdbw.load_number
                                    AND sdbw.warehouse_id = adbw.warehouse_id
                                    AND sdbw.ops_date = adbw.ops_date
                                    AND sdbw.load_number = adbw.load_number
                                    AND sdbw.stop_number = adbw.stop_number
                                    AND adbw.merged_status = 'N'
                                  """
                                      /*
                                       * snapshot_id = '9dc72720-47a5-11e7-98ae-bddb3bd318f0'
                                       invoice_site IN ($warehouseId)
                                            AND load_nbr IN ('000000059617' ,'000000000000', '000000244262')
                                            AND assgn_nbr IN  ('045261')
                                       */
       logger.debug( " - Query - " + loadDetailsQuery );
       val loadDetails = sqlContext.sql(loadDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       loadDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "LoadDetails/") 
       
       logger.debug( " - Query - " + stopDetailsQuery );
       val stopDetails = sqlContext.sql(stopDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       stopDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "StopDetails/") 
       
       logger.debug( " - Query - " + assignmentDetailsQuery );
       val assignmentDetails = sqlContext.sql(assignmentDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       assignmentDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "AssignmentDetails/")  
       
       logger.debug( " - Query - " + completeDetailsQuery );
       val completeDetails = sqlContext.sql(completeDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       completeDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "CompleteDetails/")        
                                          
       /*logger.debug( " - Query - " + assignmentMergedQuery );
       val assignmentMerged = sqlContext.sql(assignmentMergedQuery)     
       logger.debug( " - Query fired " ) 
       assignmentMerged.show(40)
       
       /*assignmentMerged.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "AssignmentDetails/") */
       */
       logger.debug("Process Completed")
       
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def verifyData(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      verifyData                    " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val previousDate =  addDaystoCurrentDatePv(-1)  
    val warehouseId = "'29','76','44','14'"
    val startTime = addDaystoCurrentDatePv(-1)   //addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate  // getCurrentDate  //getPreviousDate(1)
    
    logger.debug( "warehouseId - " + warehouseId + "\n"
               + "startTime - " + startTime + "\n"
               + "endTime - " + endTime ) 
               
    var existingRows = new ListBuffer[Row]()           
    try{         
        val loadDetailsQuery = s""" SELECT * 
                                     FROM load_details_by_warehouse 
                                    WHERE warehouse_id IN ($warehouseId)
                                      AND ops_date >= '$previousDate'
                                """
         
       val stopDetailsQuery = s""" SELECT * 
                                     FROM stop_details_by_warehouse 
                                    WHERE warehouse_id IN ($warehouseId)
                                      AND ops_date >= '$previousDate'
                               """
                                      
       val assignmentDetailsQuery = s""" SELECT * 
                                     FROM assignment_details_by_warehouse 
                                    WHERE warehouse_id IN ($warehouseId)
                                      AND ops_date >= '$previousDate'
                                """                                      
       
      logger.debug( " - Query - " + loadDetailsQuery );
       val loadDetails = sqlContext.sql(loadDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       loadDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "LoadDetails/") 
       
       logger.debug( " - Query - " + stopDetailsQuery );
       val stopDetails = sqlContext.sql(stopDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       stopDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "StopDetails/") 
       
       logger.debug( " - Query - " + assignmentDetailsQuery );
       val assignmentDetails = sqlContext.sql(assignmentDetailsQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       assignmentDetails.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "AssignmentDetails/")  
 
         val existingLoadsQuery = s""" SELECT warehouse_id
                                             ,ops_date
                                             ,customer_number
                                             ,customer_name
                                             ,time_sequence
                                             ,hourly_bills_list
                                             ,cases_selected_list
                                             ,scratch_quantity_list
                                             ,total_scratches_list
                                             ,cases_remaining_list
                                             ,loads_closed_list
                                             ,loads_remaining_list
                                             ,time_sequence_ts
                                         FROM load_details_by_customer
                                        WHERE warehouse_id IN ($warehouseId)
                                          AND ops_date >= '$previousDate' 
                                   """ 
                                        //ops_date >= '$startTime'
                                          //AND ops_date <= '$endTime' 
         logger.debug( " - Query - " + existingLoadsQuery );
         val existingLoads = sqlContext.sql(existingLoadsQuery)   
         //existingLoads.show(40)
         
         val existingData = existingLoads.map { x =>  if ( x.get(4) == null ) null 
                                                     else x.getAs[WrappedArray[String]](4)
                                                     .map { y => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getString(3)
                                                       ,y
                                                       ,if ( x.get(5) == null ) null else x.getAs[WrappedArray[String]](5).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](5).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(6) == null ) null else x.getAs[WrappedArray[String]](6).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](6).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(7) == null ) null else x.getAs[WrappedArray[String]](7).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](7).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(8) == null ) null else x.getAs[WrappedArray[String]](8).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](8).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(9) == null ) null else x.getAs[WrappedArray[String]](9).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](9).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(10) == null ) null else x.getAs[WrappedArray[String]](10).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](10).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) ) 
                                                       ,if ( x.get(11) == null ) null else x.getAs[WrappedArray[String]](11).apply( if (x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](11).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(12) == null ) null else x.getAs[WrappedArray[java.sql.Timestamp]](12).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[java.sql.Timestamp]](12).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                        )
                                                          }
         
                                             }
                  
         logger.debug( " existingData " );

         val existingRowSchema = new StructType( Array( StructField("warehouse_id", StringType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("customer_number", StringType, false ) 
                                                      ,StructField("customer_name", StringType, true ) 
                                                      ,StructField("time_sequence", StringType, false ) 
                                                      ,StructField("hourly_bill", StringType, true ) 
                                                      ,StructField("cases_selected", StringType, true )
                                                      ,StructField("scratch_quantity", StringType, true )
                                                      ,StructField("total_scratches", StringType, true ) 
                                                      ,StructField("cases_remaining", StringType, true ) 
                                                      ,StructField("loads_closed", StringType, true )
                                                      ,StructField("loads_remaining", StringType, true )
                                                      ,StructField("time_sequence_ts", TimestampType, true )
                                                      )
                                     )  

         for ( i <- 0 to ( existingData.count().toInt - 1 ) )  {
             existingRows.++=(
             getRow( existingData.collect().apply(i) )
             )
         }

        logger.debug( " existingRows " );
        //combinedRow.foreach { println }
        //existingRows.foreach { println }
        
         val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), existingRowSchema)
         logger.debug( " - Update Dataframe - " )     
         //existingRowsDF.show(40)
         
         existingRowsDF.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "ExistingData/")
        
                          
      }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing verifyData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def getSnapshot(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      getSnapshot                    " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'81'"
    val startTime = getCurrentDate   //addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  addDaystoCurrentDatePv(1)  // getCurrentDate  //getPreviousDate(1)
    
    logger.debug( "warehouseId - " + warehouseId + "\n"
               + "startTime - " + startTime + "\n"
               + "endTime - " + endTime ) 
               
    var existingRows = new ListBuffer[Row]()           
    try{         
                                        /*AND ops_date >= '2017-03-17'
                                        AND ops_date <= '2017-03-25'*/
         /*
          *                                       AND snapshot_id IN ('19628610-2f17-11e7-a580-ebf0ec05afba', '1232b060-2f15-11e7-a580-ebf0ec05afba', '54349a20-2f1d-11e7-a580-ebf0ec05afba'
                                                           ,'28ad8990-2f20-11e7-a580-ebf0ec05afba', 'dbd34140-2f25-11e7-a580-ebf0ec05afba', '88f3b600-2f28-11e7-a580-ebf0ec05afba'
                                                           ) 
                                                           *                                        UNION
                                      SELECT * 
                                       FROM assignment_details_by_warehouse_history
                                      WHERE warehouse_id = '44'
                                        AND merged_status = 'Y'
                                        AND ops_date >= '2017-05-01'
                                        AND ops_date <= '2017-05-03'
                                                           */
                                                                            
         /*                           UNION     SELECT * 
                                         FROM assignment_details_by_warehouse_history
                                        WHERE warehouse_id IN ( $warehouseId )
                                           AND snapshot_id IN ('f8953370-3438-11e7-a580-ebf0ec05afba')*/
         
         val assgnHistorySnapshot = s""" SELECT adbwh.*, am.ext_timestamp merge_ext_timestamp
                                       FROM assignment_details_by_warehouse_history adbwh
                                       LEFT OUTER JOIN assignments_merged am
                                         ON adbwh.warehouse_id = am.invoice_site
                                            AND adbwh.ops_date = am.ops_date
                                            AND adbwh.assignment_id = am.assgn_nbr
                                      WHERE adbwh.warehouse_id IN ( $warehouseId )
                                        AND adbwh.ops_date >= '$startTime'
                                        AND adbwh.ops_date <= '$endTime'
                                   ORDER BY adbwh.snapshot_id, adbwh.warehouse_id, adbwh.ops_date, adbwh.load_number, adbwh.stop_number, adbwh.assignment_id, adbwh.update_time
                                      """
         //                                             ,time_sequence_ts             
         val existingLoadsQuery = s""" SELECT warehouse_id
                                             ,ops_date
                                             ,customer_number
                                             ,customer_name
                                             ,time_sequence
                                             ,hourly_bills_list
                                             ,cases_selected_list
                                             ,scratch_quantity_list
                                             ,total_scratches_list
                                             ,cases_remaining_list
                                             ,loads_closed_list
                                             ,loads_remaining_list
                                             ,time_sequence_ts
                                         FROM load_details_by_customer
                                        WHERE warehouse_id IN ( $warehouseId )
                                          AND ops_date >= '$startTime'
                                          AND ops_date <= '$endTime' 
                                   """ 
         logger.debug( " - Query - " + existingLoadsQuery );
         val existingLoads = sqlContext.sql(existingLoadsQuery)   
         existingLoads.show(40)
         
         val existingData = existingLoads.map { x =>  if ( x.get(4) == null ) null 
                                                     else x.getAs[WrappedArray[String]](4)
                                                     .map { y => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getString(3)
                                                       ,y
                                                       ,if ( x.get(5) == null ) null else x.getAs[WrappedArray[String]](5).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(6) == null ) null else x.getAs[WrappedArray[String]](6).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(7) == null ) null else x.getAs[WrappedArray[String]](7).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(8) == null ) null else x.getAs[WrappedArray[String]](8).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(9) == null ) null else x.getAs[WrappedArray[String]](9).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(10) == null ) null else x.getAs[WrappedArray[String]](10).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(11) == null ) null else x.getAs[WrappedArray[String]](11).apply( x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(12) == null ) null else x.getAs[WrappedArray[java.sql.Timestamp]](12).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[java.sql.Timestamp]](12).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                        )
                                                          }
         
                                             }
                  
         logger.debug( " existingData " );

         val existingRowSchema = new StructType( Array( StructField("warehouse_id", StringType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("customer_number", StringType, false ) 
                                                      ,StructField("customer_name", StringType, true ) 
                                                      ,StructField("time_sequence", StringType, false ) 
                                                      ,StructField("hourly_bill", StringType, true ) 
                                                      ,StructField("cases_selected", StringType, true )
                                                      ,StructField("scratch_quantity", StringType, true )
                                                      ,StructField("total_scratches", StringType, true ) 
                                                      ,StructField("cases_remaining", StringType, true ) 
                                                      ,StructField("loads_closed", StringType, true )
                                                      ,StructField("loads_remaining", StringType, true )
                                                      ,StructField("time_sequence_ts", TimestampType, true )
                                                      )
                                     )  

         for ( i <- 0 to ( existingData.count().toInt - 1 ) )  {
             existingRows.++=(
             getRow( existingData.collect().apply(i) )
             )
         }

        logger.debug( " existingRows " );
        //combinedRow.foreach { println }
        existingRows.foreach { println }
        
         val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), existingRowSchema)
         logger.debug( " - Update Dataframe - " )     
         existingRowsDF.show(40)
         
         existingRowsDF.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "ExistingData/")
        /*Workbook {
          
        }*/
        //existingData.foreach { x => for ( y <- x ) yield y }
        //existingData.map { x => x.apply(0) }
        //AND LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) IN ('52','07')
         /*
          *                                ,CASE WHEN ( total_remaining_qty - LAG( total_remaining_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) ) < 0 THEN 0
                                      ELSE total_remaining_qty - LAG( total_remaining_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) 
      
                                 END total_remaining_qty_hr
          */
        val calcQuery = s""" WITH base_data AS 
                                  ( SELECT DISTINCT adbwh.warehouse_id
                                   ,adbwh.ops_date
                                   ,adbwh.snapshot_id
                                   ,adbwh.ext_timestamp 
                                   ,adbwh.snapshot_time
                                   ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) ) hour_minute
                                   ,SUM(bill_qty) OVER (PARTITION BY adbwh.warehouse_id
                                                                    ,adbwh.ops_date
                                                                    ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) )
                                                                    ,adbwh.snapshot_time
                                                       ) total_bill_qty
                                   ,SUM(cases_selected) OVER (PARTITION BY adbwh.warehouse_id
                                                                    ,adbwh.ops_date
                                                                    ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) )
                                                                    ,adbwh.snapshot_time 
                                                       ) total_selected_qty
                                   ,SUM(scr_qty) OVER (PARTITION BY adbwh.warehouse_id
                                                                    ,adbwh.ops_date
                                                                    ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) )
                                                                    ,adbwh.snapshot_time 
                                                       ) total_scr_qty
                                   ,SUM(bill_qty) OVER (PARTITION BY adbwh.warehouse_id
                                                                    ,adbwh.ops_date
                                                                    ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) )
                                                                    ,adbwh.snapshot_time 
                                                       ) -
                                           ( SUM(cases_selected) OVER (PARTITION BY adbwh.warehouse_id
                                                                    ,adbwh.ops_date
                                                                    ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) )
                                                                    ,adbwh.snapshot_time 
                                                       )
                                            + SUM(scr_qty) OVER (PARTITION BY adbwh.warehouse_id
                                                                    ,adbwh.ops_date
                                                                    ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) )
                                                                    ,adbwh.snapshot_time
                                                                  )
                                           )
                                           total_remaining_qty
                               FROM assignment_details_by_warehouse_history adbwh
                               LEFT OUTER JOIN assignments_merged am
                                 ON adbwh.warehouse_id = am.invoice_site
                                    AND adbwh.ops_date = am.ops_date
                                    AND adbwh.assignment_id = am.assgn_nbr
                              WHERE adbwh.warehouse_id IN ( $warehouseId )
                                AND adbwh.ops_date >= '$startTime'
                                AND adbwh.ops_date <= '$endTime' 
                                AND adbwh.merged_status = 'N'
                              )
                          SELECT warehouse_id
                               ,ops_date
                               ,snapshot_id
                               ,ext_timestamp 
                               ,snapshot_time
                               ,hour_minute
                               ,total_bill_qty
                               ,total_selected_qty
                               ,total_scr_qty
                               ,total_remaining_qty
                               ,CASE WHEN ( total_bill_qty - LAG( total_bill_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) ) < 0 THEN 0
                                      ELSE total_bill_qty - LAG( total_bill_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) 
      
                                 END total_bill_qty_hr
                                , total_bill_qty - LAG( total_bill_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) 
      
                                  total_bill_qty_hr_ve
                               ,CASE WHEN ( total_selected_qty - LAG( total_selected_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) ) < 0 THEN 0
                                      ELSE total_selected_qty - LAG( total_selected_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) 
      
                                 END total_selected_qty_hr
                               ,CASE WHEN ( total_scr_qty - LAG( total_scr_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) ) < 0 THEN 0
                                      ELSE total_scr_qty - LAG( total_scr_qty, 1, 0 ) OVER 
                                                              ( PARTITION BY warehouse_id, ops_date
                                                                    ORDER BY warehouse_id, ops_date
                                                                            ,ext_timestamp
                                                                           ) 
      
                                 END total_scr_qty_hr
                              FROM base_data 
                         """
                                
         logger.debug( " - Query - " + calcQuery );
         val calc = sqlContext.sql(calcQuery)   
         calc.persist()
         //calc.show(40)
         logger.debug( " - Query fired " ) 
         
         /* calc.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "TotalCalc/")*/
         
         //AND LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) IN ('52','07')
         val loadCalcQuery = s""" WITH base_rows AS 
                                        (  SELECT DISTINCT adbwh.warehouse_id
                                                          ,adbwh.ops_date
                                                          ,adbwh.snapshot_id
                                                          ,adbwh.ext_timestamp 
                                                          ,adbwh.snapshot_time
                                                          ,concat( LPAD( hour(adbwh.ext_timestamp), 2, '0' ), LPAD( minute( adbwh.ext_timestamp ), 2, '0' ) ) hour_minute
                                                          ,adbwh.load_number
                                                          ,adbwh.closed_timestamp
                                             FROM assignment_details_by_warehouse_history adbwh
                                            WHERE adbwh.warehouse_id IN ( $warehouseId )
                                              AND adbwh.ops_date >= '$startTime'
                                              AND adbwh.ops_date <= '$endTime' 
                                              AND adbwh.merged_status = 'N'
                                        )
                                  SELECT DISTINCT warehouse_id
                                        ,ops_date
                                        ,snapshot_id
                                        ,ext_timestamp 
                                        ,hour_minute
                                        ,SUM( CASE WHEN closed_timestamp IS NOT NULL THEN 1 ELSE 0 END ) 
                                                OVER (PARTITION BY warehouse_id
                                                                  ,ops_date
                                                                  ,hour_minute 
                                                                  ,snapshot_time
                                                     ) total_closed_loads
                                        ,SUM( CASE WHEN closed_timestamp IS NULL THEN 1 ELSE 0 END ) 
                                                OVER (PARTITION BY warehouse_id
                                                                  ,ops_date
                                                                  ,hour_minute 
                                                                  ,snapshot_time
                                                     ) total_open_loads
                                    FROM base_rows
                               """   
                 
         logger.debug( " - Query - " + loadCalcQuery );
         val loadCalc = sqlContext.sql(loadCalcQuery)   
         loadCalc.persist()
         //loadCalc.show(40)
         logger.debug( " - Query fired " ) 
         
         /* loadCalc.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "TotalLoadCalc/")*/
         
         val totalCalc = calc.join(loadCalc, Seq("warehouse_id", "ops_date", "snapshot_id", "ext_timestamp", "hour_minute"), "left" )
                             .select( calc("warehouse_id"), calc("ops_date"), calc("snapshot_id"), calc("ext_timestamp"), calc("hour_minute")
                                     ,calc("total_bill_qty"), calc("total_selected_qty"), calc("total_scr_qty"), calc("total_remaining_qty")
                                     ,calc("total_bill_qty_hr"), calc("total_bill_qty_hr_ve"), calc("total_selected_qty_hr"), calc("total_scr_qty_hr") //, calc("total_remaining_qty_hr")
                                     ,loadCalc("total_closed_loads"), loadCalc("total_open_loads")
                                     ) 
                             .orderBy( calc("warehouse_id"), calc("ops_date"), calc("ext_timestamp"), calc("hour_minute") )
         
         val previousAssgnSnapshot = s""" SELECT * 
                                           FROM assignment_details_by_warehouse_prev
                                          WHERE warehouse_id IN ( $warehouseId )
                                            AND ops_date >= '$startTime'
                                            AND ops_date <= '$endTime'
                                       ORDER BY warehouse_id, ops_date,load_number, stop_number, assignment_id, update_time
                                      """
         
         val assgnSnapshot = s""" SELECT * 
                                       FROM assignment_details_by_warehouse
                                      WHERE warehouse_id IN ( $warehouseId )
                                        AND ops_date >=  '$startTime'
                                        AND ops_date <=  '$endTime'
                                   ORDER BY warehouse_id, ops_date, load_number, stop_number, assignment_id, update_time
                                      """
                                        
         val assgnMergedSnapshot = s""" SELECT am.invoice_site warehouse_id  
                                              ,am.ops_date, am.ext_timestamp merge_ext_timestamp, adbw.ext_timestamp assgn_ext_timestamp
                                              ,( unix_timestamp(am.ext_timestamp) - unix_timestamp(adbw.ext_timestamp) )  / (60) time_diff_min
                                              ,am.creation_time, adbw.merged_status
                                              ,am.update_time merge_update_time, adbw.update_time assgn_update_time
                                              ,adbw.load_number, adbw.stop_number
                                              ,am.load_nbr, am.assgn_nbr
                                              ,adbw.customer_name, adbw.customer_number
                                              ,adbw.bill_qty, adbw.cases_selected
                                              ,adbw.number_of_cubes, adbw.scr_qty
                                              ,am.snapshot_id
                                              ,am.extr_date, am.extr_time, am.print_site
                                       FROM assignments_merged am 
                                       LEFT OUTER JOIN assignment_details_by_warehouse adbw
                                         ON am.ops_date = adbw.ops_date
                                            AND adbw.warehouse_id = am.invoice_site
                                            AND adbw.assignment_id = am.assgn_nbr
                                      WHERE am.invoice_site IN ( $warehouseId )
                                        AND am.ops_date >= '$startTime'
                                        AND am.ops_date <= '$endTime'
                                   ORDER BY warehouse_id, am.ops_date, adbw.load_number, adbw.stop_number, adbw.assignment_id, adbw.update_time
                                      """    
         
        val assgnMergedSnapshotPrev = s""" SELECT am.invoice_site warehouse_id  
                                              ,am.ops_date, am.ext_timestamp merge_ext_timestamp, adbw.ext_timestamp assgn_ext_timestamp
                                              ,( unix_timestamp(am.ext_timestamp) - unix_timestamp(adbw.ext_timestamp) ) / (60) time_diff_min
                                              ,am.creation_time, adbw.merged_status
                                              ,am.update_time merge_update_time, adbw.update_time assgn_update_time
                                              ,adbw.load_number, adbw.stop_number
                                              ,am.load_nbr, am.assgn_nbr
                                              ,adbw.customer_name, adbw.customer_number
                                              ,adbw.bill_qty, adbw.cases_selected
                                              ,adbw.number_of_cubes, adbw.scr_qty
                                              ,am.snapshot_id
                                              ,am.extr_date, am.extr_time, am.print_site
                                       FROM assignments_merged am 
                                       LEFT OUTER JOIN assignment_details_by_warehouse_prev adbw
                                         ON am.ops_date = adbw.ops_date
                                            AND adbw.warehouse_id = am.invoice_site
                                            AND adbw.assignment_id = am.assgn_nbr
                                      WHERE am.invoice_site IN ( $warehouseId )
                                        AND am.ops_date >= '$startTime'
                                        AND am.ops_date <= '$endTime'
                                   ORDER BY warehouse_id, am.ops_date, adbw.load_number, adbw.stop_number, adbw.assignment_id, adbw.update_time
                                      """ 
        
                                      //,'2017-05-08 10:07', '2017-05-08 10:52', '2017-05-08 11:07'
        val shippingHistory = s""" SELECT wsdh.* 
                                     FROM warehouse_shipping_data_history wsdh
                                         ,file_streaming_tracker fst
                                    WHERE fst.snapshot_id = wsdh.snapshot_id  
                                      AND fst.file_name IN ( 'WIP_3_ASGN.201705211752.CSV','WIP_3_ASGN.201705211807.CSV', 'WIP_3_ASGN.201705211852.CSV', 'WIP_3_ASGN.201705211907.CSV'
                                                            ,'WIP_3_ASGN.201705211952.CSV','WIP_3_ASGN.201705212007.CSV', 'WIP_3_ASGN.201705212052.CSV', 'WIP_3_ASGN.201705212217.CSV'
                                                            ,'WIP_3_ASGN.201705212152.CSV','WIP_3_ASGN.201705212207.CSV', 'WIP_3_ASGN.201705212252.CSV', 'WIP_3_ASGN.201705212307.CSV'
                                                            ,'WIP_3_ASGN.201705212352.CSV','WIP_3_ASGN.201705220007.CSV', 'WIP_3_ASGN.201705220052.CSV', 'WIP_3_ASGN.201705220107.CSV'
                                                            ,'WIP_3_ASGN.201705220152.CSV','WIP_3_ASGN.201705220207.CSV', 'WIP_3_ASGN.201705220252.CSV', 'WIP_3_ASGN.201705220307.CSV'
                                                            ,'WIP_3_ASGN.201705220352.CSV','WIP_3_ASGN.201705220407.CSV', 'WIP_3_ASGN.201705220452.CSV', 'WIP_3_ASGN.201705220507.CSV'
                                                            ,'WIP_3_ASGN.201705220552.CSV','WIP_3_ASGN.201705220607.CSV', 'WIP_3_ASGN.201705220652.CSV', 'WIP_3_ASGN.201705220707.CSV'
                                                            ,'WIP_3_ASGN.201705220752.CSV','WIP_3_ASGN.201705220807.CSV', 'WIP_3_ASGN.201705220852.CSV', 'WIP_3_ASGN.201705221007.CSV'
                                                            ,'WIP_3_ASGN.201705220952.CSV','WIP_3_ASGN.201705221007.CSV', 'WIP_3_ASGN.201705221052.CSV', 'WIP_3_ASGN.201705221107.CSV'
                                                            ,'WIP_3_ASGN.201705221152.CSV','WIP_3_ASGN.201705222007.CSV', 'WIP_3_ASGN.201705221252.CSV', 'WIP_3_ASGN.201705221307.CSV'
                                                            ,'WIP_3_ASGN.201705221352.CSV','WIP_3_ASGN.201705224007.CSV', 'WIP_3_ASGN.201705221452.CSV', 'WIP_3_ASGN.201705221507.CSV'
                                                            ,'WIP_3_ASGN.201705221552.CSV','WIP_3_ASGN.201705226007.CSV', 'WIP_3_ASGN.201705221652.CSV', 'WIP_3_ASGN.201705221707.CSV'
                                                              )
                               """
        
        val shippingHistoryAssgn = s""" SELECT fst.file_name, fst.ext_timestamp file_ext_timestamp, fst.creation_time, fst.update_time, fst.validation_status
                                              ,wsdh.* 
                                         FROM warehouse_shipping_data_history wsdh
                                             ,file_streaming_tracker fst
                                        WHERE fst.snapshot_id = wsdh.snapshot_id  
                                          AND wsdh.load_nbr = '000000049793'
                                          AND assgn_nbr IN ( '098141','098142' )
                                       ORDER BY load_nbr, assgn_nbr, wsdh.ext_timestamp
                               """        
        val allCalculationsQuery = s""" SELECT DISTINCT 
                                               ext_timestamp
                                              ,hour(ext_timestamp) hour_data
                                              ,merged_status
                                              ,SUM(bill_qty) OVER ( PARTITION BY ext_timestamp, merged_status ) bill_qty
                                              ,SUM(cases_selected) OVER ( PARTITION BY ext_timestamp, merged_status ) selected_qty
                                              ,SUM(scr_qty) OVER ( PARTITION BY ext_timestamp, merged_status ) scr_qty
                                              ,SUM(bill_qty) OVER ( PARTITION BY ext_timestamp, merged_status ) 
                                               - ( SUM(cases_selected) OVER ( PARTITION BY ext_timestamp, merged_status )
                                                  + SUM(scr_qty) OVER ( PARTITION BY ext_timestamp, merged_status )
                                                 )
                                               cases_remaining
                                          FROM assignment_details_by_warehouse_history
                                         WHERE warehouse_id IN ( $warehouseId )
                                           AND ops_date >= '$startTime'
                                           AND ops_date <= '$endTime' 
                                           AND merged_status = 'N'
                                      ORDER BY ext_timestamp
                                    """
        
        /*
         *                                       fst.ext_timestamp IN ( '2017-05-08 17:52','2017-05-08 18:07', '2017-05-08 18:52', '2017-05-08 19:07'
                                                                ,'2017-05-08 19:52','2017-05-08 20:07', '2017-05-08 20:52', '2017-05-08 21:07'
                                                                ,'2017-05-08 21:52','2017-05-08 22:07', '2017-05-08 22:52', '2017-05-08 23:07'
                                                                ,'2017-05-08 23:52','2017-05-09 00:07', '2017-05-09 00:52', '2017-05-09 01:07'
                                                                ,'2017-05-09 01:52','2017-05-09 02:07', '2017-05-09 02:52', '2017-05-09 03:07'
                                                                ,'2017-05-09 03:52','2017-05-09 04:07', '2017-05-09 04:52', '2017-05-09 05:07'
                                                                ,'2017-05-09 05:52','2017-05-09 06:07', '2017-05-09 06:52', '2017-05-09 07:07'
                                                                ,'2017-05-09 07:52','2017-05-09 08:07', '2017-05-09 08:52', '2017-05-09 09:07'
                                                                ,'2017-05-09 09:52'
                                                              )
         *                                       AND snapshot_id IN ('f8953370-3438-11e7-a580-ebf0ec05afba', 'f8fb9dd0-3371-11e7-a580-ebf0ec05afba', '43098940-3378-11e7-a580-ebf0ec05afba'
                                                         ,'5ad896e0-337a-11e7-a580-ebf0ec05afba', '80370790-3380-11e7-a580-ebf0ec05afba', '582dac10-3383-11e7-a580-ebf0ec05afba'
                                                         ,'0124e220-3389-11e7-a580-ebf0ec05afba', 'cc8098e0-338b-11e7-a580-ebf0ec05afba', '4ce74970-3391-11e7-a580-ebf0ec05afba'
                                                         ,'1618a620-3394-11e7-a580-ebf0ec05afba', 'bbb98540-3399-11e7-a580-ebf0ec05afba', 'd8257390-339b-11e7-a580-ebf0ec05afba'
                                                         ) 
         */
         
        // Query 1
         logger.debug( " - Query - " + assgnHistorySnapshot );
         val queryData3 = sqlContext.sql(assgnHistorySnapshot)     
         logger.debug( " - Query fired " ) 
         //queryData3.show
         
         queryData3.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "CompleteAssignmentsHistory/") 

         totalCalc.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "CompleteTotal/")    
         
        // Query 2         
       /*  logger.debug( " - Query - " + previousAssgnSnapshot );
         val queryData = sqlContext.sql(previousAssgnSnapshot)     
         logger.debug( " - Query fired " ) 
         //queryData.show(40)
         
         queryData.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "CompleteAssignmentsPrev") 
         
        // Query 3         
         logger.debug( " - Query - " + assgnSnapshot );
         val queryData1 = sqlContext.sql(assgnSnapshot)     
         logger.debug( " - Query fired " ) 
         //queryData1.show(40)
         
         queryData1.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "CompleteAssignments")           

        // Query 4         
         logger.debug( " - Query - " + assgnMergedSnapshot );
         val queryData2 = sqlContext.sql(assgnMergedSnapshot)     
         logger.debug( " - Query fired " ) 
         //queryData2.filter("merged_status = 'Y' ").show(40)
         
         queryData2.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "CompleteMergedAssignments")     
         
        // Query 5         
         logger.debug( " - Query - " + assgnMergedSnapshotPrev );
         val queryData4 = sqlContext.sql(assgnMergedSnapshotPrev)     
         logger.debug( " - Query fired " ) 
         //queryData4.filter("merged_status = 'Y' ")show()
         
          queryData4.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "CompleteMergedAssignmentsPrev/")  
				*/
        // Query 6         
         /*logger.debug( " - Query - " + shippingHistory );
         val queryData5 = sqlContext.sql(shippingHistory)     
         logger.debug( " - Query fired " ) 
         //queryData5.filter("merged_status = 'Y' ")show()
         
          queryData5.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "ShippingHistory/")        
				*/
        /*
        // Query 7
         logger.debug( " - Query - " + allCalculationsQuery );
         val queryData6 = sqlContext.sql(allCalculationsQuery)     
         logger.debug( " - Query fired " ) 
         //queryData6.show()
         
          queryData6.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "AllCalculations/")           
           */
         
         //existingData.foreach(println)
         //var existingRows: List[Row] = List[] 
         /*existingData.foreach { x => 
                                   for ( row <- x ) {
                                    existingRows += row
                                    logger.debug("Print Row - " + existingRows.length)
                                    /*val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), existingRowSchema)
                                    logger.debug( " - Update Dataframe - " )  
                                    existingRowsDF.coalesce(1).write
                                       .format("com.databricks.spark.csv")
                                       .option("header", "true")
                                       .mode("append")
                                       .save(filePath + "ExistingData/")*/
                                 }                                  
                               }
                               */
         //existingData.foreach { x => getRow(x) }
         /*for ( data <- existingData ) {
             var combinedRow = getRow(data) 
             val existingRowSchema = new StructType( Array( StructField("warehouse_id", IntegerType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("customer_number", StringType, false ) 
                                                      ,StructField("customer_name", StringType, true ) 
                                                      ,StructField("time_sequence", StringType, false ) 
                                                      ,StructField("hourly_bill", StringType, true ) 
                                                      ,StructField("cases_selected", StringType, true )
                                                      ,StructField("scratch_quantity", StringType, true )
                                                      ,StructField("total_scratches", StringType, true ) 
                                                      ,StructField("cases_remaining", StringType, true ) 
                                                      ,StructField("loads_closed", StringType, true )
                                                      ,StructField("loads_remaining", StringType, true )
                                                      )
                                     )  
             val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(combinedRow.toList), existingRowSchema)
             existingRowsDF.write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               .mode("append")
               .save(filePath + "ExistingData/")
         }*/
         
        /*{ for ( row <- data )  {
                      existingRows += row
                      //existingRows.++(Seq(row))
                     /* logger.debug("Print Row - " + existingRows.length)
                      //existingRows.foreach {  println }*/
                     //existingRows
                    }
                    //existingRows
                 }  */
                          
      }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getSnapshot at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def reverseString(stringCur: String): String = {
      return stringCur.reverse
  }
  
  def verifyLoadLoaderPercent(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "               verifyLoadLoaderPercent              " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'29'";
    try{   
           val loadLoaderNotAssignedQuery = s""" SELECT *
                                                   FROM load_details_by_warehouse
                                                  WHERE warehouse_id IN ($warehouseId)
                                                    AND ops_date > '02/22/2017' 
                                                    AND ops_date <= '$currentTime'
                                                    AND ( loaded_pct IS NULL OR loaded_pct = 0 OR loader_employee_id IS NULL )
                                               ORDER BY warehouse_id, ops_date DESC 
                                        """
           logger.debug( " - loadLoaderNotAssignedQuery - " + loadLoaderNotAssignedQuery );
           val loadLoaderNotAssigned = sqlContext.sql(loadLoaderNotAssignedQuery)     
           logger.debug( " - Query fired " ) 
           loadLoaderNotAssigned.show(40)
           
           loadLoaderNotAssigned.coalesce(1).write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .mode("overwrite")
           .save(filePath + "LoadLoaderPercentLoads")
           
           val loadNumbers = "'" + loadLoaderNotAssigned.select("load_number")
                             .collect()
                             .map(f => f.getString(0))
                             .mkString("','") + "'"  
                             
           logger.debug( " Missing Load Numbers - " + loadNumbers )
           val loadLoadersQuery = s""" SELECT * 
                                         FROM load_loaders_by_warehouse
                                        WHERE load_number IN ($loadNumbers)
                                        UNION
                                       SELECT * 
                                         FROM load_loaders_by_warehouse
                                        WHERE pallet_ovrld_to_load IN ($loadNumbers)
                                     ORDER BY warehouse_id, gate_datetime DESC 
                                   """
           logger.debug( " - loadLoadersQuery - " + loadLoadersQuery );
           val loadLoaders = sqlContext.sql(loadLoadersQuery)     
           logger.debug( " - Query fired " ) 
           loadLoaders.show(40)
           
           loadLoaders.coalesce(1).write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .mode("overwrite")
           .save(filePath + "LoadLoaders")
           
           val loadPercentQuery = s""" SELECT * 
                                         FROM load_percent_by_warehouse
                                        WHERE load_number IN ($loadNumbers)
                                     ORDER BY warehouse_id, gate_datetime DESC 
                                   """
           logger.debug( " - loadPercentQuery - " + loadPercentQuery );
           val loadPercent = sqlContext.sql(loadPercentQuery)     
           logger.debug( " - Query fired " ) 
           loadPercent.show(40)
           
           loadPercent.coalesce(1).write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           .mode("overwrite")
           .save( filePath + "LoadLoaderPercent")
           
           
   }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing verifyLoadLoaderPercent at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }           
  
  
  def selectorsWithDelayedLoads(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "             selectorsWithDelayedLoads              " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'29'"
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate
    
    val headerStyle =
         CellStyle( fillPattern = CellFill.Solid ,fillForegroundColor = Color.AquaMarine, font = Font(bold = true)) 
         
    try{  
           /*val delayedLoadsQuery = s""" WITH base_rows AS ( SELECT ldbw.warehouse_id
                                                                  ,ldbw.ops_date
                                                                  ,ldbw.load_number
                                                                  ,ldbw.closed_timestamp
                                                                  ,ldbw.customer_number
                                                                  ,ldbw.custoemr_name 
                                                                  ,ldbw.number_of_stops
                                                                  ,ldbw.dispatch_timestamp
                                                                  ,ldbw.set_back_timestamp
                                                                  ,ldbw.total_cases
                                                                  ,ldbw.expected_pallets
                                                                  ,ldbw.actual_pallets
                                                                  ,adbw.assignment_type
                                                                  ,adbw.assignment_id
                                                                  ,adbw.employee_id
                                                                  ,adbw.employee_name
                                                              FROM load_details_by_warehouse ldbw
                                                                  ,assignment_details_by_warehouse adbw
                                                             WHERE set_back_timestamp IS NOT NULL
                                                               AND ldbw.warehouse_id = adbw.warehouse_id
                                                               AND ldbw.ops_date = adbw.ops_date
                                                               AND ldbw.load_number = adbw.load_number
                                                            ) 
                                          SELECT employee_id, employee_name, COUNT(DISTINCT load_number)
                                    """*/
      /*                                          * AND SUM( adbw.cases_selected ) OVER ( PARITION BY adbw.warehouse_id
                                                                                            ,adbw.ops_date 
                                                                                            ,adbw.load_number ) > 0
                                                                                            * 
                                                                                            */
          val delayedLoadsQuery = s""" SELECT adbw.warehouse_id
                                              ,adbw.ops_date 
                                              ,adbw.employee_id
                                              ,adbw.employee_name
                                              ,COUNT(DISTINCT adbw.load_number) delayed_loads
                                          FROM load_details_by_warehouse ldbw
                                              ,assignment_details_by_warehouse adbw
                                         WHERE set_back_timestamp IS NOT NULL
                                           AND ldbw.warehouse_id = adbw.warehouse_id
                                           AND ldbw.ops_date = adbw.ops_date
                                           AND ldbw.load_number = adbw.load_number
                                           AND adbw.merged_status = 'N'
                                           AND adbw.cases_selected > 0 
                                        GROUP BY adbw.warehouse_id, adbw.ops_date, adbw.employee_id, adbw.employee_name
                                        ORDER BY adbw.warehouse_id, adbw.ops_date, delayed_loads DESC
                                   """
          
         logger.debug( " - Query - " + delayedLoadsQuery );
         val delayedLoads = sqlContext.sql(delayedLoadsQuery)     
         logger.debug( " - Query fired " ) 
         //delayedLoads.show(40)
         delayedLoads.registerTempTable("delayed_loads")
         
         delayedLoads.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "DelayedLoads/") 
         
         //val sheet1 = Sheet( name="DelayedLoads", row = delayedLoads.collect() )
         //.withRows( XRow( delayedLoads.collect() ) )
         //.withRows( XRow( delayedLoads.collect() )  )
           
         val employeeTotaldelatedLoadsQuery = s"""
                                       SELECT DISTINCT warehouse_id
                                             ,employee_id
                                             ,employee_name
                                             ,SUM(delayed_loads) OVER ( PARTITION BY warehouse_id, employee_id ) total_delayed_loads
                                             ,COUNT(ops_date) OVER ( PARTITION BY warehouse_id, employee_id ) total_delayed_ops_days
                                             ,MAX(ops_date) OVER ( PARTITION BY warehouse_id, employee_id ) tlatest_delayed_ops_days
                                         FROM delayed_loads 
                                      ORDER BY warehouse_id, total_delayed_ops_days, total_delayed_loads, employee_id 
                                          """

         logger.debug( " - Query - " + employeeTotaldelatedLoadsQuery );
         val employeeTotaldelatedLoads = sqlContext.sql(employeeTotaldelatedLoadsQuery)     
         logger.debug( " - Query fired " ) 
         //employeeTotaldelatedLoads.show(40)
         //employeeTotaldelatedLoads.registerTempTable("delayed_loads")
         
         employeeTotaldelatedLoads.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "EmployeeDelayedCount/") 
         
         //val EmployeeDelayedCount = Sheet( name = "EmployeeDelayed" ).withRows( employeeTotaldelatedLoads )
    
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing selectorsWithDelayedLoads at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def hoursAnalysis(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      hoursAnalysis                 " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'29'"
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate    
    var existingRows = new ListBuffer[Row]()  
    try{  
         /*
          *                                         WHERE warehouse_id IN ( $warehouseId )
                                          AND ops_date >= '$startTime'
                                          AND ops_date <= '$endTime'       
          */
        //                                             ,time_sequence_ts
         val existingLoadsQuery = s""" SELECT warehouse_id
                                             ,ops_date
                                             ,customer_number
                                             ,customer_name
                                             ,time_sequence
                                             ,hourly_bills_list
                                             ,cases_selected_list
                                             ,scratch_quantity_list
                                             ,total_scratches_list
                                             ,cases_remaining_list
                                             ,loads_closed_list
                                             ,loads_remaining_list
                                             ,time_sequence_ts
                                             ,aba_capacity_list
                                         FROM load_details_by_customer
                                        WHERE warehouse_id = $warehouseId
                                          AND ops_date >= '$startTime'
                                          AND ops_date <= '$endTime' 
                                   """ 
         logger.debug( " - Query - " + existingLoadsQuery );
         val existingLoads = sqlContext.sql(existingLoadsQuery)   
         //existingLoads.show(40)
         
         val existingData = existingLoads.map { x =>  if ( x.get(4) == null ) null 
                                 else x.getAs[WrappedArray[String]](4)
                                       .map { y => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getString(3)
                                                       ,y
                                                       ,if ( x.get(5) == null ) null else x.getAs[WrappedArray[String]](5).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](5).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(6) == null ) null else x.getAs[WrappedArray[String]](6).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](6).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(7) == null ) null else x.getAs[WrappedArray[String]](7).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](7).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(8) == null ) null else x.getAs[WrappedArray[String]](8).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](8).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(9) == null ) null else x.getAs[WrappedArray[String]](9).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](9).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(10) == null ) null else x.getAs[WrappedArray[String]](10).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](10).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(11) == null ) null else x.getAs[WrappedArray[String]](11).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](11).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(12) == null ) null else x.getAs[WrappedArray[java.sql.Timestamp]](12).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[java.sql.Timestamp]](12).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) )
                                                       ,if ( x.get(13) == null ) null else x.getAs[WrappedArray[String]](13).apply( if ( x.getAs[WrappedArray[String]](4).indexOf(y) >= x.getAs[WrappedArray[String]](13).length ) 0 else x.getAs[WrappedArray[String]](4).indexOf(y) ) 
                                                        )
                                               }
         
                            }
                  
         logger.debug( " existingData " );

         val existingRowSchema = new StructType( Array( StructField("warehouse_id", StringType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("customer_number", StringType, false ) 
                                                      ,StructField("customer_name", StringType, true ) 
                                                      ,StructField("time_sequence", StringType, false ) 
                                                      ,StructField("hourly_bill", StringType, true ) 
                                                      ,StructField("cases_selected", StringType, true )
                                                      ,StructField("scratch_quantity", StringType, true )
                                                      ,StructField("total_scratches", StringType, true ) 
                                                      ,StructField("cases_remaining", StringType, true ) 
                                                      ,StructField("loads_closed", StringType, true )
                                                      ,StructField("loads_remaining", StringType, true )
                                                      ,StructField("time_sequence_ts", TimestampType, true )
                                                      ,StructField("aba_capacity", StringType, true )
                                                      )
                                     )  

         for ( i <- 0 to ( existingData.count().toInt - 1 ) )  {
             existingRows.++=(
             getRow( existingData.collect().apply(i) )
             )
         }

        logger.debug( " existingRows " );
        //combinedRow.foreach { println }
        //existingRows.foreach { println }
        
         val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), existingRowSchema)
         logger.debug( " - Update Dataframe - " )     
         //existingRowsDF.show(40)
         existingRowsDF.registerTempTable("hourly_data")
         
         existingRowsDF.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "ExistingHourlyData/")
         
          val hoursAnalysisQuery = s""" SELECT hd.warehouse_id
                                                  ,hd.ops_date 
                                                  ,hd.customer_number
                                                  ,hd.customer_name
                                                  ,hd.time_sequence
                                                  ,hd.cases_selected
                                                  ,hd.loads_closed
                                                  ,MAX( hd.cases_selected ) OVER ( PARTITION BY hd.warehouse_id
                                                                                            ,hd.ops_date 
                                                                                            ,hd.customer_number
                                                                                            ,hd.customer_name ) max_cases_selected_forday
                                                  ,MAX( hd.loads_closed ) OVER ( PARTITION BY hd.warehouse_id
                                                                                            ,hd.ops_date 
                                                                                            ,hd.customer_number
                                                                                            ,hd.customer_name ) max_loads_closed_forday
                                              FROM hourly_data hd
                                        """
         logger.debug( " - Query - " + hoursAnalysisQuery );
         val hoursAnalysis = sqlContext.sql(hoursAnalysisQuery)     
         logger.debug( " - Query fired " ) 
         //hoursAnalysis.show(40)
         hoursAnalysis.registerTempTable("hourly_analysis")
         
         hoursAnalysis.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "hoursAnalysis/") 
         
         val hoursEmployeeDetailsQuery = s"""  SELECT warehouse_id
                                              ,ops_date 
                                              ,customer_number
                                              ,customer_name
                                              ,time_sequence
                                              ,cases_selected
                                              ,loads_closed
                                              ,max_cases_selected_forday
                                              ,max_loads_closed_forday
                                              ,CASE WHEN max_cases_selected_forday = cases_selected THEN
                                                         'Y'
                                                    ELSE 'N'
                                                 END cases_selected_hour
                                              ,CASE WHEN max_loads_closed_forday = loads_closed THEN
                                                         'Y'
                                                    ELSE 'N'
                                                 END load_closed_hour
                                          FROM hourly_analysis
                                         WHERE ( max_cases_selected_forday = cases_selected 
                                               OR max_loads_closed_forday = loads_closed 
                                               )                                    
                                    ORDER BY warehouse_id, ops_date, max_cases_selected_forday, max_loads_closed_forday
                                   """
          
         logger.debug( " - Query - " + hoursEmployeeDetailsQuery );
         val hoursEmployeeDetails = sqlContext.sql(hoursEmployeeDetailsQuery)     
         logger.debug( " - Query fired " ) 
         //hoursAnalysis.show(40)
         
         hoursEmployeeDetails.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "hourlyEmployeeDetails/") 
    
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing hoursAnalysis at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def trailerNeeds(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   trailerNeeds                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'29'"
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate
    var existingRows = new ListBuffer[Row]()  
    
    val headerStyle =
         CellStyle( fillPattern = CellFill.Solid ,fillForegroundColor = Color.AquaMarine, font = Font(bold = true)) 
         
    try{  
          val trailerNeedsQuery = s""" SELECT warehouse_id
                                             ,ops_date
                                             ,customer_number
                                             ,customer_name
                                             ,customer_need
                                             ,need_by_profile
                                         FROM trailer_need_by_warehouse
                                   """ 
         logger.debug( " - Query - " + trailerNeedsQuery );
         val trailerNeeds = sqlContext.sql(trailerNeedsQuery)   
         //existingLoads.show(40)
         
         val trailerNeedsRow = trailerNeeds.map { x =>  if ( x.get(5) == null ) null 
                                                   else x.getAs[Map[String, Int]](5)
                                                 .map { y => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getString(3), x.getInt(4)
                                                                 ,y._1.trim()
                                                                 ,y._2
                                                                  )
                                                       }
                                               }
                  
         logger.debug( " existingData " );

         val existingRowSchema = new StructType( Array( StructField("warehouse_id", StringType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("customer_number", StringType, false ) 
                                                      ,StructField("customer_name", StringType, true ) 
                                                      ,StructField("total_customer_need", IntegerType, false ) 
                                                      ,StructField("trailer_profile", StringType, true ) 
                                                      ,StructField("trailer_profile_need", IntegerType, true )
                                                      )
                                     )  

         for ( i <- 0 to ( trailerNeedsRow.count().toInt - 1 ) )  {
             existingRows.++=(
             getTrailerRow( trailerNeedsRow.collect().apply(i) )
             )
         }

        logger.debug( " existingRows " );
        //combinedRow.foreach { println }
        //existingRows.foreach { println }
        
         val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), existingRowSchema)
         logger.debug( " - Update Dataframe - " )     
         //existingRowsDF.show(40)
         existingRowsDF.registerTempTable("trailer_needs")
        
        existingRowsDF.coalesce(1)
         .orderBy("warehouse_id", "ops_date", "customer_number", "trailer_profile_need") 
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "TrailerNeedsTotal/")    

         val maxTrailerNeedOpsDayQuery = s""" SELECT DISTINCT warehouse_id
                                          ,ops_date
                                          ,customer_number
                                          ,customer_name
                                          ,total_customer_need
                                          ,TRIM(trailer_profile) trailer_profile
                                          ,trailer_profile_need
                                          ,SUM( trailer_profile_need ) OVER ( PARTITION BY warehouse_id
                                                                                          ,ops_date
                                                                                          ,trailer_profile
                                                                            ) total_trailer_profile_need
                                     FROM trailer_needs
                                  ORDER BY warehouse_id, ops_date, customer_number, total_trailer_profile_need DESC
                               """
         logger.debug( " - Query - " + maxTrailerNeedOpsDayQuery );
         val maxTrailerNeedOpsDay = sqlContext.sql(maxTrailerNeedOpsDayQuery)   
         //existingLoads.show(40)
        
        maxTrailerNeedOpsDay.coalesce(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "MaxTrailerNeedsOpsDay/")   
         
         val maxTrailerNeedQuery = s""" SELECT DISTINCT warehouse_id
                                          ,customer_number
                                          ,customer_name
                                          ,TRIM(trailer_profile) trailer_profile
                                          ,SUM( trailer_profile_need ) OVER ( PARTITION BY warehouse_id
                                                                                          ,customer_number
                                                                                          ,customer_name
                                                                                          ,trailer_profile
                                                                            ) total_trailer_profile_need
                                     FROM trailer_needs
                                  ORDER BY warehouse_id, customer_number, total_trailer_profile_need DESC
                               """
         logger.debug( " - Query - " + maxTrailerNeedQuery );
         val maxTrailerNeed = sqlContext.sql(maxTrailerNeedQuery)   
         //existingLoads.show(40)
        
        maxTrailerNeed.coalesce(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "MaxTrailerNeeds/")   
         
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing trailerNeeds at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def trailerAvailability(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   trailerAvailability              " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val warehouseId = "'29'"
    val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
    val endTime =  getCurrentDate
    var existingRows = new ListBuffer[Row]()  
    
    val headerStyle =
         CellStyle( fillPattern = CellFill.Solid ,fillForegroundColor = Color.AquaMarine, font = Font(bold = true)) 
         
    try{  
          val trailerAvailabilityQuery = s""" SELECT warehouse_id
                                             ,ops_date
                                             ,load_type
                                             ,total_availability
                                             ,availability_by_profile
                                         FROM trailer_availability_by_warehouse
                                   """ 
         logger.debug( " - Query - " + trailerAvailabilityQuery );
         val trailerAvailability = sqlContext.sql(trailerAvailabilityQuery)   
         //existingLoads.show(40)
         
         val trailerAvailabilityRow = trailerAvailability.map { x =>  if ( x.get(4) == null ) null 
                                                   else x.getAs[Map[String, Int]](4)
                                                 .map { y => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getInt(3)
                                                                 ,y._1.trim()
                                                                 ,y._2
                                                                  )
                                                       }
                                               }
                  
         logger.debug( " existingData " );

         val existingRowSchema = new StructType( Array( StructField("warehouse_id", StringType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("load_type", StringType, false ) 
                                                      ,StructField("total_availability", IntegerType, true ) 
                                                      ,StructField("trailer_profile", StringType, true ) 
                                                      ,StructField("trailer_profile_availability", IntegerType, true )
                                                      )
                                     )  

         for ( i <- 0 to ( trailerAvailabilityRow.count().toInt - 1 ) )  {
             existingRows.++=(
             getTrailerRow( trailerAvailabilityRow.collect().apply(i) )
             )
         }

        logger.debug( " existingRows " );
        //combinedRow.foreach { println }
        //existingRows.foreach { println }
        
         val existingRowsDF = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), existingRowSchema)
         logger.debug( " - Update Dataframe - " )     
         //existingRowsDF.show(40)
         existingRowsDF.registerTempTable("trailer_availability")
        
        existingRowsDF.coalesce(1)
          .orderBy("warehouse_id", "ops_date", "load_type", "trailer_profile")
          .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "TrailerAvailabilityTotal/")    
         
         val maxTrailerAvailabilityOpsDayQuery = s""" SELECT DISTINCT warehouse_id
                                          ,ops_date
                                          ,load_type
                                          ,total_availability
                                          ,TRIM(trailer_profile) trailer_profile
                                          ,trailer_profile_availability
                                          ,SUM( trailer_profile_availability ) OVER ( PARTITION BY warehouse_id
                                                                                          ,ops_date
                                                                                          ,trailer_profile
                                                                            ) total_trailer_availability
                                     FROM trailer_availability
                                  ORDER BY warehouse_id, ops_date, load_type, total_trailer_availability DESC
                               """
         logger.debug( " - Query - " + maxTrailerAvailabilityOpsDayQuery );
         val maxTrailerAvailabilityOpsDay = sqlContext.sql(maxTrailerAvailabilityOpsDayQuery)   
         //existingLoads.show(40)
        
        maxTrailerAvailabilityOpsDay.coalesce(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "MaxTrailerAvailabilityOpsDay/")         
         
         val maxTrailerAvailabilityQuery = s""" SELECT DISTINCT warehouse_id
                                          ,load_type
                                          ,TRIM(trailer_profile) trailer_profile
                                          ,SUM( trailer_profile_availability ) OVER ( PARTITION BY warehouse_id
                                                                                          ,load_type
                                                                                          ,trailer_profile
                                                                            ) total_trailer_availability_by_loadType
                                          ,SUM( trailer_profile_availability ) OVER ( PARTITION BY warehouse_id
                                                                                          ,trailer_profile
                                                                            ) total_trailer_availability
                                     FROM trailer_availability
                                  ORDER BY warehouse_id, load_type, total_trailer_availability DESC, total_trailer_availability_by_loadType DESC
                               """
         logger.debug( " - Query - " + maxTrailerAvailabilityQuery );
         val maxTrailerAvailability = sqlContext.sql(maxTrailerAvailabilityQuery)   
         //existingLoads.show(40)
        
        maxTrailerAvailability.coalesce(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save(filePath + "MaxTrailerAvailability/")            
    
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing trailerAvailability at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def validateAnalysis(sqlContext: SQLContext, sc: SparkContext) = {

      logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   validateAnalysis              " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
      val currentTime = getCurrentTime();
      val warehouseId = "'29'"
      val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
      val endTime =  addDaystoCurrentDatePv(1)  //getCurrentDate

      try{
        
            val invalidSelectorQuery = s""" SELECT ldbw.warehouse_id
                                        ,ldbw.ops_date
                                        ,ldbw.customer_number
                                        ,ldbw.customer_name
                                        ,ldbw.load_number
                                        ,ldbw.cube_qty
                                        ,ldbw.total_cases
                                        ,ldbw.actual_pallets
                                        ,ldbw.expected_pallets
                                        ,ldbw.selected_pct
                                        ,ldbw.loaded_pct
                                        ,ldbw.closed_timestamp
                                        ,ldbw.set_back_timestamp
                                        ,ldbw.dispatch_timestamp
                                        ,ldbw.ext_timestamp
                                        ,ldbw.gate_status
                                        ,ldbw.loader_employee_id
                                        ,ldbw.loader_name
                                        ,ldbw.trailer_type
                                        ,sdbw.stop_number
                                        ,sdbw.store_number
                                        ,sdbw.store_name
                                        ,sdbw.cs_customer_number
                                        ,sdbw.cs_customer_name
                                        ,concat( sdbw.store_address, ' ', sdbw.store_city, ' ' , sdbw.store_state, ' ', sdbw.store_zip ) store_address
                                        ,adbw.assignment_id
                                        ,adbw.assignment_type
                                        ,adbw.assgn_start_time
                                        ,adbw.assgn_end_time
                                        ,adbw.bill_qty
                                        ,adbw.cases_selected
                                        ,adbw.scr_qty
                                        ,adbw.number_of_cubes
                                        ,adbw.employee_id
                                        ,adbw.employee_name
                                        ,adbw.is_load_deferred
                                        ,adbw.selected_pct
                                        ,adbw.snapshot_id
                                    FROM load_details_by_warehouse ldbw
                                        ,stop_details_by_warehouse sdbw
                                        ,assignment_details_by_warehouse adbw
                                  WHERE ldbw.warehouse_id = sdbw.warehouse_id
                                    AND ldbw.ops_date = sdbw.ops_date
                                    AND ldbw.load_number = sdbw.load_number
                                    AND sdbw.warehouse_id = adbw.warehouse_id
                                    AND sdbw.ops_date = adbw.ops_date
                                    AND sdbw.load_number = adbw.load_number
                                    AND sdbw.stop_number = adbw.stop_number
                                    AND adbw.merged_status = 'N'
                                    AND ( adbw.employee_id IN ( '0000111111', '0000000000') 
                                         OR adbw.employee_id IS NULL 
                                         )
                                    AND adbw.cases_selected > 0
                                    AND ldbw.ops_date >= '$startTime'
                                    AND ldbw.ops_date <= '$endTime'
                                ORDER BY  ldbw.warehouse_id
                                         ,ldbw.ops_date DESC
                                         ,ldbw.load_number
                                         ,sdbw.stop_number
                                         ,adbw.assignment_id
                                  """

       logger.debug( " - Query - " + invalidSelectorQuery );
       val invalidSelector = sqlContext.sql(invalidSelectorQuery)     
       logger.debug( " - Query fired " ) 
       //queryData3.show
       
       invalidSelector.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "InvalidSelectors/")  
       
            val invalidTrailerQuery = s""" SELECT ldbw.warehouse_id
                                        ,ldbw.ops_date
                                        ,ldbw.customer_number
                                        ,ldbw.customer_name
                                        ,ldbw.load_number
                                        ,ldbw.cube_qty
                                        ,ldbw.total_cases
                                        ,ldbw.actual_pallets
                                        ,ldbw.expected_pallets
                                        ,ldbw.selected_pct
                                        ,ldbw.loaded_pct
                                        ,ldbw.closed_timestamp
                                        ,ldbw.set_back_timestamp
                                        ,ldbw.dispatch_timestamp
                                        ,ldbw.ext_timestamp
                                        ,ldbw.gate_status
                                        ,ldbw.loader_employee_id
                                        ,ldbw.loader_name
                                        ,ldbw.trailer_type
                                        ,LENGTH(ldbw.trailer_type) lenght_trailer_type
                                    FROM load_details_by_warehouse ldbw
                                   WHERE TRIM(ldbw.trailer_type) IS NULL OR TRIM(ldbw.trailer_type) = ''
                                     AND ldbw.ops_date >= '$startTime'
                                     AND ldbw.ops_date <= '$endTime'
                                 ORDER BY ldbw.warehouse_id
                                         ,ldbw.ops_date DESC
                                         ,ldbw.load_number 
                                  """
        // ldbw.ops_date >= '$startTime'
                                    //AND ldbw.ops_date <= '$endTime'
                                    //AND
       logger.debug( " - Query - " + invalidTrailerQuery );
       val invalidTrailer = sqlContext.sql(invalidTrailerQuery)     
       logger.debug( " - Query fired " ) 
       //invalidTrailer.show
       
       invalidTrailer.coalesce(1).write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save(filePath + "InvalidTrailers/")       
        
      }
      catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing validateAnalysis at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def copyData(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      copyData                      " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{ 
         val copyData1 = """ SELECT * FROM load_loaders_by_warehouse WHERE update_status = 'Y'"""
         val copyData2 = """ SELECT * FROM load_percent_by_warehouse WHERE update_status = 'Y'"""
         
         logger.debug( " - Query - " + copyData1 );
         val queryData = sqlContext.sql(copyData1)     
         logger.debug( " - Query fired " ) 
         queryData.show(40)
         
         queryData.write
         .format("org.apache.spark.sql.cassandra")
         .option("keyspace", "wip_shipping_ing")
         .option("table", "load_loaders_by_warehouse")
         .mode("append")
         .save()
         
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing copyData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
    
  def getData(sqlContext: SQLContext) = {
    logger.debug( "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      getData                       " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    val recordDate = addDaystoCurrentDatePv(-1)
    val previousTime = addDaystoCurrentDatePv(-1)
    logger.debug( " - recordDate - " + recordDate )
    try{ 
         
         val qDistinctCustomerQty = """ SELECT DISTINCT wsd.whse_nbr
                            ,first_value(wsd.ent_date) ent_date
                            ,concat( closed_date, ' ', closed_time ) closed_date
                            ,ldbe.ops_date
                            ,wsd.chain customer_number
                            ,wsd.chain_name customer_name
                            ,wsd.ext_time
                            ,SUM(SEL_QTY)  selected_qty
                            ,SUM(BILL_QTY) billed_qty
                            ,SUM(SCR_QTY)  scratched_qty
                            ,( SUM(BILL_QTY) - SUM(SEL_QTY) - SUM(SCR_QTY) ) cases_remaining
                            ,COUNT(DISTINCT load_nbr) total_loads
                        FROM warehouse_shipping_data wsd
                        JOIN load_details_by_entry_date ldbe
                          ON wsd.whse_nbr = ldbe.warehouse_id
                             AND wsd.min_ent_date = ldbe.entry_date
                             AND wsd.load_nbr = ldbe.load_number
                       WHERE wsd.whse_nbr = '76'
                         AND ldbe.ops_date = '2016-12-04 05:00:00+0000'
                         AND chain IN ( '00001','00028','00081' )
                    GROUP BY wsd.whse_nbr
                            ,ldbe.ops_date
                            ,wsd.chain 
                            ,wsd.chain_name
                            ,wsd.ext_time
                            ,concat( wsd.closed_date, ' ', wsd.closed_time ) 
                  """
         
         //WHERE ent_date IN ( '20161209', '20161210', '20161211', '20161212', '20161213', '20161214')
         val qMultipleEntDate = """WITH base_data AS 
                                     ( SELECT wsd.whse_nbr
                                           ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_nbr 
                                           ,COUNT(DISTINCT ent_date) ent_date_cnt
                                      FROM warehouse_shipping_data wsd
                                    GROUP BY wsd.whse_nbr
                                            ,concat( '"',CAST( wsd.load_nbr as string) ,'"' )
                                      )
                                    SELECT whse_nbr
                                          ,load_nbr 
                                          ,ent_date_cnt
                                      FROM base_data bd
                                     WHERE bd.ent_date_cnt > 1
                                   ORDER BY whse_nbr, load_nbr 
                                 """
         
         val qMultipleDispDate = """WITH base_data AS 
                                     ( SELECT wsd.whse_nbr
                                           ,wsd.ent_date
                                           ,concat( wsd.ext_date, ' ', wsd.ext_time ) ext_date_time
                                           ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_nbr
                                           ,COUNT(DISTINCT wsd.disp_date) disp_date_cnt
                                           ,COUNT(DISTINCT wsd.disp_time) disp_time_cnt  
                                      FROM warehouse_shipping_data wsd
                                     WHERE com_code NOT IN ('SHU', 'HB' )
                                  GROUP BY wsd.whse_nbr
                                          ,wsd.ent_date
                                          ,concat( wsd.ext_date, ' ', wsd.ext_time )
                                          ,wsd.load_nbr
                                      )
                                    SELECT whse_nbr
                                          ,ent_date
                                          ,load_nbr
                                          ,ext_date_time 
                                      FROM base_data bd
                                     WHERE bd.disp_date_cnt > 1 OR disp_time_cnt > 1
                                 """     
         
         val qVerifyHBSHU = """ SELECT COUNT(1) 
                                  FROM warehouse_shipping_data
                                 WHERE com_code IN ('SHU', 'HB' )
                            """
         //logger.debug( " - qMultipleEntDate Query - " + "\n" + qMultipleEntDate)
         //val dMultipleEntDate = sqlContext.sql(qMultipleEntDate)
         //distinctCounts.saveAsTable("wsh_distinct_counts")
         //dMultipleEntDate.registerTempTable("wsh_multiple_ent_date")
         //sqlContext.cacheTable("wsh_multiple_ent_date")
         //logger.debug( " - wsh_multiple_ent_date table registered" )
         
         val qLoadDetails = """ SELECT ldbw.warehouse_id
                                      ,concat( '"',CAST( ldbw.load_number as string) ,'"' ) load_number1
                                      ,ldbw.*
                                  FROM load_details_by_warehouse ldbw
                                 WHERE ldbw.warehouse_id = '29'
                                   AND ops_date = '2016-12-13 05:00:00+0000'  
                                   AND load_number IN ('000000049331', '000000049330' )
                 """
         /*
          * wmed.whse_nbr
                                      ,wmed.load_nbr
                                      ,
          * JOIN wsh_multiple_ent_date wmed
                                     ON ldbw.load_number = wmed.load_nbr
                                        AND ldbw.warehouse_id = wmed.whse_nbr
          */
          
         val qStopDetails = """ SELECT * 
                        FROM stop_details_by_warehouse
                       WHERE warehouse_id = '29'
                         AND ops_date = '2016-12-11 05:00:00+0000'
                         AND load_number IN ('000000029877','000000029876','000000029850','000000029849'
                        ,'000000029848','000000029847','000000029846','000000029845','000000029844'
                        ,'000000029843','000000029842','000000029841','000000029840','000000029839'
                        ,'000000029838','000000029837','000000029836','000000029835','000000029832'
                        ,'000000029831','000000029830')
                  """
         
         val qAssignmentDetails = """ SELECT * 
                        FROM assignment_details_by_warehouse
                       WHERE warehouse_id = '44'
                         AND assignment_id IN ('077551', '077552', '854546','854548')
                  """
         
         val qData = """ SELECT wsd.whse_nbr
                            ,wsd.ent_date ent_date
                            ,ldbe.ops_date
                            ,ldbe.dispatch_date master_disp_date
                            ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_number
                            ,wsd.chain customer_number
                            ,wsd.chain_name customer_name
                            ,concat( closed_date, ' ', closed_time ) closed_date_time
                            ,wsd.closed_date
                            ,wsd.closed_time
                            ,wsd.disp_date
                            ,wsd.disp_time
                            ,concat( '"',wsd.ext_time,'"') ext_time
                            ,wsd.cust_nbr
                            ,concat( '"',CAST( wsd.stop_nbr as string) ,'"' ) stop_number
                            ,concat( '"',CAST( wsd.assgn_nbr as string) ,'"' ) assgn_number
                            ,wsd.trailr_type
                            ,wsd.trailr_nbr
                            ,concat( '"',CAST( wsd.emp_id as string ) ,'"' ) emp_id
                            ,wsd.SEL_QTY  selected_qty
                            ,wsd.BILL_QTY billed_qty
                            ,wsd.SCR_QTY  scratched_qty 
                            ,wsd.cube                          
                            ,wsd.*
                        FROM warehouse_shipping_data wsd
                        JOIN load_details_by_entry_date ldbe
                          ON wsd.whse_nbr = ldbe.warehouse_id
                             AND wsd.min_ent_date = ldbe.entry_date
                             AND wsd.load_nbr = ldbe.load_number
                       WHERE whse_nbr = '29'   
                         AND ent_date = '20161215' 
                         AND wsd.load_nbr IN ('000000069582' ) 
                    ORDER BY whse_nbr, ops_date, load_number,  stop_number, assgn_number                      
                  """       
         
         /*AND emp_id IN ( '000000000', '0000000000', '000111111' )
          *                             ,CAST ( wsd.disp_date as date ) disp_date_2
                            ,CAST( wsd.disp_time as timestamp ) disp_time_2
          */
         val qWhsPallets = """ SELECT wsd.whse_nbr
                            ,wsd.min_ent_date
                            ,ldbe.ops_date
                            ,concat( wsd.closed_date, ' ', wsd.closed_time ) closed_date_time
                            ,cast( emp_id AS int ) emp_id
                            ,COUNT(DISTINCT load_number) load_number_counts
                            ,FLOOR( SUM(SEL_QTY) / 70 ) actual_pallets
                            ,FLOOR( SUM(SEL_QTY) ) selected_qty
                        FROM warehouse_shipping_data wsd
                        JOIN load_details_by_entry_date ldbe
                          ON wsd.whse_nbr = ldbe.warehouse_id
                             AND wsd.min_ent_date = ldbe.entry_date
                             AND wsd.load_nbr = ldbe.load_number
                       WHERE wsd.whse_nbr = '76'
                         AND wsd.ent_date = '20161204'
                    GROUP BY wsd.whse_nbr
                            ,wsd.min_ent_date
                            ,ldbe.ops_date
                            ,concat( wsd.closed_date, ' ', wsd.closed_time )
                            ,wsd.emp_id
                    ORDER BY wsd.whse_nbr, ldbe.ops_date
                  """
         
         val qDistinctLoads = """ SELECT DISTINCT wsd.whse_nbr, wsd.ent_date
                                        ,ldbe.ops_date
                                        , concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_nbr
                        FROM warehouse_shipping_data wsd
                        JOIN load_details_by_entry_date ldbe
                            ON wsd.whse_nbr = ldbe.warehouse_id
                             AND wsd.min_ent_date = ldbe.entry_date
                             AND wsd.load_nbr = ldbe.load_number
                        LEFT OUTER JOIN employee_details_by_warehouse edbw
                            ON wsd.whse_nbr = CAST( edbw.warehouse_id as int )
                               AND cast( wsd.emp_id as int) = edbw.employee_number
                               AND ldbe.ops_date = edbw.shift_date
                        LEFT OUTER JOIN wsh_multiple_ent_date wmed
                            ON 
                  """
                        
         val qDistinctEmployees = """ SELECT DISTINCT edbw.warehouse_id, edbw.shift_date, edbw.shift_id, edbw.employee_number
                        FROM employee_details_by_warehouse edbw
                        WHERE edbw.warehouse_id = '76'
                          AND edbw.employee_number IN ()
                  """
         
         val qDistinctTestLoads = """ SELECT DISTINCT wsd.whse_nbr warehouse_id
                                       ,ldbe.ops_date
                                       ,wsd.min_ent_date
                                       ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_number
                                       ,wsd.chain customer_number
                                       ,wsd.chain_name customer_name
                                       ,wsd.closed_date
                                       ,wsd.closed_time
                                       ,wsd.disp_date
                                       ,wsd.disp_time
                                       ,CAST ( wsd.disp_date as date ) disp_date_2
                                       ,CAST( wsd.disp_time as timestamp ) disp_time_2
                                       ,trailr_type trailer_type                                       
                                  FROM warehouse_shipping_data wsd
                                  JOIN load_details_by_entry_date ldbe 
                                    ON wsd.whse_nbr = ldbe.warehouse_id
                                      AND wsd.min_ent_date = ldbe.entry_date
                                      AND wsd.load_nbr = ldbe.load_number
                                      AND wsd.whse_nbr IN ('29', '76', '44' )   
                                 ORDER BY warehouse_id, ops_date, load_number, min_ent_date, customer_number, trailer_type
                                  """
         
         val qSetBackTimeMultipleCustomers = 
                  """ SELECT *
                        FROM load_details_by_warehouse ldbw
                       WHERE customer_name = 'Multiple' OR set_back_time IS NOT NULL
                 """

         val qTrailerCustomerDataQuery =  """ SELECT DISTINCT wsd.whse_nbr warehouse_id
                                       ,ldbe.ops_date
                                       ,wsd.chain customer_number
                                       ,wsd.chain_name customer_name
                                       ,trailr_type trailer_type
                                       ,wsd.load_nbr
                                  FROM warehouse_shipping_data wsd
                                  JOIN load_details_by_entry_date ldbe
                                      ON wsd.whse_nbr = ldbe.warehouse_id
                                         AND wsd.min_ent_date = ldbe.entry_date
                                         AND wsd.load_nbr = ldbe.load_number
                                  """
         val qMultipleAssignments = """ SELECT wsd.whse_nbr warehouse_id
                                              ,wsd.ent_date
                                              ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_nbr
                                              ,concat( '"',CAST( wsd.stop_nbr as string ),'"' ) stop_nbr
                                              ,concat( '"',CAST( wsd.assgn_nbr as string ),'"' ) assgn_nbr
                                              ,wsd.ext_time
                                              ,COUNT(1) assgn_nbr_cnt
                                          FROM warehouse_shipping_data wsd
                                         GROUP BY wsd.whse_nbr
                                              ,wsd.ent_date
                                              ,wsd.load_nbr
                                              ,wsd.stop_nbr
                                              ,wsd.assgn_nbr
                                              ,wsd.ext_time
                                        HAVING COUNT(1) > 1
                                      ORDER BY warehouse_id, load_nbr, stop_nbr, assgn_nbr, ent_date 
                                    """
         
         val qDataWSH = """ SELECT wsd.whse_nbr
                            ,wsd.ent_date ent_date
                            ,wsd.min_ent_date 
                            ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_number
                            ,wsd.chain customer_number
                            ,wsd.chain_name customer_name
                            ,concat( closed_date, ' ', closed_time ) closed_date_time
                            ,wsd.closed_date
                            ,wsd.closed_time
                            ,wsd.disp_date
                            ,wsd.disp_time
                            ,wsd.ext_time
                            ,wsd.cust_nbr
                            ,concat( '"',CAST( wsd.stop_nbr as string ),'"' ) stop_number
                            ,concat( '"',CAST( wsd.assgn_nbr as string ),'"' ) assgn_number
                            ,wsd.trailr_type
                            ,wsd.trailr_nbr
                            ,concat( '"',CAST( wsd.emp_id as string ),'"' ) emp_id
                            ,wsd.SEL_QTY  selected_qty
                            ,wsd.BILL_QTY billed_qty
                            ,wsd.SCR_QTY  scratched_qty                            
                            ,wsd.*
                        FROM warehouse_shipping_data wsd
                       WHERE whse_nbr = '29'    
                         AND load_nbr = '000000049338'
                    ORDER BY whse_nbr, load_number,  stop_number, assgn_number                       
                  """ 
         
         val qTotalCount = """ SELECT COUNT(1) FROM warehouse_shipping_data """
         //emp_id IN ( '000000000', '0000000000', '000111111' ) ,'29','44'
         //AND sel_qty > 0 
         //AND ent_date = '20161213'  
         //                            ,CAST( wsd.disp_date as date) disp_date_2
                            //,CAST( wsd.disp_date as timestamp) disp_time_2
         
         val missingTrailerType = """ SELECT * FROM yms_by_warehouse WHERE profile IS NULL """
         
          val dateTimeStamp = """ SELECT whse_nbr, ent_date, load_nbr
                                        ,ext_date, ext_time
                                        ,unix_timestamp( wsd.ext_date, 'yyyyMMdd' ) ext_date_unix
                                        ,TO_DATE( CAST( unix_timestamp( wsd.ext_date, 'yyyyMMdd' ) as Timestamp ) ) ext_date_todate
                                        ,from_unixtime( unix_timestamp( wsd.ext_date, 'yyyyMMdd' ) ) ext_date_fromunix
                                        ,unix_timestamp( wsd.ext_time, 'HHmmss' ) ext_time_unix
                                        ,TO_DATE( CAST( unix_timestamp( wsd.ext_time, 'HHmmss' ) as Timestamp ) ) ext_time_todate
                                        ,from_unixtime( unix_timestamp( wsd.ext_time, 'HHmmss' ) ) ext_time_fromunix
                                        ,from_unixtime( unix_timestamp( concat( wsd.ext_date, ' ', wsd.ext_time ), 'yyyyMMdd HHmmss' ) ) ext_datetime_fromunix
                                        ,TO_DATE( CONCAT( CAST( unix_timestamp( wsd.ext_date, 'yyyyMMdd' ) as Timestamp ), ' ',  CAST( unix_timestamp( wsd.ext_time, 'HHmmss' ) as Timestamp ) 
                                                        ) ) ext_datetime_todate
                                    FROM warehouse_shipping_data wsd
                                   WHERE whse_nbr = '76'
                                     AND ent_date = '20161221'
                                     AND load_nbr = '000000576447' 
                          """
          /*
           * ,unix_timestamp( wsd.ext_date, 'dd-MMM-YYYY' ) ext_date1
           */
         val loadLoadersMaxGateTime = """ SELECT warehouse_id, MAX( gate_datetime) 
                                            FROM load_loaders_by_warehouse 
                                        GROUP BY warehouse_id
                                            """
         
         val loadPercentMaxGateTime = """ SELECT warehouse_id, MAX( gate_datetime) 
                                            FROM load_percent_by_warehouse 
                                        GROUP BY warehouse_id
                                            """
         
         val loadDetailsMaxDispTime = """ SELECT warehouse_id, MAX( ops_date), MAX( dispatch_timestamp )
                                            FROM load_details_by_warehouse 
                                        GROUP BY warehouse_id
                                            """
         
         val loadDetailsEntDate = """ SELECT COUNT(1) 
                                        FROM load_details_by_entry_date
                                        WHERE warehouse_id = '29'
                                  """
         
         val loadDetails = """ SELECT COUNT(1) 
                                        FROM load_details_by_warehouse
                                        WHERE warehouse_id = '29'
                                  """
         //AND load_number = '000000069083'
         
         val openLoadsScratches = """ SELECT DISTINCT ldb.warehouse_id
                                            ,ldb.ops_date
                                            ,ldb.load_number
                                        FROM load_details_by_warehouse ldb 
                                            ,assignment_details_by_warehouse adb
                                      WHERE ldb.closed_timestamp IS NULL
                                        AND ldb.warehouse_id = adb.warehouse_id
                                        AND ldb.ops_date = adb.ops_date 
                                        AND ldb.load_number = adb.load_number  
                                        AND adb.scr_qty > 0
                                   """
         
         val maxExtTimePrev = s""" SELECT DISTINCT warehouse_id
                                                   ,ops_date ops_date
                                                   ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id ) whs_max_ext_timestamp
                                                   ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id, ops_date ) max_ext_timestamp
                                                   ,hour( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_hour
                                                   ,minute( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_minute
                                                   ,'P' date_type
                                     FROM assignment_details_by_warehouse_prev adwp
                                    WHERE adwp.ext_timestamp < '$extDateTimeSt'
                                      AND warehouse_id = '44'      
                                  """
         val casesRemainingQuery = s""" SELECT warehouse_id
                                        ,ops_date
                                        ,'ALL' customer_number 
                                        ,'ALL' customer_name                                        
                                        ,NVL( SUM(bill_qty) - SUM(cases_selected) - SUM(scr_qty) , 0 ) cases_remaining
                                     FROM assignment_details_by_warehouse
                                    WHERE ( is_load_deferred = 'N' OR is_load_deferred IS NULL )
                                      AND warehouse_id = '44'
                                 GROUP BY warehouse_id
                                        ,ops_date
                                        ,'ALL'
                                        ,'ALL'
                                  UNION
                                  SELECT warehouse_id
                                        ,ops_date
                                        ,customer_number 
                                        ,customer_name                                        
                                        ,NVL( SUM(bill_qty) - SUM(cases_selected) - SUM(scr_qty) , 0 ) cases_remaining
                                     FROM assignment_details_by_warehouse
                                    WHERE ( is_load_deferred = 'N' OR is_load_deferred IS NULL )
                                      AND warehouse_id = '44'
                                 GROUP BY warehouse_id
                                        ,ops_date
                                        ,customer_number
                                        ,customer_name
                                """
         
         val extDateTimes = s""" SELECT DISTINCT warehouse_id
                                       ,ops_date
                                       ,ext_timestamp
                                   FROM assignment_details_by_warehouse
                                 ORDER BY warehouse_id, ops_date, ext_timestamp
                             """
         
         val prevAssignmentData = s""" DISTINCT wot.warehouse_id 
                                    ,wot.ops_date ops_date
                                    ,wot.oracle_start_time
                                    ,adbw.customer_number
                                    ,adbw.customer_name
                                    ,SELECT NVL( SUM( adbw_prev.cases_selected ) OVER ( PARTITION BY adbw_prev.warehouse_id 
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
                                FROM load_details_by_warehouse ldbw 
                                JOIN warehouse_operation_time wot
                                  ON ( wot.warehouse_id = ldbw.warehouse_id
                                       AND wot.ops_date = ldbw.ops_date )
                                LEFT OUTER JOIN assignment_details_by_warehouse_prev adbw_prev
                                  ON ( ldbw.warehouse_id = adbw_prev.warehouse_id
                                     AND ldbw.ops_date = adbw_prev.ops_date
                                     AND ldbw.load_number = adbw_prev.load_number
                                     AND adbw.customer_number IS NOT NULL
                                     )
                            """
          /*,fst.ext_timestamp*/
                                           /*,fst.creation_time
                                  ,fst.snapshot_id
                                  ,fst.houry_cases_status, fst.load_status, fst.validation_status*/
         val extDateTime = s""" SELECT DISTINCT fst.file_id, fst.file_name file_name_tracker_qa_wip_3_asgn
                                  ,SUBSTR( fst.file_name, INSTR( fst.file_name, '.') + 1 ) file_name_date 
                                  ,from_unixtime( unix_timestamp( concat( wsd.ext_date, ' ', wsd.ext_time ), 'yyyyMMdd HHmmss' ) ) ext_timestamp_ship_calc
                                  ,wsd.ext_timestamp ext_timestamp_ship
                                  ,fst.ext_timestamp ext_timestamp_tracker
                                  ,fst.creation_time creation_time_tracker
                                  ,fst.snapshot_id
                                  ,fst.houry_cases_status, fst.load_status, fst.validation_status
                              FROM file_streaming_tracker fst
                                  ,warehouse_shipping_data wsd
                             WHERE fst.snapshot_id = wsd.snapshot_id
                               AND fst.load_status = 'Load_complete'
                             """   
         //'$extDateTime'         
         val negCasesRemaining = s""" WITH base_rows AS 
                                          (  SELECT adbw.*
                                                    ,( bill_qty - ( cases_selected + scr_qty ) ) cases_remaining
                                                FROM assignment_details_by_warehouse adbw
                                                    ,warehouse_operation_time wot
                                               WHERE wot.facility = adbw.warehouse_id
                                                 AND wot.ops_date = adbw.ops_date
                                                 AND '$recordDate' BETWEEN wot.start_time and wot.end_time
                                          ) 
                                        SELECT * FROM base_rows br WHERE br.cases_remaining < 0 
                                    """
         
         val dataValidationAssign = s""" SELECT COUNT(DISTINCT adbw.load_number) 
                                           FROM assignment_details_by_warehouse adbw
                                          WHERE adbw.warehouse_id = '44'
                                            AND ops_date = '2017-02-08 05:00:00+0000'
                                            AND customer_number = '00001'
                                            AND ext_timestamp < '2017-02-08 13:00:00+0000'
                                                """
      
         //adbw.warehouse_id IN ('44')
         //                                           AND distinct_closed_timestamp > 1
         
         val maxExtTimePrevQueryInt = s""" SELECT DISTINCT warehouse_id
                                                         ,ops_date ops_date
                                                         ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id ) whs_max_ext_timestamp
                                                         ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id, ops_date ) max_ext_timestamp
                                                         ,hour( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_hour
                                                         ,minute( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_minute
                                                         ,'PD' date_type
                                                     FROM assignment_details_by_warehouse_prev adwp
                                                    WHERE adwp.ext_timestamp < '2017-02-10 01:07:01'                                                 
                                                """
         
        val maxExtTimePrevQuery = s"""WITH prev_max AS (
                                                   SELECT DISTINCT warehouse_id
                                                         ,ops_date ops_date
                                                         ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id ) whs_max_ext_timestamp
                                                         ,MAX( ext_timestamp ) OVER ( PARTITION BY warehouse_id, ops_date ) max_ext_timestamp
                                                         ,hour( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_hour
                                                         ,minute( MAX(ext_timestamp) OVER ( PARTITION BY warehouse_id, ops_date ) )  ext_timestart_minute
                                                         ,'PD' date_type
                                                     FROM assignment_details_by_warehouse_prev adwp
                                                    WHERE adwp.ext_timestamp < '2017-02-10 01:07:00'                                                 
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
                                                                                                LPAD( pmm.ext_timestart_hour, 2, '0' )
                                                                                           ELSE LPAD( ( pmm.ext_timestart_hour - 1 ), 2, '0' )
                                                                                       END )
                                                                                 ,LPAD( minute(oracle_start_time) , 2 , '0' ) )
                                                                        , 'HHmm' )
                                                          , 'HH:mm' ) ext_hour_bucket
                                           ,pmm.date_type
                                           ,( CASE WHEN minute(wot.oracle_start_time) > "00" THEN  
                                                        hour( wot.oracle_start_time ) + 1
                                                   ELSE hour( wot.oracle_start_time )  
                                               END ) start_hour
                                       FROM warehouse_operation_time wot
                                           ,prev_max pmm
                                      WHERE wot.facility = pmm.warehouse_id
                                        AND wot.ops_date = pmm.ops_date
                                        AND pmm.whs_max_ext_timestamp = pmm.max_ext_timestamp
                                      """       
  val prevhourFile = """ SELECT DISTINCT ext_timestamp 
                          FROM assignment_details_by_warehouse_prev 
                          WHERE warehouse_id = '29' AND ops_date = '2017-02-07 00:00:00' 
                          ORDER BY ext_timestamp DESC"""
  
   val asgnEndDtGTClosedDt = s"""SELECT adbw.warehouse_id, adbw.ops_date ops_date_format_unix
                                          ,adbw.customer_number, customer_name, adbw.load_number
                                          ,adbw.stop_number, adbw.assignment_id
                                          ,closed_timestamp, assgn_start_time
                                          ,assgn_end_time
                             FROM assignment_details_by_warehouse adbw
                            WHERE assgn_end_time >= closed_timestamp
                          ORDER BY warehouse_id, ops_date_format_unix DESC, load_number, stop_number, assignment_id """
   
   val openLoadQuery = s""" SELECT DISTINCT adbw.warehouse_id
                                  ,adbw.ops_date
                                  ,adbw.customer_number
                                  ,adbw.customer_name
                                  ,adbw.load_number
                                  ,adbw.closed_timestamp
                                  ,adbw.is_load_deferred
                                  ,adbw.merged_status
                                  ,ldbw.load_number
                              FROM assignment_details_by_warehouse adbw
                              JOIN warehouse_operation_time wot
                                ON wot.facility = adbw.warehouse_id
                                 AND wot.ops_date = adbw.ops_date 
                                 AND '$currentTime' BETWEEN wot.start_time AND wot.end_time
                                 AND adbw.customer_number IS NOT NULL
                              JOIN load_details_by_warehouse ldbw
                                ON ldbw.warehouse_id = adbw.warehouse_id
                                 AND ldbw.ops_date = adbw.ops_date
                                 AND ldbw.load_number = adbw.load_number
                                 AND ldbw.closed_timestamp IS NULL
                           ORDER BY adbw.warehouse_id
                                   ,adbw.ops_date
                                   ,adbw.load_number
                                          """
      val openLoadLoadsQuery = s""" SELECT DISTINCT ldbw.warehouse_id
                                  ,ldbw.ops_date
                                  ,ldbw.customer_number
                                  ,ldbw.customer_name
                                  ,ldbw.load_number
                                  ,ldbw.closed_timestamp
                                  ,ldbw.is_load_deferred
                              FROM load_details_by_warehouse ldbw
                              JOIN warehouse_operation_time wot
                                ON wot.facility = ldbw.warehouse_id
                                 AND wot.ops_date = ldbw.ops_date 
                                 AND '$currentTime' BETWEEN wot.start_time AND wot.end_time
                                 AND ldbw.customer_number IS NOT NULL
                           ORDER BY ldbw.warehouse_id
                                   ,ldbw.ops_date
                                   ,ldbw.load_number
                                          """                                 
         val maxHistoryDate = """ SELECT whse_nbr, MAX( ext_timestamp ) 
                                    FROM warehouse_shipping_data_history
                                GROUP BY whse_nbr
                              """
         
         val assgnHistory = """ SELECT wsdh.whse_nbr
                                      ,wsdh.snapshot_id
                                      ,wsdh.load_nbr
                                      ,wsdh.stop_nbr
                                      ,wsdh.assgn_nbr
                                      ,wsdh.assgn_start_date
                                      ,wsdh.assgn_start
                                      ,wsdh.assgn_end_date
                                      ,wsdh.assgn_end
                                      ,from_unixtime( unix_timestamp( concat( wsdh.assgn_end_date, ' ', wsdh.assgn_end ), 'yyyyMMdd HHmmss' ) ) assgn_timestamp
                                      ,wsdh.closed_timestamp
                                      ,wsdh.ext_timestamp orig_ext_timestamp
                                      ,CASE WHEN ( ( from_unixtime( unix_timestamp( concat( wsdh.assgn_end_date, ' ', wsdh.assgn_end ), 'yyyyMMdd HHmmss' ) )  > from_unixtime( unix_timestamp( concat( wsdh.assgn_start_date, ' ', wsdh.assgn_start ), 'yyyyMMdd HHmmss' ) )
                                                   AND wsdh.assgn_end_date <> '00000000'                                                     
                                                   AND from_unixtime( unix_timestamp( concat( wsdh.assgn_end_date, ' ', wsdh.assgn_end ), 'yyyyMMdd HHmmss' ) ) < wsdh.ext_timestamp 
                                                   ) 
                                                  OR wsdh.closed_timestamp < wsdh.ext_timestamp ) THEN
                                                 CASE WHEN ( from_unixtime( unix_timestamp( concat( wsdh.assgn_end_date, ' ', wsdh.assgn_end ), 'yyyyMMdd HHmmss' ) )  > from_unixtime( unix_timestamp( concat( wsdh.assgn_start_date, ' ', wsdh.assgn_start ), 'yyyyMMdd HHmmss' ) ) 
                                                            AND wsdh.assgn_end_date <> '00000000'                                                             
                                                            AND from_unixtime( unix_timestamp( concat( wsdh.assgn_end_date, ' ', wsdh.assgn_end ), 'yyyyMMdd HHmmss' ) ) < wsdh.closed_timestamp 
                                                             ) THEN
                                                           from_unixtime( unix_timestamp( concat( wsdh.assgn_end_date, ' ', wsdh.assgn_end ), 'yyyyMMdd HHmmss' ) ) 
                                                      ELSE wsdh.closed_timestamp
                                                  END 
                                            ELSE wsdh.ext_timestamp
                                        END ext_timestamp 
                                      ,wsdh.chain
                                      ,wsdh.chain_name
                                      ,wsdh.bill_qty
                                      ,wsdh.cube
                                      ,wsdh.sel_cube
                                      ,wsdh.sel_qty
                                      ,wsdh.scr_qty
                                  FROM warehouse_shipping_data_history wsdh
                                WHERE whse_nbr = '44'
                               ORDER BY ext_timestamp DESC
                               """  
         val getMinute = s""" SELECT minute('2017-02-15 00:00:39') FROM  assignment_details_by_warehouse LIMIT 1"""
         
         val employeeDetailsVerif = s""" SELECT warehouse_id, MAX(shift_date)
                                           FROM employee_details_by_warehouse
                                          GROUP BY warehouse_id 
                                      """
         val closedLoads = """SELECT COUNT(DISTINCT load_number)
                                FROM load_details_by_warehouse ldb 
                               WHERE warehouse_id = '29'
                                 AND closed_timestamp IS NOT NULL
                                 AND ops_date  = '2017-02-22'  """
         val closedLoadsAssgn = """ SELECT COUNT(DISTINCT load_number)
                                      FROM assignment_details_by_warehouse adb
                                     WHERE warehouse_id = '29'
                                       AND closed_timestamp IS NOT NULL
                                       AND ops_date  = '2017-02-22'"""
         
         val openLoadsMinus =  s""" SELECT DISTINCT adbw.warehouse_id
                                          ,adbw.ops_date
                                          ,adbw.customer_number
                                          ,adbw.customer_name
                                          ,adbw.load_number
                                          ,adbw.closed_timestamp
                                          ,adbw.is_load_deferred
                                          ,adbw.date_type
                                      FROM assignment_details_by_warehouse_prev adbw
                                          ,load_details_by_warehouse ldbw
                                     WHERE adbw.customer_number IS NOT NULL
                                       AND ldbw.warehouse_id = adbw.warehouse_id
                                       AND ldbw.ops_date = adbw.ops_date
                                       AND ldbw.load_number = adbw.load_number
                                       AND ( adbw.is_load_deferred = 'N' OR adbw.is_load_deferred IS NULL )
                                       AND adbw.closed_timestamp IS NULL 
                                       AND adbw.warehouse_id = '29'
                                       AND ops_date = '2017-02-22'
                                     MINUS
                                    SELECT DISTINCT adbw.warehouse_id
                                          ,adbw.ops_date
                                          ,adbw.customer_number
                                          ,adbw.customer_name
                                          ,adbw.load_number
                                          ,adbw.closed_timestamp
                                          ,adbw.is_load_deferred
                                          ,adbw.date_type
                                      FROM assignment_details_by_warehouse_prev adbw
                                          ,load_details_by_warehouse ldbw
                                     WHERE adbw.customer_number IS NOT NULL
                                       AND ldbw.warehouse_id = adbw.warehouse_id
                                       AND ldbw.ops_date = adbw.ops_date
                                       AND ldbw.load_number = adbw.load_number
                                       AND ( adbw.is_load_deferred = 'N' OR adbw.is_load_deferred IS NULL )
                                       AND adbw.closed_timestamp IS NOT NULL 
                                       AND adbw.warehouse_id = '29'
                                       AND ops_date = '2017-02-22'
                                          """
         val loadLoaderPercentQuery = """ SELECT * 
                                       FROM load_percent_by_warehouse 
                                      WHERE warehouse_id = '29'
                                        AND load_percent > 100 """
         
         val loadLoaderQuery = """ SELECT * 
                                       FROM load_loaders_by_warehouse 
                                      WHERE warehouse_id = '29'
                                        AND pallet_ovrld_to_load = '000000029822' """       
         
         val casesData = """ SELECT edbw.warehouse_id
                                   ,edbw.shift_date 
                                   ,edbw.employee_number
                                  ,edbw.employee_name
                                  ,edbw.employee_type
                                  ,edbw.active_loads
                                  ,edbw.actual_hours
                                  ,edbw.hired_days_nbr
                                  ,edbw.home_supervisor_id
                                  ,edbw.home_supervisor_name
                               FROM employee_details_by_warehouse edbw

                        """
         
         val leadLagTest = """ WITH base_data AS (
                              SELECT DISTINCT wsdh.warehouse_id 
                                    ,wsdh.ops_date ops_date
                                    ,wsdh.customer_number
                                    ,wsdh.customer_name
                                    ,wsdh.load_number
                                    ,NVL( SUM( wsdh.cases_selected ) OVER ( PARTITION BY wsdh.warehouse_id 
                                                                      ,wsdh.customer_number
                                                                      ,wsdh.ops_date
                                                                      ,wsdh.load_number
                                                              ), 0 ) 
                                                      sel_qty
                                    ,NVL( SUM( wsdh.bill_qty ) OVER ( PARTITION BY wsdh.warehouse_id 
                                                                      ,wsdh.customer_number
                                                                      ,wsdh.ops_date
                                                                      ,wsdh.load_number
                                                         ), 0 ) 
                                                      bill_qty
                                    ,NVL( SUM( wsdh.scr_qty ) OVER ( PARTITION BY wsdh.warehouse_id 
                                                                      ,wsdh.customer_number
                                                                      ,wsdh.ops_date
                                                                      ,wsdh.load_number
                                                         ), 0 ) 
                                                    scr_qty
                                FROM assignment_details_by_warehouse wsdh
                                WHERE warehouse_id = '76' 
                                  AND ops_date = '2017-02-28'
                                  AND customer_number = '00081'
                             ORDER BY warehouse_id ,ops_date
                                          ,customer_number
                                          ,load_number
                                ) 
                                SELECT warehouse_id 
                                      ,bd.ops_date ops_date
                                      ,bd.customer_number
                                      ,bd.customer_name
                                      ,bd.load_number
                                      ,bd.sel_qty - NVL( SUM( bd.sel_qty ) OVER 
                                            ( PARTITION BY warehouse_id ,ops_date
                                                          ,customer_number
                                                  ORDER BY warehouse_id ,ops_date
                                                      ,customer_number
                                                      ,load_number
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ), 0 )  sel_qty
                                      ,bd.bill_qty - NVL( SUM( bd.bill_qty ) OVER 
                                            ( PARTITION BY warehouse_id ,ops_date
                                                          ,customer_number
                                                  ORDER BY warehouse_id ,ops_date
                                                      ,customer_number
                                                      ,load_number
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING  ), 0 ) bill_qty
                                      ,bd.scr_qty - NVL( SUM( bd.scr_qty ) OVER 
                                            ( PARTITION BY warehouse_id ,ops_date
                                                          ,customer_number
                                                  ORDER BY warehouse_id ,ops_date
                                                      ,customer_number
                                                      ,load_number
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING  ), 0 ) scr_qty
                                  FROM base_data bd
                                 ORDER BY warehouse_id ,ops_date
                                          ,customer_number
                                          ,load_number
                                   """
         
          val LoaderPerformance =  s""" SELECT ldbw.warehouse_id
                                              ,ldbw.ops_date
                                              ,ldbw.load_number
                                              ,ldbw.loader_employee_id
                                              ,ldbw.loader_name 
                                              ,edbw.employee_number
                                              ,epbw.employee_id 
                                              ,ldbw.closed_timestamp
                                              ,edbw.active_loads
                                              ,edbw.closed_loads
                                              ,edbw.pallets_per_hour
                                              ,SUM( CASE WHEN closed_timestamp IS NULL THEN 1 ELSE 0 END ) 
                                                              OVER ( PARTITION BY ldbw.warehouse_id
                                                                        ,ldbw.ops_date
                                                                        ,ldbw.loader_employee_id
                                                                    ) actual_active_loads 
                                              ,SUM( CASE WHEN closed_timestamp IS NOT NULL THEN 1 ELSE 0 END ) 
                                                              OVER ( PARTITION BY ldbw.warehouse_id
                                                                        ,ldbw.ops_date
                                                                        ,ldbw.loader_employee_id
                                                                    ) actual_closed_loads
                                              ,( ( NVL( SUM( cube_qty ) 
                                                              OVER ( PARTITION BY ldbw.warehouse_id
                                                                        ,ldbw.ops_date
                                                                        ,ldbw.loader_employee_id
                                                                    )
                                                            , 0 )
                                                        ) / 70 
                                                     ) / MAX( epbw.work_hours ) OVER ( PARTITION BY ldbw.warehouse_id
                                                                                                   ,ldbw.ops_date
                                                                                                   ,ldbw.loader_employee_id
                                                                                      )  actual_pallets_per_hour
                                              ,edbw.shift_date
                                              ,edbw.start_time
                                              ,edbw.end_time
                                              ,edbw.work_hours details_work_hour
                                              ,epbw.report_date
                                              ,epbw.work_hours punches_work_hour
                                              ,ldbw.actual_pallets
                                              ,ldbw.expected_pallets
                                              ,ldbw.cube_qty
                                              ,edbw.cases_selected
                                          FROM load_details_by_warehouse ldbw
                                          LEFT OUTER JOIN employee_details_by_warehouse edbw
                                              ON ldbw.warehouse_id = edbw.warehouse_id
                                                 AND ldbw.ops_date = edbw.shift_date
                                                 AND ldbw.loader_employee_id = edbw.employee_number
                                          LEFT OUTER JOIN employee_punches_by_warehouse epbw
                                              ON ldbw.warehouse_id = epbw.warehouse_id
                                                 AND edbw.start_time = epbw.report_date
                                                 AND ldbw.loader_employee_id = epbw.employee_id  
                                         WHERE ldbw.warehouse_id = '29'
                                           AND ldbw.ops_date > '2017-02-22' 
                                           AND ldbw.ops_date < '2017-03-03' 
                                           AND ldbw.loader_employee_id IS NOT NULL
                                      ORDER BY warehouse_id, ops_date DESC, loader_employee_id, closed_timestamp, load_number
                                    """       
         //AND load_number = '000000039332'
         logger.debug( " - Query - " + qAssignmentDetails );
         val loadLoader = sqlContext.sql(qAssignmentDetails)     
         logger.debug( " - Query fired " ) 
         loadLoader.show(40)
         
         /*logger.debug( " - Query - " + closedLoadsAssgn );
         val queryData1 = sqlContext.sql(closedLoadsAssgn)     
         logger.debug( " - Query fired " ) 
         queryData1.show(40)*/
         /*queryData.write
         .format("org.apache.spark.sql.cassandra")
         .option("keyspace", "wip_shipping_ing")
         .option("table", "load_loaders_by_warehouse")
         .mode("append")
         .save()*/
         
         /*loadLoader.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save( filePath + "LoaderPerformance") */
				
         /*logger.debug( " - Query - " + openClosedLoads );
         val queryData1 = sqlContext.sql(openClosedLoads)     
         logger.debug( " - Query fired 1 " ) 
         queryData1.show(40)*/
         
         /*queryData1.write
         .format("org.apache.spark.sql.cassandra")
         .option("keyspace", "wip_shipping_ing")
         .option("table", "load_percent_by_warehouse")
         .mode("append")
         .save()*/
         
         //sqlContext.sql(loadDetails).show()
         
         /*queryData1.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "CompleteAssignments")  */
         
         //sqlContext.uncacheTable("wsh_multiple_ent_date")
         //logger.debug( " - wsh_multiple_ent_date table registered" )

         //logger.debug( " - Data Copied at C:/LoadDetails/MultipleEntdate" ) 
      
      }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def validateCustomerHourly(sqlContext: SQLContext
                            ,dfAssgnDtlsWarehouse: DataFrame
                            ) = {
  logger.debug( "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
              + "                validateCustomerHourly              " + "\n" 
              + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
           
  val currentTime = getCurrentTime();
  val awaitDuration = scala.concurrent.duration.Duration(360000, java.util.concurrent.TimeUnit.MILLISECONDS)
  try{ 
    
         val opsDateQuery = s""" SELECT facility, ops_date
                                   FROM warehouse_operation_time
                                  WHERE ops_Date = '2017-02-07 05:00:00+0000'
                                    AND facility = '29'
                        """
         //'$currentTime' BETWEEN start_time AND end_time 
         logger.debug( " - Query - " + opsDateQuery );
         val opsDate = sqlContext.sql(opsDateQuery)     
         logger.debug( " - Query fired " ) 
         opsDate.show(10)
         
         /*val distinctOpsDateQuery = s""" SELECT DISTINCT ops_date FROM warehouse_ops_days"""
         logger.info(" - distinctOpsDateQuery - " + distinctOpsDateQuery )
         val distinctOpsDate = sqlContext.sql(distinctOpsDateQuery) 
         distinctOpsDate.show()*/
         
         /*val OpsDates = "'" + distinctOpsDate.select(distinctOpsDate("ops_date"))
                            .collect()
                            .map{ x => x.getTimestamp(0) }
                            .mkString("','") + "'"
         
         logger.info(" - OpsDates - " + OpsDates )*/
         val dfAssgnDetailsWarehouse = dfAssgnDtlsWarehouse  //.filter(s" ops_date IN ( $OpsDates ) ")
                                       .alias("asgn_det")
                                       .join(opsDate, opsDate("ops_date") === dfAssgnDtlsWarehouse("ops_date") && opsDate("facility") === dfAssgnDtlsWarehouse("warehouse_id") )
                                       .select("asgn_det.*") 

         logger.debug(" - Filtered Assignment Details - ")
         
         dfAssgnDetailsWarehouse.registerTempTable("assignment_details_by_warehouse")
         dfAssgnDetailsWarehouse.persist()         
         logger.debug(" - Persisted Assignment Details - ")      
         
         val completeDataQuery = s""" SELECT * FROM assignment_details_by_warehouse """
         val completeData = sqlContext.sql(completeDataQuery)
         
         completeData.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save( filePath + "CompleteAssignments")
         logger.debug(" - Save Assignment Details - ")  
         
         val allCalculationsQuery = s""" SELECT DISTINCT adbw.warehouse_id
                                        ,adbw.ops_date
                                        ,adbw.customer_number 
                                        ,adbw.customer_name   
                                        ,adbw.is_load_deferred
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
                                        ,NVL( SUM(bill_qty) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.customer_number
                                                                 ,adbw.ops_date
                                                             )
                                            - SUM(cases_selected) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.customer_number
                                                                 ,adbw.ops_date
                                                             )
                                            - SUM(scr_qty) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.customer_number
                                                                 ,adbw.ops_date
                                                             )
                                            , 0 ) cases_remaining
                                    FROM assignment_details_by_warehouse adbw
                                    UNION
                                  SELECT DISTINCT adbw.warehouse_id
                                        ,adbw.ops_date
                                        ,'ALL' customer_number 
                                        ,'ALL' customer_name   
                                        ,adbw.is_load_deferred
                                        ,NVL( SUM( adbw.cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                         ), 0 ) sel_qty
                                        ,NVL( SUM( adbw.bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                          ,adbw.ops_date
                                                             ), 0 ) bill_qty
                                        ,NVL( SUM( adbw.scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                          ,adbw.ops_date
                                                             ), 0 ) scr_qty
                                        ,NVL( SUM(bill_qty)  
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.ops_date
                                                             )
                                            - SUM(cases_selected) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.ops_date
                                                             )
                                            - SUM(scr_qty) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.ops_date
                                                             )
                                            , 0 ) cases_remaining
                                    FROM assignment_details_by_warehouse adbw
                                    """  
         logger.debug( " - Query - " + allCalculationsQuery );
         val allCalculations = sqlContext.sql(allCalculationsQuery)     
         logger.debug( " - Query fired " ) 
         allCalculations.show(30)
         
          val closedLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL ")
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - Closed Loads Current hour - " )
          //closedLoadsCurrFT.show()
          //closedLoadsCurrFT.cache()
          
          val openLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL )  ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads" )
          }
          logger.debug(" - Open Loads Current hour - " )
          //openLoadsCurrFT.show()
          //openLoadsCurrFT.cache()
          
          val allClosedLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse  
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL  ")
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") , dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - All Closed Loads Current hour - " )
          //allClosedLoadsCurrFT.show()
          //allClosedLoadsCurrFT.cache()
          
          val allOpenLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse  
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL ) ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name"), dfAssgnDetailsWarehouse("load_number")  )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads" )
          }
          logger.debug(" - All Open Loads Current hour - " + "\n" )
          
          val totalOpenLoadsFT = openLoadsCurrFT.flatMap { x => allOpenLoadsCurrFT.map { y => y.unionAll(x) } } 
          val totalClosedLoadsFT = closedLoadsCurrFT.flatMap { x => allClosedLoadsCurrFT.map { y => x.unionAll(y) } }
          val totalOpenLoads =  Await.result(totalOpenLoadsFT, awaitDuration )  
          val totalClosedLoads =  Await.result(totalClosedLoadsFT, awaitDuration ) 
          logger.debug(" - Total Closed and Open Loads Current Hour - " )
          totalClosedLoads.show()
          totalOpenLoads.show()
          
          val customerHourly =  allCalculations
          .join(totalClosedLoads, Seq("warehouse_id", "ops_date", "customer_number" ), "left" )  //, "ext_date", "data_for_hour" 
          .join(totalOpenLoads, Seq("warehouse_id", "ops_date", "customer_number" ), "left" )  //,"customer_name"
          .select(allCalculations("warehouse_id"), allCalculations("ops_date"), allCalculations("customer_number"), allCalculations("customer_name")
                 ,allCalculations("is_load_deferred")
                 ,allCalculations("sel_qty"), allCalculations("cases_remaining" ), allCalculations("scr_qty"), allCalculations("bill_qty")
                 ,totalClosedLoads("closed_loads"), totalOpenLoads("open_loads")
                 )
          .orderBy("warehouse_id", "ops_date", "customer_number","customer_name")  //customer_name
          
          customerHourly.show(30)
        
        customerHourly.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "CustomerHourly")
         
  }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing validateCustomerHourly at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def validateExtTime(sqlContext: SQLContext
                     ,dfWarehouseShippingData: DataFrame
                            ) = {
  logger.debug( "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
              + "                validateExtTime                     " + "\n" 
              + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
           
  val currentTime = getCurrentTime();
  try{   
    
      val fileStreamingTrackerQuery = s""" SELECT DISTINCT 
                                   fst.file_id
                                  ,fst.file_name file_name_tracker_qa_wip_3_asgn
                                  ,SUBSTR( fst.file_name, INSTR( fst.file_name, '.') + 1 ) file_name_date 
                                  ,fst.ext_timestamp ext_timestamp_tracker
                                  ,fst.creation_time creation_time_tracker
                                  ,fst.snapshot_id
                                  ,fst.houry_cases_status, fst.load_status, fst.validation_status
                              FROM file_streaming_tracker fst
                             WHERE fst.load_status = 'Load_complete'
                             """   
         
         logger.debug( " - Query - " + fileStreamingTrackerQuery );
         val fileStreamingTracker = sqlContext.sql(fileStreamingTrackerQuery)     
         logger.debug( " - Query fired " ) 
         fileStreamingTracker.show(10)
         
         fileStreamingTracker.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save( filePath + "MultipleEntdate")
         
         val warehouseIds = "'44','76','29'"
         val extTimeData = dfWarehouseShippingData
         .join(fileStreamingTracker, ( dfWarehouseShippingData("snapshot_id") === fileStreamingTracker("snapshot_id")  )  ) //&& dfWarehouseShippingData("whse_nbr") IN lit($warehouseIds)
         .select( fileStreamingTracker("file_id"), fileStreamingTracker("file_name_tracker_qa_wip_3_asgn")
                  ,fileStreamingTracker("file_name_date")
                  ,dfWarehouseShippingData("ext_timestamp").alias("ext_timestamp_ship")
                  ,fileStreamingTracker("ext_timestamp_tracker")
                  ,expr("from_unixtime( unix_timestamp( concat( ext_date, ' ', ext_time ), 'yyyyMMdd HHmmss' ) ) ext_timestamp_ship_calc")
                  ,fileStreamingTracker("snapshot_id")
                  ,fileStreamingTracker("houry_cases_status"),fileStreamingTracker("load_status"),fileStreamingTracker("validation_status")
                 )
        .distinct()
        
        extTimeData.show()
        
        extTimeData.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save( filePath + "loadDetails")
         
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing validateCustomerHourly at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  def validateCustomerHourlyExt(sqlContext: SQLContext
                            ,dfAssgnDtlsWarehouse: DataFrame
                            ) = {
  logger.debug( "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
              + "                validateCustomerHourly              " + "\n" 
              + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
           
  val currentTime = getCurrentTime();
  val awaitDuration = scala.concurrent.duration.Duration(360000, java.util.concurrent.TimeUnit.MILLISECONDS)
  try{ 
    
         val opsDateQuery = s""" SELECT facility, ops_date
                                   FROM warehouse_operation_time
                                  WHERE '$currentTime' BETWEEN start_time AND end_time 
                        """
         logger.debug( " - Query - " + opsDateQuery );
         val opsDate = sqlContext.sql(opsDateQuery)     
         logger.debug( " - Query fired " ) 
         opsDate.show(10)
         
         /*val distinctOpsDateQuery = s""" SELECT DISTINCT ops_date FROM warehouse_ops_days"""
         logger.info(" - distinctOpsDateQuery - " + distinctOpsDateQuery )
         val distinctOpsDate = sqlContext.sql(distinctOpsDateQuery) 
         distinctOpsDate.show()*/
         
         /*val OpsDates = "'" + distinctOpsDate.select(distinctOpsDate("ops_date"))
                            .collect()
                            .map{ x => x.getTimestamp(0) }
                            .mkString("','") + "'"
         
         logger.info(" - OpsDates - " + OpsDates )*/
         val dfAssgnDetailsWarehouse = dfAssgnDtlsWarehouse  //.filter(s" ops_date IN ( $OpsDates ) ")
                                       .alias("asgn_det")
                                       .join(opsDate, opsDate("ops_date") === dfAssgnDtlsWarehouse("ops_date") )
                                       .select("asgn_det.*") 

         logger.debug(" - Filtered Assignment Details - ")
         
         dfAssgnDetailsWarehouse.registerTempTable("assignment_details_by_warehouse")
         dfAssgnDetailsWarehouse.persist()         
         logger.debug(" - Persisted Assignment Details - ")         
         
         val allCalculationsQuery = s""" SELECT DISTINCT adbw.warehouse_id
                                        ,adbw.ops_date
                                        ,adbw.customer_number 
                                        ,adbw.customer_name   
                                        ,adbw.ext_timestamp
                                        ,adbw.is_load_deferred
                                        ,NVL( SUM( adbw.cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.customer_number
                                                                      ,adbw.ops_date
                                                                      ,ext_timestamp
                                                         ), 0 ) sel_qty
                                        ,NVL( SUM( adbw.bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                          ,adbw.customer_number
                                                                          ,adbw.ops_date
                                                                          ,ext_timestamp
                                                             ), 0 ) bill_qty
                                        ,NVL( SUM( adbw.scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                          ,adbw.customer_number
                                                                          ,adbw.ops_date
                                                                          ,ext_timestamp
                                                             ), 0 ) scr_qty
                                        ,NVL( SUM(bill_qty) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.customer_number
                                                                 ,adbw.ops_date
                                                                 ,ext_timestamp
                                                             )
                                            - SUM(cases_selected) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.customer_number
                                                                 ,adbw.ops_date
                                                                 ,ext_timestamp
                                                             )
                                            - SUM(scr_qty) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.customer_number
                                                                 ,adbw.ops_date
                                                                 ,ext_timestamp
                                                             )
                                            , 0 ) cases_remaining
                                    FROM assignment_details_by_warehouse adbw
                                    UNION
                                  SELECT DISTINCT adbw.warehouse_id
                                        ,adbw.ops_date
                                        ,'ALL' customer_number 
                                        ,'ALL' customer_name   
                                        ,adbw.ext_timestamp
                                        ,adbw.is_load_deferred
                                        ,NVL( SUM( adbw.cases_selected ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                      ,adbw.ops_date
                                                                      ,ext_timestamp
                                                         ), 0 ) sel_qty
                                        ,NVL( SUM( adbw.bill_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                          ,adbw.ops_date
                                                                          ,ext_timestamp
                                                             ), 0 ) bill_qty
                                        ,NVL( SUM( adbw.scr_qty ) OVER ( PARTITION BY adbw.warehouse_id 
                                                                          ,adbw.ops_date
                                                                          ,ext_timestamp
                                                             ), 0 ) scr_qty
                                        ,NVL( SUM(bill_qty)  
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.ops_date
                                                                 ,ext_timestamp
                                                             )
                                            - SUM(cases_selected) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.ops_date
                                                                 ,ext_timestamp
                                                             )
                                            - SUM(scr_qty) 
                                              OVER ( PARTITION BY adbw.warehouse_id 
                                                                 ,adbw.ops_date
                                                                 ,ext_timestamp
                                                             )
                                            , 0 ) cases_remaining
                                    FROM assignment_details_by_warehouse adbw
                                    """  
         logger.debug( " - Query - " + allCalculationsQuery );
         val allCalculations = sqlContext.sql(allCalculationsQuery)     
         logger.debug( " - Query fired " ) 
         allCalculations.show(30)
         
          val closedLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL ")
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number") )  //,"ext_date","ext_time",
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("ext_timestamp") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - Closed Loads Current hour - " )
          //closedLoadsCurrFT.show()
          //closedLoadsCurrFT.cache()
          
          val openLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL )  ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("load_number"), dfAssgnDetailsWarehouse("ext_timestamp") )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),dfAssgnDetailsWarehouse("customer_number"),dfAssgnDetailsWarehouse("customer_name"), dfAssgnDetailsWarehouse("ext_timestamp") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads" )
          }
          logger.debug(" - Open Loads Current hour - " )
          //openLoadsCurrFT.show()
          //openLoadsCurrFT.cache()
          
          val allClosedLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse  
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NOT NULL  ")
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name") , dfAssgnDetailsWarehouse("load_number"), dfAssgnDetailsWarehouse("ext_timestamp") )  //,"ext_date","ext_time",
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"), lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name"), dfAssgnDetailsWarehouse("ext_timestamp") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "closed_loads")
          }
          logger.debug(" - All Closed Loads Current hour - " )
          //allClosedLoadsCurrFT.show()
          //allClosedLoadsCurrFT.cache()
          
          val allOpenLoadsCurrFT = Future{ 
            dfAssgnDetailsWarehouse  
          //.join(opsDate, (dfAssgnDetailsWarehouse("warehouse_id") === opsDate("facility") && dfAssgnDetailsWarehouse("ops_date")  === opsDate("ops_date")), "inner")
          .filter(s" closed_timestamp IS NULL AND ( is_load_deferred = 'N' OR is_load_deferred IS NULL ) ")  //AND warehouse_id = '$warehouseTesting'
          .select(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name"), dfAssgnDetailsWarehouse("load_number") , dfAssgnDetailsWarehouse("ext_timestamp") )
          .groupBy(dfAssgnDetailsWarehouse("warehouse_id"), dfAssgnDetailsWarehouse("ops_date"),lit("ALL").alias("customer_number") ,lit("ALL").alias("customer_name"), dfAssgnDetailsWarehouse("ext_timestamp") )  //,"data_for_hour","ext_datetime_fromunix"
          .agg(countDistinct("load_number") as "open_loads" )
          }
          logger.debug(" - All Open Loads Current hour - " + "\n" )
          
          val totalOpenLoadsFT = openLoadsCurrFT.flatMap { x => allOpenLoadsCurrFT.map { y => y.unionAll(x) } } 
          val totalClosedLoadsFT = closedLoadsCurrFT.flatMap { x => allClosedLoadsCurrFT.map { y => x.unionAll(y) } }
          val totalOpenLoads =  Await.result(totalOpenLoadsFT, awaitDuration )  
          val totalClosedLoads =  Await.result(totalClosedLoadsFT, awaitDuration ) 
          logger.debug(" - Total Closed and Open Loads Current Hour - " )
          totalClosedLoads.show()
          totalOpenLoads.show()
          
          val customerHourly =  allCalculations
          .join(totalClosedLoads, Seq("warehouse_id", "ops_date", "customer_number", "ext_timestamp" ), "left" )  //, "ext_date", "data_for_hour" 
          .join(totalOpenLoads, Seq("warehouse_id", "ops_date", "customer_number", "ext_timestamp" ), "left" )  //,"customer_name"
          .select(allCalculations("warehouse_id"), allCalculations("ops_date"), allCalculations("customer_number"), allCalculations("customer_name")
                 ,allCalculations("ext_timestamp"), allCalculations("is_load_deferred")
                 ,allCalculations("sel_qty"), allCalculations("cases_remaining" ), allCalculations("scr_qty"), allCalculations("bill_qty")
                 ,totalClosedLoads("closed_loads"), totalOpenLoads("open_loads")
                 )
          .orderBy("warehouse_id", "ops_date", "customer_number","customer_name", "ext_timestamp")  //customer_name
          
          customerHourly.show(30)
        
        customerHourly.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save( filePath + "CustomerHourly")
         
  }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing validateCustomerHourly at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
  
  
}
         /*ldbw.warehouse_id = '44'
                         AND ldbw.ops_date = '2016-12-04 05:00:00+0000' 
                         AND ldbw.load_number IN ('000000276078','000000276079'
                              ,'000000276080','000000276082','000000276083','000000276084'
                              ,'000000276085','000000276086','000000276087','000000276088')
                              * customer_name = 'Multiple' OR set_back_time IS NOT NULL
                              AND ldbw.ops_date = '2016-12-04 05:00:00+0000'
                         AND load_number IN (,'000000276088'
                              ,'000000276087','000000276086','000000276085','000000276084','000000276083'
                              ,'000000276082','000000276080','000000276079','000000276078'
                              )
                              * */
         /*queryData.write
         .format("parquet")
         .save("C:/LoadDetails/hourlyUpdate/hourlyUpdate.parquet")*/
         /* DISTINCT wsd.whse_nbr, wsd.ent_date, ldbe.ops_date, edbw.shift_date, cast( wsd.emp_id as int) emp_id
                            ,wsd.disp_date, wsd.disp_time, wsd.shift, ldbe.mf_shift_id
                            ,edbw.warehouse_id, edbw.shift_id, edbw.employee_number,edbw.employee_name
                            ,edbw.active_loads, edbw.closed_loads, edbw.pallets_per_hour
                            ,edbw.actual_hours, edbw.projected_hours, edbw.employee_type, edbw.hired_days_nbr
                            ,edbw.latest_punch, edbw.overtime_hours, edbw.plan_percent, edbw.work_hours
                        WHERE wsd.whse_nbr = '76'
                          AND wsd.ent_date = '20161204'
                        * */

        // AND chain IN ( '00001','00028','00081' )
         //AND ldbe.ops_date = '2016-12-04 05:00:00+0000'
         /*
          * AND ent_date = '20161211' 
                         AND load_nbr IN ( '000000276232','000000276240' )
          */
         /* ent_date
          *                        WHERE NOT EXISTS ( SELECT 1 FROM employee_details_by_warehouse edbw
          */
          /*                                  WHERE wsd.whse_nbr = CAST( edbw.warehouse_id as int )
                                              AND cast( wsd.emp_id as int) = edbw.employee_number
                                              AND ldbe.ops_date = edbw.shift_date )
          * WHERE wsd.whse_nbr = '76'
                         AND ent_date = '20161211'
                         AND load_nbr = '000000276246'  
          */

/* WITH base_data AS 
                                     ( SELECT wsd.whse_nbr
                                           ,wsd.ent_date
                                           ,concat( '"',CAST( wsd.load_nbr as string) ,'"' ) load_nbr 
                                           ,COUNT(ent_date)
                                      FROM warehouse_shipping_data wsd
                                      )
                                    SELECT wsd.whse_nbr
                                          ,wsd.ent_date
                                          ,wsd.load_nbr
                                          ,bd1.ent_date_cnt
                                      FROM warehouse_shipping_data wsd
                                          ,(
                                    SELECT whse_nbr
                                          ,ent_date
                                          ,load_nbr 
                                          ,COUNT(ent_date) OVER (PARTITION BY whse_nbr
                                                                             ,ent_date
                                                                             ,load_nbr
                                                                ) ent_date_cnt 
                                      FROM base_data bd
                                            ) bd1
                                     WHERE bd1.whse_nbr = wsd.whse_nbr
                                       AND bd1.ent_date = wsd.ent_date
                                       AND bd1.load_nbr = wsd.load_nbr 
                                       AND bd1.ent_date_cnt > 1 
          * AND ldbe.ops_date = '2016-12-12 05:00:00+0000'
          * OVER ( PARTITION BY wsd.whse_nbr
                                                      ,ldbe.ops_date
                                                      ,wsd.chain 
                                                      ,wsd.chain_name
                                                      ,wsd.ext_time )
           ,CASE WHEN closed_date = '00000000' AND closed_time = '0000' THEN 
                                       0
                                  ELSE COUNT(DISTINCT load_nbr) 
                              END total_loads
                              JOIN employee_details_by_warehouse edbw
                            ON wsd.whse_nbr = edbw.warehouse_id
                               AND wsd.emp_id.toInt = edbw.employee_number
           LEFT OUTER JOIN aba_capacity_by_opsday acbo
                          ON wsd.whse_nbr = acbo.warehouse_id
                             AND ldbe.ops_date = acbo.ops_date
           */