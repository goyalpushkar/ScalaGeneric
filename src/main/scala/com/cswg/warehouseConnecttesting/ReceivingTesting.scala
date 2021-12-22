package com.cswg.testing

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext,DataFrame,Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.slf4j.LoggerFactory
import org.slf4j.MDC

import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import java.sql.Timestamp
import java.util.stream.Collectors._
import scala.collection.mutable.{ WrappedArray, ListBuffer }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.MapType

import com.typesafe.config.ConfigFactory
import scala.collection.immutable.List

class ReceivingTesting {
  
}

object ReceivingTesting {
  var logger = LoggerFactory.getLogger("Examples");
  //val logger = LoggerFactory.getLogger(this.getClass())
  MDC.put("fileName","M5999_WCRCVTesting")
  var filePath = "C:/opt/cassandra/default/dataFiles/ReceivingDetails/"
  var loggingEnabled = "N"
  var productivitySteamingTime = getCurrentTime()
  var saveDataFrame = "N"
  val teamType = "Receiving";
  val teamIdTesting = "7202";  
  val poNumber = null;
 
 def main(args: Array[String]) {    
     logger.debug("Main for Receiving Testing")
     
     val environment = if ( args(2) == null ) "dev" else args(2).toString 
     saveDataFrame = if ( args(3) == null ) "N" else args(3).toString()
     logger.info( " - Cassandra environment - " + environment )
     val (hostName, userName, password) = getCassandraConnectionProperties(environment)
         
     val conf = new SparkConf()
      .setAppName("M5999_WCRCVTesting")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      //.set("spark.cassandra.output.ignorenulls","true")
      //.set("spark.eventLog.enabled", "true" )
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]")  // This is required for IDE run
     
     val sc = new SparkContext(conf)
     //val sqlContext: SQLContext = new SQLContext(sc)
     val hiveContext: SQLContext = new HiveContext(sc)
     val currentTimeToday = getCurrentTime()
     val currentTime = getCurrentTime()   //add_minutes(currentTimeToday, -960)   //getCurrentTime()  // getPreviousOpsDate(currentTimeToday, -4)   //getCurrentTime()  //removeHours(currentTimeToday, -16)
     val previousTime = getPreviousOpsDate(currentTime, -1)
     val previousSpclTime = getPreviousOpsDate(currentTime, -2)      
     //val host = sc.getConf.get("spark.cassandra.connection.host")
     
     val jobId = if ( args(0) == null ) 1 else args(0).toString()  //"3" "1"
     loggingEnabled = if ( args(1) == null ) "N" else args(1).toString()
     
     import hiveContext.implicits._
     
     logger.debug( " - Conf Declared - ")      
     
     try{
         logger.debug( "\n" + " - Current Time - " + currentTime  + "\n"
                     + " - previousTime - " + previousTime
                     + " - Job Id - " + jobId  + "\n"
                     + " - filePath - " + filePath  + "\n"
                     + " - loggingEnabled - " + loggingEnabled + "\n"
                    // + " - host - " + host + "\n"
                     + " - saveDataFrame - " + saveDataFrame
                    );
         
         
         val dfJobControl =  hiveContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "rcv_job_control"
                                      , "keyspace" -> "wip_rcv_ing") )
                         .load()
                         .filter(s"job_id = '$jobId' AND enabled = 'Y' ")   // 
                         
         val warehouseId = "'" + dfJobControl.select(dfJobControl("warehouse_id"))
                            .collect()
                            .map{ x => x.getString(0) }
                            .mkString("','") + "'"
                           
                          // " '29' "
         val warehouseList = dfJobControl.select(dfJobControl("warehouse_id")).collect() 
                            
         logger.info( " - Warehouse Ids - " + warehouseId )
         
         var configmap:Map[String,String] = Map()               
         configmap = getProfileValuePv(hiveContext, "WC_RCV_THRESHOLD_TIME", "3", warehouseList , null);      
         val configDF = sc.parallelize(configmap.toSeq).toDF("wh_id","threshold_time")
         configDF.registerTempTable("rcv_threshold_time")
         configDF.show()
         //configDF.printSchema()
         
         var configmapStart:Map[String,String] = Map()               
         configmapStart = getProfileValuePv(hiveContext, "WC_RCV_OPS_DAY_DELAY_TIME", "30", warehouseList , null);      
         val configStartDF = sc.parallelize(configmapStart.toSeq).toDF("wh_id","threshold_time")
         configStartDF.registerTempTable("rcv_threshold_start_time")
         configStartDF.show()
         hiveContext.cacheTable("rcv_threshold_start_time")         
         
         val dfWarehouseOperationTimeAll = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_operation_time"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load()
                          .filter(s"facility IN ( $warehouseId ) ").persist()  //$currentTime  
                          //AND ( '$currentTime' BETWEEN start_time AND end_time )  Removed on 11/08/2017 
                          // 
         
         
         // Added on 11/08/2017
         val dfWarehouseOperationTimeToday = dfWarehouseOperationTimeAll.filter(s" facility IN ( $warehouseId ) AND '$currentTime' BETWEEN start_time AND end_time ").persist()
         dfWarehouseOperationTimeToday.registerTempTable("warehouse_operation_time_today")
         logger.debug("dfWarehouseOperationTimeAll toDay")
         dfWarehouseOperationTimeToday.show()
         
         val dfWarehouseOperationTimePrev = dfWarehouseOperationTimeAll.filter(s" facility IN ( $warehouseId ) AND '$previousTime' BETWEEN start_time AND end_time ").persist()
         logger.debug("dfWarehouseOperationTimePrev prevDay")
         dfWarehouseOperationTimePrev.show()
         
         // Added on 11/20/2017
         /*val dfWarehouseOperationTimePrevSpcl = dfWarehouseOperationTimeAll.filter(s" '$previousSpclTime' BETWEEN start_time AND end_time ").persist()
         logger.debug("dfWarehouseOperationTimePrevSpcl prevPrevDay")
         dfWarehouseOperationTimePrevSpcl.show()    */     
         // Ended on 11/20/2017
         
         hiveContext.udf.register("add_minutes", add_minutes _)
         hiveContext.udf.register("add_hrs", add_hrs _)
          val dfWarehouseOperationTime = //dfWarehouseOperationTimePrev
                                         dfWarehouseOperationTimeAll.alias("wot")
                                        //dashbaord
                                       .join(configStartDF.alias("conf"), ( dfWarehouseOperationTimeAll("facility") === configStartDF("wh_id") ), "left")
                                       //Performance
                                       //.join(configDF.alias("conf"), ( dfWarehouseOperationTimeAll("facility") === configDF("wh_id") ), "left")
                                       .select($"wot.*", $"conf.threshold_time")
                                       .withColumn("add_minutes",  expr("add_minutes( end_time, CAST(threshold_time as int ) )"))
                                       .withColumn("add_hrs",  expr("add_hrs( end_time, CAST(conf.threshold_time as int ) + 1 ) "))
                                       //dashbaord
                                       //.filter(s" ( '$currentTime' BETWEEN start_time AND end_time ) OR ( '$currentTime' >= add_minutes( end_time, CAST(conf.threshold_time as int ) ) AND NVL( wot.rcv_ops_day_delayed_process, 'N' ) = 'N' ) ")
                                       //Performance
                                       //.filter(s" ( '$currentTime' BETWEEN start_time AND end_time OR '$currentTime' <= add_hrs( end_time, CAST(conf.threshold_time as int ) + 1 ) OR NVL( wot.rcv_ops_day_delayed_process_perf, 'N' ) = 'N' ) ")
                                       //OR NVL( wot.rcv_ops_day_delayed_process, 'N' ) = 'N' removed for testing
                                       .filter(s" ( '$currentTime' BETWEEN start_time AND end_time OR '$previousTime' BETWEEN start_time AND end_time ) ")
                                        
         logger.debug("dfWarehouseOperationTime processing")
         dfWarehouseOperationTime.show()                                            
         // Ended on 11/08/2017
         
         /*val dfWarehouseOperationTime = dfWarehouseOperationTimeAll
                                             .filter(s"facility IN ( $warehouseId ) AND ( '$currentTime' BETWEEN start_time AND end_time ) ")   
         dfWarehouseOperationTime.show()
         
         val dfWarehouseOperationTimePrev = dfWarehouseOperationTimeAll
                                              .filter(s"facility IN ( $warehouseId ) AND ( '$previousTime' BETWEEN start_time AND end_time ) ")
         dfWarehouseOperationTimePrev.show()
         */
        val dfRDSLoads = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "rds_loads_ing_details"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          
        val dfRDSUnsched = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "rds_unsched_bh_ing_details"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          
        val dfWhsMappingDetails = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "whs_mapping_details"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()       
                          
        val dfPriorityList = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "priority_list_ing_details"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId )")
         
        /* val dfPriorityListComments = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "priority_list_comments"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          */
         val dfReceivingSecondPass = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_recv_secondpass_data"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          .filter(s" destination IN ( $warehouseId ) ")                          
    
         val dfLumperLinkLoad = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "lumperlink_data_by_warehouse"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          //.filter(s" facility IN ( $warehouseId ) ")
                                            
         val dfRecvPerformance = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "team_performance_by_warehouse"
                                        ,"keyspace" -> "wip_rcv_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ") 
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          
         val dfRecvDashboard = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "rcv_dashboard_by_whs_ops_date"
                                        ,"keyspace" -> "wip_rcv_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ") 
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 

         val dfRecvUnplannedPOs = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "rcv_unplanned_po_by_warehouse"
                                        ,"keyspace" -> "wip_rcv_prs" ) )
                          .load()
                          .filter(s" CAST( warehouse_id as int) IN ( $warehouseId ) ")   
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          
         val dfEmployeeDetails = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_details_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s" CAST( warehouse_id as Int) IN ( $warehouseId ) ") 
                          
        // Added on 12/07/2017
        val dfRcvDahsboardThreeHours = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "rcv_dashboard_three_hr_po"
                                        ,"keyspace" -> "wip_rcv_ing" ) )
                          .load()
                          .filter(s" CAST( warehouse_id as int) IN ( $warehouseId ) ")                             
        // Ended on 12/07/2017                          
         logger.debug( " - DataFrame Declared")
         
         dfWarehouseOperationTime.registerTempTable("warehouse_operation_time")
         dfPriorityList.registerTempTable("priority_list_ing_details")
         //dfPriorityListComments.registerTempTable("priority_list_comments")
         dfReceivingSecondPass.registerTempTable("warehouse_recv_secondpass_data")
         dfLumperLinkLoad.registerTempTable("lumperlink_data_by_warehouse")
         dfRDSLoads.registerTempTable("rds_loads_ing_details")
         dfRDSUnsched.registerTempTable("rds_unsched_bh_ing_details")
         dfWhsMappingDetails.registerTempTable("whs_mapping_details")
         dfRecvPerformance.registerTempTable("team_performance_by_warehouse")
         dfRecvDashboard.registerTempTable("rcv_dashboard_by_whs_ops_date")
         dfRecvDashboard.registerTempTable("rcv_dashboard_by_whs_ops_date_all")
         dfRecvUnplannedPOs.registerTempTable("rcv_unplanned_po_by_warehouse_all")
         dfEmployeeDetails.registerTempTable("employee_details_by_warehouse")
         logger.debug( " - Temp Tables Registered")
         
         val dfSparkStreaming = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "spark_streaming_table_config"
                                        ,"keyspace" -> "wip_configurations" ) )
                          .load()
                          //filter(s" UPPER(source_system_name) = UPPER('mframe_prod_dbsvrt4p') AND UPPER(source_table_name) = 'WICS_PRODUCTIVITY' AND active = 'Y' ") 
         
         dfPriorityList.registerTempTable("priority_list_ing_details_base")
         
         val dfPriorityListTodayQuery = s""" SELECT wot.facility warehouse_id
                                                   ,wot.ops_date 
                                                   ,TRIM( plid.po_number ) po_number
                                                   ,plid.shift_date
                                                   ,plid.active_flag 
                                                   ,plid.appt_date 
                                                   ,plid.cases
                                                   ,plid.current_status
                                                   ,plid.pallets
                                                   ,plid.priority_num
                                                   ,plid.start_time
                                                   ,plid.vendor_name
                                                   ,plid.po_number untrim_po_number
                                                   ,plid.is_synced
                                               FROM priority_list_ing_details_base plid
                                                   ,warehouse_operation_time wot
                                              WHERE CAST( plid.warehouse_id as Int) = wot.facility 
                                                AND plid.shift_date = wot.ops_date
                                        """
         logger.debug( " - dfPriorityListTodayQuery - " + dfPriorityListTodayQuery )
         val dfPriorityListToday = hiveContext.sql(dfPriorityListTodayQuery).persist()
         
         /*if ( loggingEnabled == "Y" ) {
           logger.debug( " - dfPriorityListToday Count - " + dfPriorityListToday.count() )  
           dfPriorityListToday.show(  dfPriorityListToday.count().toInt )
         }*/
         dfPriorityListToday.registerTempTable("priority_list_ing_details")
         
          val dfRcvDashboardToday = dfRecvDashboard.alias("rcv")
         .join(dfWarehouseOperationTime, ( dfRecvDashboard("warehouse_id") === dfWarehouseOperationTime("facility") && dfRecvDashboard("ops_date") === dfWarehouseOperationTime("ops_date") ), "inner" )
         .select("rcv.*")
         /*if ( loggingEnabled == "Y" ) {
           logger.debug( " - dfRcvDashboardToday Count - " + dfRcvDashboardToday.count() )  
           dfRcvDashboardToday.show( dfRcvDashboardToday.count().toInt )
         } */          
         dfRcvDashboardToday.registerTempTable("rcv_dashboard_by_whs_ops_date_today")
         
         val dfRecvPerformanceToday = dfRecvPerformance.alias("rcv")
         .join(dfWarehouseOperationTime, ( dfRecvPerformance("warehouse_id") === dfWarehouseOperationTime("facility") && dfRecvPerformance("ops_date") === dfWarehouseOperationTime("ops_date") ), "inner" )
         .select("rcv.*")
         /*if ( loggingEnabled == "Y" ) {
           logger.debug( " - dfRecvPerformanceToday Count - " + dfRecvPerformanceToday.count() )  
           dfRecvPerformanceToday.show( dfRcvDashboardToday.count().toInt )
         } */          
         dfRecvPerformanceToday.registerTempTable("team_performance_by_warehouse_today")
         
         val dfRcvDashboardPrev = dfRecvDashboard.alias("rcv")
         .join(dfWarehouseOperationTimePrev, ( dfRecvDashboard("warehouse_id") === dfWarehouseOperationTimePrev("facility") && dfRecvDashboard("ops_date") === dfWarehouseOperationTimePrev("ops_date") ), "inner" )
         .select("rcv.*")
         /*if ( loggingEnabled == "Y" ) {
           logger.debug( " - dfRcvDashboardPrev Count - " + dfRcvDashboardPrev.count() )  
           dfRcvDashboardPrev.show( dfRcvDashboardPrev.count().toInt )
         }*/
         dfRcvDashboardPrev.registerTempTable("rcv_dashboard_by_whs_ops_date_prev")         
                 
         // Added on 11/20/2017
         /*val dfRcvDashboardPrevSpcl = dfRecvDashboard.alias("rcv")
         .join(dfWarehouseOperationTimePrevSpcl, ( dfRecvDashboard("warehouse_id") === dfWarehouseOperationTimePrevSpcl("facility") && dfRecvDashboard("ops_date") === dfWarehouseOperationTimePrevSpcl("ops_date") ), "inner" )
         .select("rcv.*")
         /*if ( loggingEnabled == "Y" ) {
           logger.debug( " - dfRcvDashboardPrevSpcl Count - " + dfRcvDashboardPrevSpcl.count() )  
           dfRcvDashboardPrevSpcl.show( dfRcvDashboardPrevSpcl.count().toInt )
         } */          
         dfRcvDashboardPrevSpcl.registerTempTable("rcv_dashboard_by_whs_ops_date_prev_spcl")
         val dfRcvDashboardPrevTotal = dfRcvDashboardPrev.unionAll(dfRcvDashboardPrevSpcl) */
         // Ended on 11/20/2017 
         
        // Added on 12/07/2017
        val dfRcvDahsboardThreeHoursProcessing = dfRcvDahsboardThreeHours.alias("rcv")
         .join(dfWarehouseOperationTime, ( dfRcvDahsboardThreeHours("warehouse_id") === dfWarehouseOperationTime("facility") && dfRcvDahsboardThreeHours("ops_date_processed") === dfWarehouseOperationTime("ops_date") ), "inner" )
         .select("rcv.*")     
        dfRcvDahsboardThreeHoursProcessing.registerTempTable("rcv_dashboard_three_hr_po") 
        hiveContext.cacheTable("rcv_dashboard_three_hr_po")
        //Ended on 12/07/2017          
         
         val dfRcvUnplannedPOToday = dfRecvUnplannedPOs.alias("rcv")
         .join(dfWarehouseOperationTime, ( dfRecvUnplannedPOs("warehouse_id") === dfWarehouseOperationTime("facility") && dfRecvUnplannedPOs("ops_date") === dfWarehouseOperationTime("ops_date") ), "inner" )
         .select("rcv.*")         
         /*if ( loggingEnabled == "Y" ) {
           logger.debug( " - dfRcvUnplannedPOToday Count - " + dfRcvUnplannedPOToday.count() )  
           dfRcvUnplannedPOToday.show()
         }         */
         dfRcvUnplannedPOToday.registerTempTable("rcv_unplanned_po_by_warehouse")         
         dfPriorityListToday.unpersist()         
        /* 
         val warehouseOperationDateQuery = s""" SELECT * 
                                                  FROM warehouse_operation_time
                                              """
         
         logger.debug( " - warehouseOperationDateQuery - " + warehouseOperationDateQuery )
         val warehouseOperationDate = hiveContext.sql(warehouseOperationDateQuery)
         warehouseOperationDate.show() */  

         //closeCarryForwarded(hiveContext, dfRcvDashboardToday, dfReceivingSecondPass, dfLumperLinkLoad )
         
         /*val rdsCommonsLoad = baseData(hiveContext)
         //val rdsCommonsLoad = null
         backhaulsPOLoad(hiveContext, rdsCommonsLoad)
         listPrevOpsDayPOs(sc, hiveContext, dfRcvDashboardPrev, dfWarehouseOperationTimeToday
                          //,dfRcvDashboardPrevSpcl, dfWarehouseOperationTimePrev
                          )  // Added on 11/20/2017  
         if ( saveDataFrame == "Y" ) {
               dfRcvDahsboardThreeHoursProcessing
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RcvDashboardThreeHours/" ) 
               }
         removePOsProcessedPrevDay(sc, hiveContext)  // Added on 11/20/2017   
                   */     
         basePerformanceData(hiveContext)
         removePOsPerfProcessedPrevDay( hiveContext )  //sc, dfRcvDashboardPrevTotal
  
         checkDashboard(sc, hiveContext)
         checkRcvPerformance(hiveContext)
         

         //checkLumperLinkData(hiveContext)
         //checkWicsData(hiveContext)         
         //checkUnplannedPO(hiveContext)
         
         //testNoValueinDF(hiveContext, sc)
         //validateLoadId(hiveContext)
         //getBaseDataIng(hiveContext, sc)
         //checkDashboard(sc, hiveContext)
         
         //processPrevOpsDaySecondPass(hiveContext, dfRcvDashboardPrev)
         //checkGroupedData(hiveContext, dfRcvDashboardToday)
         //intervalHourTesting(hiveContext)
         //filterConditionCheck(hiveContext,dfRcvDashboardPrev)
        
          //updateData(hiveContext, sc, dfWarehouseOperationTimeAll)
         
         //updateTimeLumperLink(hiveContext, dfLumperLink)
         //updateRcvUnplanned(hiveContext)
         //val duration = getDuration(hiveContext)
         //logger.debug( "- duration - " + duration )         
         logger.debug( " - Program Completed")
     }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
     
   }
    
    def updateData(sqlContext: SQLContext, sc: SparkContext, dfUpdateDF: DataFrame) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      updateData                    " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val warehouseId =  "'76','23','81','77'"   //"'20','8','81','23','92','97','1'";            
    val currentTime = getCurrentTime();
    val previousTime = getPreviousOpsDate(currentTime, -2)  //getCurrentTime();
    try{ 
         logger.debug( " - currentTime - "  + currentTime + " - previousTime - " + previousTime)
         val fileStreaming = dfUpdateDF
                               //.filter("file_id = 1 AND ext_timestamp > '2017-08-06 10:58:01' AND validation_status = 'Ingested-Hold'")  //
                               .filter(s" facility IN ( $warehouseId ) AND ops_date < '$previousTime'")
                               .select("facility", "start_time", "end_time")
                               .withColumn("rcv_ops_day_delayed_process", lit("Y"))
                               .withColumn("rcv_ops_day_delayed_process_perf", lit("Y"))
         
         fileStreaming
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "wip_shipping_ing")
        .option("table", "warehouse_operation_time")
        .mode("append").save()
        
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing updateData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
    
  def testNoValueinDF(sqlContext: SQLContext, sc: SparkContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    testNoValueinDF                 " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val warehouseId =  "'76','23','81','77'"   //"'20','8','81','23','92','97','1'";            
    val currentTime = getCurrentTime();
    val previousTime = getPreviousOpsDate(currentTime, -2)  //getCurrentTime();
    try{ 
         logger.debug( " - currentTime - "  + currentTime + " - previousTime - " + previousTime)
         val updateDataQuery = s""" SELECT rdpp.warehouse_id, rdpp.ops_date, rdpp.load_id
                                          ,lpd.team lumperlink_team
                                          ,lpd.ticketnumber lumperlink_ticket_number
                                          ,lpd.totalloadstarttime from_time
                                          ,lpd.door door_no
                                          ,lpd.endingpallets lumperlink_ending_pallets
                                          ,lpd.damage lumperlink_damage_qty
                                          ,lpd.status lumperlink_status
                                          ,wpd.team_id wics_team
                                          ,wpd.entry_date_timestamp wics_entry_time
                                      FROM rcv_dashboard_prev_po rdpp
                                      LEFT OUTER JOIN lumperlink_data_by_warehouse lpd
                                        ON rdpp.warehouse_id = lpd.facility
                                          AND rdpp.po_number = lpd.po
                                      LEFT OUTER JOIN warehouse_recv_secondpass_data wpd
                                        ON rdpp.warehouse_id = wpd.destination
                                          AND rdpp.po_number = wpd.po_number
                                     WHERE warehouse_id = '29'
                                       AND load_id = 'f8cd7bc6-0b9b-3cd8-861e-5bbd2714e84d'

                                 """
         logger.debug( " - updateDataQuery - " + updateDataQuery )
         val updateData = sqlContext.sql(updateDataQuery)
         updateData.show()
         
         updateData
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "wip_rcv_prs")
        .option("table", "rcv_dashboard_by_whs_ops_date")
        //.option("spark.cassandra.output.ignoreNulls", "true")
        .mode("append").save()
        
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing testNoValueinDF at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
    

    def validateLoadId(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    validateLoadId                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val warehouseId = "'29','44','76'";            
    val currentTime = getCurrentTime();
    sqlContext.udf.register( "getLoadId", getLoadId _)
    val previousTime = getPreviousOpsDate(currentTime, -3)  //getCurrentTime();
    try{ 
         logger.debug( " - currentTime - "  + currentTime + " - previousTime - " + previousTime)
         val loadIdQuery = """ SELECT getLoadId( '464767' )
                                 FROM rcv_dashboard_by_whs_ops_date rdb
                                WHERE load_id = 'bef04ff1-78fe-3ce7-83f6-144c30b7116b'
                           """
         
         val loadId = sqlContext.sql(loadIdQuery)
         loadId.show()
        
    }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing validateLoadId at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }    
    
     def processPrevOpsDaySecondPass(sqlContext: SQLContext, dfRcvDashboardPrev: DataFrame ) = {
       logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "             processPrevOpsDaySecondPass            " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
       val currentTimeToday = getCurrentTime()      
       val currentTime = removeHours(currentTimeToday, -15)    // getCurrentTime()   
       logger.debug( "currentTime - " + currentTime)      
       //val poAlreadyProcessedUDF = udf(poAlreadyProcessed _)
       //sqlContext.udf.register("poExistsSecondPass", poExistsSecondPass _)
       val poExistsSecondPassUDF = udf(poExistsSecondPass _)
       try{
              /*
               * unix_timestamp('$currentTime') currentTime
                                                 ,unix_timestamp(start_time)  start_time
                                                 ,unix_timestamp(max_entry_date_timestamp)  max_entry_date_timestamp 
                                                 ,ABS( unix_timestamp('$currentTime') - unix_timestamp(start_time) ) currentTimeDiff
                                                 ,
               */
             val secondPassDataQuery = s"""SELECT sbd.* 
                                           FROM secondpass_base_data sbd
                                          WHERE ( CASE WHEN ( unix_timestamp(max_entry_date_timestamp) - unix_timestamp(start_time) ) < 0 THEN 
                                                            ( threshold_time * 3600 ) + 1  
                                                     ELSE ( unix_timestamp(max_entry_date_timestamp) - unix_timestamp(start_time) )
                                                 END ) <= threshold_time * 3600
                                            AND ( CASE WHEN ( unix_timestamp('$currentTime') - unix_timestamp(start_time) ) < 0 THEN
                                                            ( threshold_time * 3600 ) + 1  
                                                     ELSE ( unix_timestamp('$currentTime') - unix_timestamp(start_time) )
                                                 END )  <= threshold_time * 3600
                                       """
             //AND ABS( unix_timestamp('$currentTime') - unix_timestamp(start_time) ) <= threshold_time * 3600
             logger.debug(  " - secondPassDataQuery - " + secondPassDataQuery )
             val secondPassData = sqlContext.sql(secondPassDataQuery)
                      
             if ( loggingEnabled == "Y" ) {
                  logger.debug(  " - secondPassData - " + secondPassData.count() )
                  secondPassData.show( secondPassData.count().toInt )
               }
             
             secondPassData
               .coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/SecondPassedPOList/" )
               
             val poNumberListSecondPass = secondPassData.select( secondPassData("warehouse_id"), secondPassData("ops_date"), secondPassData("po_number") )
                                       .groupBy("warehouse_id", "ops_date").agg( expr("collect_set(po_number)").alias("po_number_secondpass") )    
             
            val rcvDashboardSecondPass = dfRcvDashboardPrev.alias("rcv").filter(s" status <> '$ticket_closed' ")
                            .join(poNumberListSecondPass, (poNumberListSecondPass("warehouse_id") === dfRcvDashboardPrev("warehouse_id") ), "inner" )
                            //&& poNumberListSecondPass("ops_date") === dfRcvDashboardPrev("ops_date")
                            .select( dfRcvDashboardPrev("warehouse_id"), dfRcvDashboardPrev("ops_date"), dfRcvDashboardPrev("load_id")
                                    ,poNumberListSecondPass("po_number_secondpass"), dfRcvDashboardPrev("po_number_list")
                                   )
                            .withColumn("po_number_list_new", poExistsSecondPassUDF( dfRcvDashboardPrev("po_number_list"), poNumberListSecondPass("po_number_secondpass") ) )
                            .select( "warehouse_id", "ops_date", "load_id", "po_number_list_new")
                            .map { x => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getAs[Map[String, String]](3) 
                                            ,getLoadStatus( if ( x.get(3) == null ) null 
                                                else x.getAs[Map[String, String]](3) )
                                           ,previousOpsDay
                                           ,currentTime
                                           ) }
             
             val rcvDashboardSecondPassStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                      StructField("ops_date", TimestampType, nullable=false),
                                                      StructField("load_id", StringType, nullable=false),
                                                      StructField("po_number_list", MapType(StringType, StringType, true), nullable=true),
                                                      StructField("status", StringType, nullable=true),
                                                      StructField("source_dataframe", StringType, nullable=true),
                                                      StructField("upsert_timestamp", TimestampType, nullable=true)
                                                     )
                                             )
       
               val rcvDashboardSecondPassFinal = sqlContext.createDataFrame(rcvDashboardSecondPass, rcvDashboardSecondPassStruct).persist()

              if ( loggingEnabled == "Y" ) {
                  logger.debug(  " - rcvDashboardSecondPassFinal - " )
                  rcvDashboardSecondPassFinal.printSchema()
                  rcvDashboardSecondPassFinal.show( )
               }
            
              rcvDashboardSecondPassFinal
               .coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RCVDashboardPrevSecondPassed/" ) 
                   
              /*rcvDashboardSecondPassFinal
                   .write
                   .format("org.apache.spark.sql.cassandra")
                   .options( Map("keyspace" -> "wip_rcv_prs"
                               ,"table" -> "rcv_dashboard_by_whs_ops_date"))
                   .mode("append")
                   .save() */
                   
               logger.debug( " - rcvDashboardSecondPass saved for Prev Ops Date" )
         
       }catch{
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
            logger.error("Error while executing processPrevOpsDaySecondPass at : " +  e.getMessage + " " + e.printStackTrace() )
       }
   } 
     
   def closeCarryForwarded(sqlContext: SQLContext, dfRcvDashboardToday: DataFrame, dfReceivingSecondPass: DataFrame, dfLumperLinkLoad: DataFrame) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  closeCarryForwarded               " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
     val currentTime = getCurrentTime()       
     val poExistsSecondPassUDF = udf(poExistsSecondPass _)
     val poExistsFirstPassUDF = udf(poExistsFirstPass _)
     try{
           /*val secondPassDataQuery = """SELECT * 
                                           FROM secondpass_base_data 
                                       """
           logger.debug(  " - secondPassDataQuery - " + secondPassDataQuery )
           val secondPassData = sqlContext.sql(secondPassDataQuery)
             */
           val rcvDashboardFirstPassStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                      StructField("ops_date", TimestampType, nullable=false),
                                                      StructField("load_id", StringType, nullable=false),
                                                      StructField("po_number_list", MapType(StringType, StringType, true), nullable=true),
                                                      StructField("status", StringType, nullable=true),
                                                      StructField("appt_time", TimestampType, nullable=true),
                                                      StructField("upsert_timestamp", TimestampType, nullable=true)
                                                     )
                                             )
                        
            val rcvDashboardSecondPassStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                      StructField("ops_date", TimestampType, nullable=false),
                                                      StructField("load_id", StringType, nullable=false),
                                                      StructField("po_number_list", MapType(StringType, StringType, true), nullable=true),
                                                      StructField("status", StringType, nullable=true),
                                                      StructField("appt_time", TimestampType, nullable=true),
                                                      StructField("upsert_timestamp", TimestampType, nullable=true)
                                                     )
                                             )

           val poNumberListFirstPass = dfLumperLinkLoad.select( dfLumperLinkLoad("facility"), dfLumperLinkLoad("po") )
                                       .groupBy("facility").agg( expr("collect_set(po)").alias("po_number_firstpass") )    
                                       
           val poNumberListSecondPass = dfReceivingSecondPass.select( dfReceivingSecondPass("destination"), dfReceivingSecondPass("po_number") )
                                       .groupBy("destination").agg( expr("collect_set(po_number)").alias("po_number_secondpass") )    
             
           poNumberListFirstPass.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "FirstPassed" +  "/" )
               
           poNumberListSecondPass.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "SecondPassed" +  "/" )
           
           val rcvDashboardFirstPass = dfRcvDashboardToday.alias("rcv").filter(s" carry_forwarded = 'Y' AND status <> '$ticket_closed' ")   // AND load_id = 'f0aa886c-aca4-32bc-a8eb-94aa2eb0317d' AND load_id = 'ffcae87c-c85d-3908-b569-3e257f8ecd67'AND load_id = 'fe8860c1-82b3-3e08-a0ee-48ab12aaefa7'
                            .join(poNumberListFirstPass, (poNumberListFirstPass("facility") === dfRcvDashboardToday("warehouse_id") ), "left" )
                            //&& poNumberListSecondPass("ops_date") === dfRcvDashboardToday("ops_date")
                            .select( dfRcvDashboardToday("warehouse_id"), dfRcvDashboardToday("ops_date"), dfRcvDashboardToday("load_id")
                                    ,poNumberListFirstPass("po_number_firstpass"), dfRcvDashboardToday("po_number_list"), dfRcvDashboardToday("appt_time")
                                   )
                            .withColumn("po_number_list_new", poExistsFirstPassUDF( dfRcvDashboardToday("po_number_list"), poNumberListFirstPass("po_number_firstpass") ) )
                            .select( "warehouse_id", "ops_date", "load_id", "po_number_list_new", "appt_time")
                            .map { x => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getAs[Map[String, String]](3) 
                                            ,getLoadStatus( if ( x.get(3) == null ) null 
                                                else x.getAs[Map[String, String]](3) )
                                           ,if ( x.get(4) == null) null else x.getTimestamp(4), currentTime
                                           ) }
           
            val rcvDashboardFirstPassFinal = sqlContext.createDataFrame(rcvDashboardFirstPass, rcvDashboardSecondPassStruct).persist()
            
            if ( loggingEnabled == "Y" ) {
              logger.debug(  " - rcvDashboardFirstPassFinal - " )
              rcvDashboardFirstPassFinal.printSchema()
              rcvDashboardFirstPassFinal.show( rcvDashboardFirstPassFinal.count().toInt )
              rcvDashboardFirstPassFinal.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "RCVDashboardFirstPassed" +  "/" )
            }
            
            val rcvDashboardSecondPass = rcvDashboardFirstPassFinal.alias("rcv")
                            //.filter(s" carry_forwarded = 'Y' AND status <> '$ticket_closed' ")
                            .join(poNumberListSecondPass, (poNumberListSecondPass("destination") === rcvDashboardFirstPassFinal("warehouse_id") ), "left" )
                            //&& poNumberListSecondPass("ops_date") === dfRcvDashboardToday("ops_date")
                            .select( rcvDashboardFirstPassFinal("warehouse_id"), rcvDashboardFirstPassFinal("ops_date"), rcvDashboardFirstPassFinal("load_id")
                                    ,poNumberListSecondPass("po_number_secondpass"), rcvDashboardFirstPassFinal("po_number_list"), rcvDashboardFirstPassFinal("appt_time")
                                   )
                            .withColumn("po_number_list_new", poExistsSecondPassUDF( rcvDashboardFirstPassFinal("po_number_list"), poNumberListSecondPass("po_number_secondpass") ) )
                            .select( "warehouse_id", "ops_date", "load_id", "po_number_list_new", "appt_time")
                            .map { x => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getAs[Map[String, String]](3) 
                                            ,getLoadStatus( if ( x.get(3) == null ) null 
                                                else x.getAs[Map[String, String]](3) )
                                           ,if ( x.get(4) == null) null else x.getTimestamp(4), currentTime
                                           ) }

       
            val rcvDashboardSecondPassFinal = sqlContext.createDataFrame(rcvDashboardSecondPass, rcvDashboardSecondPassStruct).persist()

           if ( loggingEnabled == "Y" ) {
              logger.debug(  " - rcvDashboardSecondPassFinal - " )
              rcvDashboardSecondPassFinal.printSchema()
              rcvDashboardSecondPassFinal.show( rcvDashboardSecondPassFinal.count().toInt )
              rcvDashboardSecondPassFinal.coalesce(1)
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "RCVDashboardSeconPassed" +  "/" )
           }
        
          rcvDashboardSecondPassFinal
           .write
           .format("org.apache.spark.sql.cassandra")
           .options( Map("keyspace" -> "wip_rcv_prs"
                       ,"table" -> "rcv_dashboard_by_whs_ops_date"))
           .mode("append")
           .save()  
           
           logger.debug( " - rcvDashboardSecondPass saved for Ops Date" )
         
       }catch{
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
            logger.error("Error while executing processPrevOpsDaySecondPass at : " +  e.getMessage + " " + e.printStackTrace() )
       }
      
    }
     
   def markClosed(sqlContext: SQLContext, dfRcvDashboardToday: DataFrame) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                       markClosed                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
     try{
            
            dfRcvDashboardToday.filter( "appt_time IN ('2017-10-21 11:30:00', '2017-10-21 12:00:00', '2017-10-22 11:30:00','2017-10-24 12:30:00' ")
       
     }catch{
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
            logger.error("Error while executing markClosed at : " +  e.getMessage + " " + e.printStackTrace() )
       }
   }
   def checkGroupedData(sqlContext: SQLContext, dfRcvDashboardToday: DataFrame) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                       checkGroupedData             " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
     try{
            val dataF:(Map[String, String]) => String =  _.map(_._1).mkString(",")
            val dataUDF= udf[String, Map[String, String]](dataF) 
            
            /*val poNumberListToDay =  dfRcvDashboardToday.select(dfRcvDashboardToday("warehouse_id"), dfRcvDashboardToday("po_number_list"))
                            .collect()
                            .map{ x => Row( x.getString(0), x.getAs[Map[String, String]](1) ) }
                            .mkString
                            //.mkString("','") */
                                                                
            val poNumberListPrevDay = dfRcvDashboardToday.select( dfRcvDashboardToday("warehouse_id"), dfRcvDashboardToday("po_number_list") )
                                     .withColumn("po_numbers", dataUDF( dfRcvDashboardToday("po_number_list") ) )
                                     .groupBy("warehouse_id").agg( expr("collect_set(po_numbers)") )
                                     //.groupBy("warehouse_id").agg( expr("LISTAGG(po_numbers, ',') ") )

            poNumberListPrevDay.show()
            poNumberListPrevDay.printSchema()
            //poNumberListPrevDay.saveAsTable("po_list")
            //poNumberListPrevDay.save(filePath + "po_list")
            poNumberListPrevDay.coalesce(1)
            .write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
           .mode("overwrite")
           .save(filePath + "PO_Numbers" +  "/" )
            
     }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkGroupedData at : " +  e.getMessage + " " + e.printStackTrace() )
     }
     
   }
   
   def filterConditionCheck(sqlContext: SQLContext, dfRcvDashboardPrev: DataFrame) = {
      
      /*val poListString1: (Map[String, String]) = null;
      poListString1.keys.toString().flatten*/
      
      sqlContext.udf.register("add_minutes", add_minutes _)
      val poListString:(Map[String, String]) => String =  _.map(_._1).mkString(",")  //_.keySet.toString()  //.flatten.toArray()
      val poListUDF = udf[String, Map[String, String]](poListString)
      val poNumberListPrevDay = dfRcvDashboardPrev.select( dfRcvDashboardPrev("warehouse_id"), dfRcvDashboardPrev("po_number_list") )
                                 .withColumn("po_numbers", poListUDF( dfRcvDashboardPrev("po_number_list") ) )
                                 .groupBy("warehouse_id").agg( expr("collect_set(po_numbers)").alias("po_number_list") )    
                                 //.select("warehouse_id", "po_number_list")
      
      poNumberListPrevDay.printSchema()
      poNumberListPrevDay.show()
      poNumberListPrevDay.coalesce(1)
            .write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
           .mode("overwrite")
           .save(filePath + "PONumbers" +  "/" )
      
      val secondPassBaseDataQuery = s""" WITH base_rows AS 
                                                   ( SELECT wrsd.destination warehouse_id
                                                           ,TRIM(wrsd.team_id) team_id
                                                           ,TRIM( wrsd.po_number ) po_number
                                                           ,MAX( wrsd.entry_date_timestamp ) OVER ( PARTITION BY wrsd.destination                                                                                                       
                                                                                                          ,TRIM(wrsd.team_id)
                                                                                                          ,TRIM( wrsd.po_number )
                                                                                            ) max_entry_date_timestamp
                                                           ,wrsd.entry_date_timestamp                                                           
                                                           ,wrsd.back_common
                                                           ,wrsd.damage_qty
                                                           ,wrsd.recv_qty
                                                           ,wrsd.shippers
                                                           ,wrsd.type
                                                           ,wot.ops_date
                                                    FROM warehouse_recv_secondpass_data wrsd
                                                    JOIN rcv_threshold_time rtt
                                                      ON rtt.wh_id = wrsd.destination
                                                    JOIN warehouse_operation_time wot
                                                       ON wrsd.destination = wot.facility
                                                        AND add_minutes( wrsd.entry_date_timestamp, ( CAST(rtt.threshold_time as int) ) )  BETWEEN wot.start_time AND wot.end_time                                                        
                                                    )
                                                   SELECT br.warehouse_id
                                                         ,TRIM(br.team_id) team_id
                                                         ,br.po_number
                                                         ,br.max_entry_date_timestamp
                                                         ,br.back_common
                                                         ,br.damage_qty
                                                         ,br.recv_qty
                                                         ,br.shippers
                                                         ,br.type
                                                         ,br.ops_date
                                                     FROM base_rows br
                                                    WHERE max_entry_date_timestamp = entry_date_timestamp
                                             """     
                  
               logger.debug( " - secondPassBaseDataQuery - " + secondPassBaseDataQuery )
               val secondPassBaseData = sqlContext.sql(secondPassBaseDataQuery).persist()
               
               secondPassBaseData.coalesce(1)
            .write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
           .mode("overwrite")
           .save(filePath + "SecondPass" +  "/" )
               
       val poAlreadyProcessedUDF = udf(poAlreadyProcessed _)
       
       val secondPassBaseDataExists = secondPassBaseData.alias("rcv")
          .join(poNumberListPrevDay, poNumberListPrevDay("warehouse_id") === secondPassBaseData("warehouse_id"), "inner" )
          .filter( !( poAlreadyProcessedUDF( secondPassBaseData("po_number"), poNumberListPrevDay("po_number_list") ) ) )
          .select( secondPassBaseData("warehouse_id"), secondPassBaseData("team_id"), secondPassBaseData("po_number")
                  ,secondPassBaseData("max_entry_date_timestamp"), secondPassBaseData("back_common"), secondPassBaseData("damage_qty") 
                  ,secondPassBaseData("recv_qty"), secondPassBaseData("shippers"), secondPassBaseData("type") 
                  ,secondPassBaseData("ops_date")  //, poNumberListPrevDay("po_number_list") 
                 )
          //.filter(  secondPassBaseData("po_number").in( poNumberListPrevDay("po_number_list") ) ) 
          //.filter(  poNumberListPrevDay("po_number_list").contains(secondPassBaseData("po_number")) )
          /*.map { x => Row( x.getString(0), x.getString(1), x.getString(2), x.getTimestamp(3), x.getString(4) 
                          ,x.getInt(5),x.getInt(6),x.getInt(7),x.getString(8),x.getTimestamp(9)
                        ) }*/
          //.withColumn( "po_exists", poAlreadyProcessed( secondPassBaseData("po_number"), poNumberListPrevDay("po_number_list") ))
          //.filter(   )
       // Ended on 10/23/2017
        logger.debug("Whole Data")
       secondPassBaseData.show()
       
       logger.debug("Not Exists")
       secondPassBaseDataExists.show()
       
       logger.debug("Exists")
       secondPassBaseData.except(secondPassBaseDataExists).show()
   }
   
   def intervalHourTesting(sqlContext: SQLContext) =
   {
     
     sqlContext.udf.register("add_minutes", add_minutes _)
     val secondPassBaseDataQuery = s""" WITH base_rows AS 
                                                   ( SELECT wrsd.destination warehouse_id
                                                           ,TRIM(wrsd.team_id) team_id
                                                           ,TRIM( wrsd.po_number ) po_number
                                                           ,MAX( wrsd.entry_date_timestamp ) OVER ( PARTITION BY wrsd.destination                                                                                                       
                                                                                                          ,TRIM(wrsd.team_id)
                                                                                                          ,TRIM( wrsd.po_number )
                                                                                            ) max_entry_date_timestamp
                                                           ,wrsd.entry_date_timestamp                                                           
                                                           ,wrsd.back_common
                                                           ,wrsd.damage_qty
                                                           ,wrsd.recv_qty
                                                           ,wrsd.shippers
                                                           ,wrsd.type
                                                           ,wot.ops_date
                                                           ,rtt.threshold_time
                                                    FROM warehouse_recv_secondpass_data wrsd
                                                    JOIN rcv_threshold_time rtt
                                                      ON rtt.wh_id = wrsd.destination
                                                    JOIN warehouse_operation_time wot
                                                       ON wrsd.destination = wot.facility
                                                        AND add_minutes( wrsd.entry_date_timestamp, ( CAST(rtt.threshold_time as int)  ) ) BETWEEN wot.start_time AND wot.end_time                                                        
                                                    )
                                                   SELECT br.warehouse_id
                                                         ,TRIM(br.team_id) team_id
                                                         ,br.po_number
                                                         ,br.max_entry_date_timestamp
                                                         ,br.back_common
                                                         ,br.damage_qty
                                                         ,br.recv_qty
                                                         ,br.shippers
                                                         ,br.type
                                                         ,br.ops_date
                                                         ,add_minutes( br.max_entry_date_timestamp, ( CAST(threshold_time as int) ) ) max_entry_date_timestamp_add
                                                     FROM base_rows br
                                                    WHERE max_entry_date_timestamp = entry_date_timestamp
                                             """     
     //,minutes_add( br.max_entry_date_timestamp, ( CAST(rtt.threshold_time as int) * 60 )  )  max_entry_date_timestamp_add
                  
               logger.debug( " - secondPassBaseDataQuery - " + secondPassBaseDataQuery )
               val secondPassBaseData = sqlContext.sql(secondPassBaseDataQuery).persist()
               
               logger.debug( " - secondPassBaseData Count - " + secondPassBaseData.count() )  
               secondPassBaseData.show( secondPassBaseData.count().toInt )
   }
   
   def getBaseDataIng(sqlContext: SQLContext, sc: SparkContext ) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                       getBaseDataIng                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
           
           sqlContext.udf.register("add_minutes", add_minutes _)
           try {
             
             val warehouseOperationDateQuery = s""" SELECT * 
                                                  FROM warehouse_operation_time
                                              """
         
             logger.debug( " - warehouseOperationDateQuery - " + warehouseOperationDateQuery )
             val warehouseOperationDate = sqlContext.sql(warehouseOperationDateQuery)
             warehouseOperationDate.show()
         
             /*
              *                                                   WHERE llbw.facility = 29
                                                    AND llbw.entrydate IN ('2017-10-18 00:00:00','2017-10-19 00:00:00','2017-10-20 00:00:00')
              */
                     val lumperLinkIngQuery = """SELECT CAST( wmd.warehouse_id as int ) warehouse_id, wot.ops_date, llbw.*
                                                   FROM lumperlink_data_by_warehouse llbw
                                                   JOIN whs_mapping_details wmd
                                                      ON TRIM(llbw.facility) = CAST( wmd.firstpass_warehouse_id as int )
                                                       AND wmd.enabled_flag = 'Y'
                                                   LEFT OUTER JOIN warehouse_operation_time wot
                                                    ON TRIM(llbw.facility) = wot.facility
                                                      AND llbw.totalloadstarttime BETWEEN wot.start_time AND wot.end_time 
                                                ORDER BY llbw.facility, llbw.entrydate, llbw.team, llbw.totalloadstarttime
                                              """
                     logger.debug("lumperLinkIngQuery - " + lumperLinkIngQuery )
                     val lumperLinkIng = sqlContext.sql(lumperLinkIngQuery)
                     //lumperLinkIng.show()
                     
                     lumperLinkIng
                     .coalesce(1)          
                     .write
                     .format("com.databricks.spark.csv")
                     .option("header", "true")
                     //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                     .mode("overwrite")
                     .save(filePath + "/LumperLinkIng/" ) 
                     
                     /*
                      *                                                 WHERE wrsd.destination = 29
                                                  AND entry_date IN ('2017-10-17 00:00:00','2017-10-18 00:00:00','2017-10-19 00:00:00')
                      */
                    val secondPassQuery = """SELECT wot.ops_date, wrsd.*
                                                   FROM warehouse_recv_secondpass_data wrsd
                                                   JOIN rcv_threshold_time rtt
                                                      ON rtt.wh_id = wrsd.destination
                                                   LEFT OUTER JOIN warehouse_operation_time wot
                                                    ON wrsd.destination = wot.facility
                                                        AND add_minutes( wrsd.entry_date_timestamp, ( CAST(rtt.threshold_time as int)  ) ) BETWEEN wot.start_time AND wot.end_time  
                                                ORDER BY wrsd.destination, wrsd.entry_date, wrsd.team_id, entry_date_timestamp, wrsd.po_number
                                              """
                     logger.debug("secondPassQuery - " + secondPassQuery )
                     val secondPassIng = sqlContext.sql(secondPassQuery)
                     //secondPassIng.show()
                             
                     secondPassIng
                     .coalesce(1)          
                     .write
                     .format("com.databricks.spark.csv")
                     .option("header", "true")
                     //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                     .mode("overwrite")
                     .save(filePath + "/SecondPassIng/" ) 
                     
                     
                     val rcvDashboardPrsQuery = """SELECT rdbw.*
                                                   FROM rcv_dashboard_by_whs_ops_date rdbw
                                                  WHERE rdbw.warehouse_id = 29
                                                    AND ops_date = '2017-10-25'
                                                ORDER BY rdbw.warehouse_id, ops_date, rdbw.team, rdbw.lumperlink_team, rdbw.wics_team
                                              """
                     logger.debug("rcvDashboardPrsQuery - " + rcvDashboardPrsQuery )
                     val rcvDashboardPrs = sqlContext.sql(rcvDashboardPrsQuery)
                     //lumperLinkIng.show()
                     
                     rcvDashboardPrs
                     .coalesce(1)          
                     .write
                     .format("com.databricks.spark.csv")
                     .option("header", "true")
                     //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                     .mode("overwrite")
                     .save(filePath + "/RcvDashboardPrs/" ) 
                     
                     val rcvDashboardPrsPOList = rcvDashboardPrs
                     .select("warehouse_id","ops_date","load_id", "appt_time", "carry_forwarded", "from_time","lumperlink_ticket_number"
                                           ,"po_number_list","rds_load_id","status", "team", "wics_team", "vendor_name"
                                           )
                     .map { x => if ( x.get(7) == null ) null 
                               else x.getAs[Map[String, String]](7)
                                                     .map { y => Row( x.getString(0), x.getTimestamp(1), x.getString(2)
                                                                     ,x.getTimestamp(3), x.getString(4), x.getTimestamp(5), x.getString(6)
                                                                     ,y._1
                                                                     ,x.getString(8), x.getString(9), x.getString(10), x.getString(11), x.getString(12)
                                                                    )
                                                         }
                          }
                     
                   logger.debug( " existingData " );
                   
                   var existingRows = new ListBuffer[Row]()    
                   for ( i <- 0 to ( rcvDashboardPrsPOList.count().toInt - 1 ) )  {
                       existingRows.++=(
                                   getRowRcv( rcvDashboardPrsPOList.collect().apply(i) )
                                   )
                   }
          
                  logger.debug( " existingRows " );
                  //combinedRow.foreach { println }
                  existingRows.foreach { println }

                    val rcvDashboardPrsPOListSchema = new StructType( Array( StructField("warehouse_id", StringType, false ) 
                                                      ,StructField("ops_date", TimestampType, false ) 
                                                      ,StructField("load_id", StringType, false ) 
                                                      ,StructField("appt_time", TimestampType, true ) 
                                                      ,StructField("carry_forwarded", StringType, false ) 
                                                      ,StructField("from_time", TimestampType, true ) 
                                                      ,StructField("lumperlink_ticket_number", StringType, true )
                                                      ,StructField("po_number", StringType, true )
                                                      ,StructField("rds_load_id", StringType, true ) 
                                                      ,StructField("status", StringType, true ) 
                                                      ,StructField("team", StringType, true )
                                                      ,StructField("wics_team", StringType, true )
                                                      ,StructField("vendor_name", TimestampType, true )
                                                      )
                                                        )  
         
                   val rcvDashboardPrsPOListFinal = sqlContext.createDataFrame( sc.parallelize(existingRows.toList), rcvDashboardPrsPOListSchema)
                   logger.debug( " - Update Dataframe - " )     
                   rcvDashboardPrsPOListFinal.show(40)
                   
                   rcvDashboardPrsPOListFinal.coalesce(1).write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   .mode("overwrite")
                   .save(filePath + "RcvDashboardPrsPOList/")
           }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
   }
   
   def updateRcvUnplanned(sqlContext: SQLContext) = {
       val currentTime = getCurrentTime()
       val unplannedPOQuery = s""" SELECT warehouse_id, ops_date
                                         ,TRIM(po_number) po_number 
                                    FROM rcv_unplanned_po_by_warehouse
                               """
       val unplannedPO = sqlContext.sql(unplannedPOQuery)
       
       unplannedPO
             .write       
             .format("org.apache.spark.sql.cassandra")
             .options( Map("keyspace" -> "wip_rcv_prs"
                           ,"table" -> "rcv_unplanned_po_by_warehouse"))
             .mode("append")
             .save()
   }
   
   
def getDuration(sqlContext: SQLContext ): String = {
      val currentTime = getCurrentTime()
      val dashboardTimeQuery = s""" SELECT warehouse_id, ops_date, load_id, duration, from_time, from_time_update_flag
                                     FROM rcv_dashboard_by_whs_ops_date
                                     WHERE warehouse_id = '29' 
                                       AND ops_date = '2017-09-18'
                                       AND load_id = 'fd4be224-d446-3804-8fce-d14e84a94d97'
                                """
     val dashboardTime = sqlContext.sql(dashboardTimeQuery)
     dashboardTime.show() 
     val fromTime = dashboardTime.first().getTimestamp(4)
     
     logger.debug( " - fromTime - " + fromTime + "\n"
                 + " - currentTime - " + currentTime + "\n"
                // + " - fromTime.getTime - " + fromTime.getTime + "\n"
                 + " - currentTime.getTime - " + currentTime.getTime + "\n"
                   )
                   
      val duration =  ( currentTime.getTime - ( if (fromTime == null) currentTime.getTime else fromTime.getTime ) ) / 60000 
                     /*( ( unix_timestamp( lit(currentTime) ) 
                       - unix_timestamp( lit(fromTime) ) 
                       ) / 60
                     )*/

      logger.debug( " - duration - " + duration + "\n"
                  + " - duration - " + duration.toString() + "\n"
                 // + " - duration format - " + duration.toFloat.formatted("%1.28")
                   )
      return duration.toString() //"$duration%1.2f" //.formatted("%1.2f")    //toString()
  }  
  
 def baseData(sqlContext: SQLContext) : DataFrame = {
         logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                       baseData                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
           
          val returnDataFrame: DataFrame = null
          sqlContext.udf.register("add_minutes", add_minutes _)
           
           try {
                  //                                                          ,rlid.ord_cpubackhaul_flag order_cpubackhaul_flag
                   val rdsScheduledLoadsQuery = s""" SELECT wot.facility warehouse_id 
                                                          ,wot.ops_date
                                                          ,rlid.load_id
                                                          ,rlid.load_id rds_load_id
                                                          ,rlid.start_time
                                                          ,regexp_replace( LTRIM( regexp_replace( TRIM( rlid.po_number ), '0', ' ' ) ), ' ', '0' ) po_number
                                                          ,rlid.ld_cpubackhaul_flag load_cpubackhaul_flag
                                                          ,rlid.vendor_code
                                                          ,rlid.vendor_name
                                                          ,rlid.sum_schedcases
                                                          ,rlid.sum_schedpallets
                                                          ,rlid.po_date
                                                          ,rlid.po_id                                                          
                                                          ,rlid.po_due_date
                                                          ,rlid.sum_schedcases ordered_cases
                                                          ,rlid.sum_schedpallets ordered_pallets 
                                                      FROM rds_loads_ing_details rlid
                                                          ,whs_mapping_details wmd
                                                          ,warehouse_operation_time wot
                                                     WHERE rlid.warehouse_id = wmd.rds_owner_code
                                                       AND wmd.enabled_flag = 'Y'
                                                       AND CAST( wmd.warehouse_id as int ) = wot.facility
                                                       AND rlid.start_time BETWEEN wot.start_time AND wot.end_time
                                                """


               logger.debug( " - rdsScheduledLoadsQuery - " + rdsScheduledLoadsQuery )
               val rdsScheduledLoads = sqlContext.sql(rdsScheduledLoadsQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rdsScheduledLoads Count - " + rdsScheduledLoads.count() )  
                 rdsScheduledLoads.show()
               }
               
               val rdsCommonsLoads = rdsScheduledLoads.filter("load_cpubackhaul_flag = 'F' ")
               rdsCommonsLoads.registerTempTable("rds_commons_loads")
               sqlContext.cacheTable("rds_commons_loads")
               logger.debug( " - rds_commons_loads table registered and cached - " ) 
               
               if ( saveDataFrame == "Y" ) {
                 rdsCommonsLoads
                 .coalesce(1)          
                 .write
                 .format("com.databricks.spark.csv")
                 .option("header", "true")
                 //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                 .mode("overwrite")
                 .save(filePath + "/RDSCommonsData/" ) 
               }
               
               val rdsScheduledBackhaulLoads = rdsScheduledLoads.filter("load_cpubackhaul_flag = 'T' ")
               rdsScheduledBackhaulLoads.registerTempTable("rds_scheduled_backhaulLoads")
               sqlContext.cacheTable("rds_scheduled_backhaulLoads")
               logger.debug( " - rds_scheduled_backhaulLoads table registered and cached - " ) 
                              
               //Backhauls
               //   ,warehouse_operation_time wot
               val rdsUnScheduledLoadsQuery = s""" SELECT wot.facility warehouse_id 
                                                          ,wot.ops_date
                                                          ,null load_id
                                                          ,null rds_load_id
                                                          ,null start_time
                                                          ,regexp_replace( LTRIM( regexp_replace( TRIM( rlid.po_number ), '0', ' ' ) ), ' ', '0' ) po_number
                                                          ,'T' load_cpubackhaul_flag
                                                          ,rlid.vendor_code
                                                          ,rlid.vendor_name
                                                          ,rlid.sum_schedcases
                                                          ,rlid.sum_schedpallets
                                                          ,rlid.po_date
                                                          ,rlid.po_id
                                                          ,rlid.po_due_date
                                                          ,rlid.ordered_cases
                                                          ,rlid.ordered_pallets
                                                      FROM rds_unsched_bh_ing_details rlid
                                                      JOIN whs_mapping_details wmd
                                                        ON rlid.warehouse_id = wmd.rds_owner_code
                                                         AND wmd.enabled_flag = 'Y'
                                                      JOIN warehouse_operation_time wot 
                                                        ON CAST( wmd.warehouse_id as int ) = wot.facility
                                                         AND rlid.po_due_date >= wot.ops_date
                                                      LEFT OUTER JOIN rds_scheduled_backhaulLoads rsb
                                                        ON wot.facility = rsb.warehouse_id
                                                         AND wot.ops_date = rsb.ops_date
                                                         AND TRIM( rlid.po_number ) = rsb.po_number
                                                     WHERE rsb.po_number IS NULL 
                                                """

               logger.debug( " - rdsUnScheduledLoadsQuery - " + rdsUnScheduledLoadsQuery )
               val rdsUnScheduledLoads = sqlContext.sql(rdsUnScheduledLoadsQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rdsUnScheduledLoads Count - " + rdsUnScheduledLoads.count() )  
                 rdsUnScheduledLoads.show()
               }
               
               val rdsBackhaulLoads = rdsScheduledBackhaulLoads.unionAll(rdsUnScheduledLoads)
               rdsBackhaulLoads.registerTempTable("rds_backhaul_loads")
               sqlContext.cacheTable("rds_backhaul_loads")
               logger.debug( " - rds_backhaul_loads table registered and cached - " ) 
               
               if ( saveDataFrame == "Y" ) {
               rdsBackhaulLoads
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RDSBackhaulsData/" ) 
               }
               
               //LEFT OUTER 
               val secondPassBaseDataQuery = s""" WITH base_rows AS 
                                                   ( SELECT wrsd.destination warehouse_id
                                                           ,TRIM(wrsd.team_id) team_id
                                                           ,regexp_replace( LTRIM( regexp_replace( TRIM( wrsd.po_number ), '0', ' ' ) ), ' ', '0' ) po_number
                                                           ,MAX( wrsd.entry_date_timestamp ) OVER ( PARTITION BY wrsd.destination                                                                                                       
                                                                                                          ,TRIM(wrsd.team_id)
                                                                                                          ,TRIM( wrsd.po_number )
                                                                                            ) max_entry_date_timestamp
                                                           ,wrsd.entry_date_timestamp                                                           
                                                           ,wrsd.back_common
                                                           ,wrsd.damage_qty
                                                           ,wrsd.recv_qty
                                                           ,wrsd.shippers
                                                           ,wrsd.type
                                                           ,wot.ops_date
                                                           ,CAST(rtt.threshold_time as int) threshold_time
                                                           ,wot.start_time
                                                    FROM warehouse_recv_secondpass_data wrsd
                                                    JOIN rcv_threshold_time rtt
                                                      ON rtt.wh_id = wrsd.destination
                                                    JOIN warehouse_operation_time wot
                                                       ON wrsd.destination = wot.facility
                                                        AND wrsd.entry_date_timestamp BETWEEN wot.start_time AND wot.end_time                                                       
                                                    )
                                                   SELECT br.warehouse_id
                                                         ,TRIM(br.team_id) team_id
                                                         ,br.po_number
                                                         ,br.max_entry_date_timestamp
                                                         ,CASE WHEN TRIM(br.back_common) = 'C' THEN 'F'
                                                               WHEN TRIM(br.back_common) = 'B' THEN 'T'
                                                               ELSE 'F'
                                                           END back_common
                                                         ,br.damage_qty
                                                         ,br.recv_qty
                                                         ,br.shippers
                                                         ,br.type
                                                         ,br.ops_date
                                                         ,br.threshold_time
                                                         ,br.start_time
                                                     FROM base_rows br
                                                    WHERE max_entry_date_timestamp = entry_date_timestamp
                                             """          
                  
               logger.debug( " - secondPassBaseDataQuery - " + secondPassBaseDataQuery )
               val secondPassBaseData = sqlContext.sql(secondPassBaseDataQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - secondPassBaseData Count - " + secondPassBaseData.count() )  
                 secondPassBaseData.show()
               }
               
               secondPassBaseData.registerTempTable("secondpass_base_data")
               sqlContext.cacheTable("secondpass_base_data")
               logger.debug( " - secondpass_base_data table registered and cached - " )
               
               if ( saveDataFrame == "Y" ) {
               secondPassBaseData
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/WICSData/" )
               }
               
              val LumperLinkBaseCompleteDataQuery = s""" WITH base_rows AS 
                                                             ( SELECT wot.facility warehouse_id
                                                                     ,wot.ops_date 
                                                                     ,llbw.ticketnumber ticket_number
                                                                     ,TRIM(llbw.team) team_id
                                                                     ,regexp_replace( LTRIM( regexp_replace( TRIM( llbw.po ), '0', ' ' ) ), ' ', '0' ) po_number
                                                                     ,llbw.door door_number
                                                                     ,llbw.status
                                                                     ,llbw.totalloadstarttime
                                                                     ,llbw.endingpallets total_pallet_qty
                                                                     ,MAX( llbw.damage ) OVER (PARTITION BY llbw.facility, llbw.ticketnumber) damage_qty
                                                                     ,llbw.vendor vendor_name
                                                                     ,llbw.entrydatetimestamp update_time
                                                                     ,MAX( llbw.endingpallets ) OVER (PARTITION BY llbw.facility, llbw.ticketnumber) ending_pallets
                                                                     ,MAX( llbw.entrydatetimestamp ) OVER (PARTITION BY llbw.facility, llbw.ticketnumber) max_update_time
                                                                     ,MAX( TRIM(revenuetype) ) OVER (PARTITION BY llbw.facility, llbw.ticketnumber) back_common
                                                                     ,CAST(rtt.threshold_time as int) threshold_time
                                                                     ,wot.start_time 
                                                                FROM lumperlink_data_by_warehouse llbw
                                                                JOIN whs_mapping_details wmd
                                                                  ON TRIM(llbw.facility) = CAST( wmd.firstpass_warehouse_id as int )
                                                                   AND wmd.enabled_flag = 'Y'
                                                                JOIN rcv_threshold_time rtt
                                                                  ON rtt.wh_id = CAST( wmd.warehouse_id as int )
                                                                JOIN warehouse_operation_time wot
                                                                  ON CAST( wmd.warehouse_id as int ) = wot.facility
                                                                    AND llbw.totalloadstarttime BETWEEN wot.start_time AND wot.end_time                                                        
                                                               )
                                                              SELECT warehouse_id
                                                                    ,ops_date
                                                                    ,ticket_number
                                                                    ,team_id
                                                                    ,po_number
                                                                    ,door_number
                                                                    ,status
                                                                    ,totalloadstarttime
                                                                    ,total_pallet_qty
                                                                    ,damage_qty
                                                                    ,vendor_name
                                                                    ,ending_pallets
                                                                    ,max_update_time
                                                                    ,CASE WHEN back_common = 'Common Carrier' THEN 'F'
                                                                          WHEN back_common = 'Backhaul' THEN 'T'
                                                                          ELSE 'F'
                                                                      END load_cpubackhaul_flag
                                                                    ,threshold_time
                                                                    ,start_time
                                                                FROM base_rows 
                                                               WHERE max_update_time = update_time
                                                     """
                   logger.debug( " - LumperLinkBaseCompleteDataQuery - " + LumperLinkBaseCompleteDataQuery )
                   val LumperLinkBaseCompleteData = sqlContext.sql(LumperLinkBaseCompleteDataQuery).persist()
                   
                   if ( loggingEnabled == "Y" ) {
                     logger.debug( " - LumperLinkBaseCompleteData Count - " + LumperLinkBaseCompleteData.count() )  
                     LumperLinkBaseCompleteData.show()
                   }
                   
                   LumperLinkBaseCompleteData.registerTempTable("lumperlink_complete_data")
                   sqlContext.cacheTable("lumperlink_complete_data")
                   logger.debug( " - lumperlink_base_data table registered and cached - " )
                   
                   if ( saveDataFrame == "Y" ) {
                   LumperLinkBaseCompleteData
                   .coalesce(1)          
                   .write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                   .mode("overwrite")
                   .save(filePath + "/LumperLinkData/" )      
                   }
                   
                   return rdsCommonsLoads
           }
         catch{
             case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing baseData at : " +  e.getMessage + " " + e.printStackTrace() )
                return returnDataFrame
           }
  }  
  
  // Added on 11/20/2017 
  def listPrevOpsDayPOs(sc: SparkContext, sqlContext: SQLContext
                       ,dfRcvDashboardPrev: DataFrame, dfWarehouseOperationTimeToday: DataFrame
                       //,dfRcvDashboardPrevSpcl: DataFrame, dfWarehouseOperationTimePrev: DataFrame
                      ): String = {
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                listPrevOpsDayPOs                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
         val currentTime = getCurrentTime()
         var returnResult = "0"
         //val poExistsSecondPassUDF = udf(poExistsSecondPass _)
         
         try {
                        
             val rcvDashboardStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false),
                                                             StructField("ops_date_new", TimestampType, nullable=false),
                                                             StructField("ops_date", TimestampType, nullable=false),
                                                             StructField("load_id", StringType, nullable=false),
                                                             StructField("po_number", StringType, nullable=false),
                                                             StructField("po_status", StringType, nullable=true),    // Added on 12/07/2017
                                                             StructField("po_vendor", StringType, nullable=true),    // Added on 12/07/2017                                                             
                                                             StructField("team", StringType, nullable=true),
                                                             StructField("from_time", TimestampType, nullable=true),
                                                             StructField("from_time_update_flag", StringType, nullable=true),
                                                             StructField("team_update_flag", StringType, nullable=true),
                                                             StructField("vendor_name", StringType, nullable=true),
                                                             StructField("load_cpubackhaul_flag", StringType, nullable=true),
                                                             StructField("appt_time", TimestampType, nullable=true),
                                                             StructField("status", StringType, nullable=true),
                                                             StructField("source_dataframe", StringType, nullable=true)      // Added on 12/07/2017                                                             
                                                           )
                                                    )
                
              val dfRcvDashboardPrevPO = dfRcvDashboardPrev
                                         .join(dfWarehouseOperationTimeToday.alias("wot"), ( dfRcvDashboardPrev("warehouse_id") === dfWarehouseOperationTimeToday("facility") ), "inner" )
                                         .select( dfRcvDashboardPrev("warehouse_id"), dfWarehouseOperationTimeToday("ops_date"), dfRcvDashboardPrev("ops_date")
                                                 ,dfRcvDashboardPrev("load_id"), dfRcvDashboardPrev("po_number_list"), dfRcvDashboardPrev("team")
                                                 ,dfRcvDashboardPrev("from_time"), dfRcvDashboardPrev("from_time_update_flag"), dfRcvDashboardPrev("team_update_flag")
                                                 ,dfRcvDashboardPrev("vendor_name"), dfRcvDashboardPrev("load_cpubackhaul_flag"), dfRcvDashboardPrev("appt_time")
                                                 ,dfRcvDashboardPrev("status"), dfRcvDashboardPrev("source_dataframe")  // Added on 12/07/2017                                                
                                                 )
                                         .map { x => ( if ( x.get(4) == null ) null 
                                                     else x.getAs[Map[String, String]](4) )
                                                    .map { y => Row( //warehouse_id, ops_date_new, ops_date
                                                                      x.getString(0), x.getTimestamp(1), x.getTimestamp(2)
                                                                     //load_id, po_number, po_status, po_vendor
                                                                     ,x.getString(3), y._1, y._2.toString().split(",").apply(0).drop(1), y._2.toString().split(",").apply(1).dropRight(1)
                                                                     //team, from_time
                                                                     ,x.getString(5), x.getTimestamp(6)
                                                                     //from_time_update_flag, team_update_flag, vendor_name
                                                                     ,x.getString(7), x.getString(8), x.getString(9)
                                                                     //load_cpubackhaul_flag, appt_time, status, source_dataframe
                                                                     ,x.getString(10), x.getTimestamp(11), x.getString(12), x.getString(13) // Added on 12/07/2017
                                                                   ) 
                                                        }
                                              }
             
             val dfRcvDashboardPrevPOFlat = dfRcvDashboardPrevPO.toArray().flatten
             logger.debug(" - dfRcvDashboardFlat flattened " )
             
             val rcvDashboardPOFinal = sqlContext.createDataFrame(sc.parallelize(dfRcvDashboardPrevPOFlat), rcvDashboardStruct).persist()
             logger.debug(" - rcvDashboardPOFinal created " )             
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rcvDashboardPOFinal - " + rcvDashboardPOFinal.count() )     
                 rcvDashboardPOFinal
                 .show(rcvDashboardPOFinal.count().toInt)
             }     
             rcvDashboardPOFinal.registerTempTable("rcv_dashboard_prev")   // Added on 12/07/2017
             sqlContext.cacheTable("rcv_dashboard_prev") // Added on 12/07/2017
             
             if ( saveDataFrame == "Y" ) {
                   rcvDashboardPOFinal
                   .coalesce(1)          
                   .write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                   .mode("overwrite")
                   .save(filePath + "/RcvDashboardPrevPO/" )      
              }
             /*val dfRcvDashboardPrevSpclPO = dfRcvDashboardPrevSpcl.alias("rcv")
                                         .join(dfWarehouseOperationTimePrev.alias("wot"), ( dfRcvDashboardPrevSpcl("warehouse_id") === dfWarehouseOperationTimePrev("facility") ), "inner" )
                                         .filter( s" NVL(wot.rcv_ops_day_delayed_process, 'N' ) = 'N' " )
                                         .select( dfRcvDashboardPrevSpcl("warehouse_id"), dfWarehouseOperationTimePrev("ops_date"), dfRcvDashboardPrevSpcl("ops_date")
                                                 ,dfRcvDashboardPrevSpcl("load_id"), dfRcvDashboardPrevSpcl("po_number_list"), dfRcvDashboardPrevSpcl("team")
                                                 ,dfRcvDashboardPrevSpcl("from_time"), dfRcvDashboardPrevSpcl("from_time_update_flag"), dfRcvDashboardPrevSpcl("team_update_flag")
                                                 ,dfRcvDashboardPrevSpcl("vendor_name"), dfRcvDashboardPrevSpcl("load_cpubackhaul_flag"), dfRcvDashboardPrevSpcl("appt_time")
                                                 ,dfRcvDashboardPrevSpcl("status"), dfRcvDashboardPrev("source_dataframe")  // Added on 12/07/2017
                                                 )
                                         .map { x => ( if ( x.get(4) == null ) null 
                                                     else x.getAs[Map[String, String]](4) )
                                                    .map { y => Row( //warehouse_id, ops_date_new, ops_date
                                                                      x.getString(0), x.getTimestamp(1), x.getTimestamp(2)
                                                                     //load_id, po_number, po_status, po_vendor
                                                                     ,x.getString(3), y._1, y._2.toString().split(",").apply(0).drop(1), y._2.toString().split(",").apply(1).dropRight(1)
                                                                     //team, from_time
                                                                     ,x.getString(5), x.getTimestamp(6)
                                                                     //from_time_update_flag, team_update_flag, vendor_name
                                                                     ,x.getString(7), x.getString(8), x.getString(9)
                                                                     //load_cpubackhaul_flag, appt_time, status, source_dataframe
                                                                     ,x.getString(10), x.getTimestamp(11), x.getString(12), x.getString(13) // Added on 12/07/2017
                                                                   ) 
                                                        }
                                              }
             
             val dfRcvDashboardPrevPOSpclFlat = dfRcvDashboardPrevSpclPO.toArray().flatten
             logger.debug(" - dfRcvDashboardPrevPOSpclFlat flattened " )
             
             val rcvDashboardPOSpclFinal = sqlContext.createDataFrame(sc.parallelize(dfRcvDashboardPrevPOSpclFlat), rcvDashboardStruct).persist()
             logger.debug(" - rcvDashboardPOSpclFinal created " )             
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rcvDashboardPOSpclFinal - " + rcvDashboardPOSpclFinal.count() )     
                 rcvDashboardPOSpclFinal
                 .show(rcvDashboardPOFinal.count().toInt)
             }          
             
             val rcvDashboard = rcvDashboardPOFinal.unionAll(rcvDashboardPOSpclFinal)
             rcvDashboard.registerTempTable("rcv_dashboard_prev_po")
             sqlContext.cacheTable("rcv_dashboard_prev_po")  
             
             if ( saveDataFrame == "Y" ) {
                   rcvDashboard
                   .coalesce(1)          
                   .write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                   .mode("overwrite")
                   .save(filePath + "/RcvDashboardPrevPO/" )      
              }*/
             
  
      }catch{
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
            logger.error("Error while executing listPrevOpsDayPOs at : " +  e.getMessage + " " + e.printStackTrace() )
            "1"
       }
       
       "0"
   } 
  // Ended on 11/20/2017
  
  def backhaulsPOLoad(sqlContext: SQLContext, rdsCommonsLoad: DataFrame) = {
       // dfRcvDashboardToday and sc Added on 10/31/2017
       // dfRcvDashboardToday and sc removed on 11/20/2017     
        logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   backhaulsPOLoad                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
         val currentTime = getCurrentTime()
         sqlContext.udf.register( "getLoadId", getLoadId _)
         try {  
           
                //Modified on 10/23/2017 - Load id was derived from rbl.vendor_code
                // this caused an issue in case 2 different POs are coming from Same vendor e.g. 892012 and 892055
                // came from same vendor - 12575 - COB!GMI-PALMYRA CSF DRY           
                /*val backhaulBaseQuery = s"""SELECT rbl.warehouse_id
                                                   ,rbl.ops_date
                                                   ,getLoadId( NVL(rbl.load_id, rbl.po_number) ) load_id
                                                   ,rbl.load_id rds_load_id
                                                   ,rbl.start_time appt_time
                                                   ,rbl.vendor_name
                                                   ,rbl.po_number
                                                   ,rbl.po_id
                                                   ,rbl.vendor_code
                                                   ,rbl.sum_schedcases
                                                   ,rbl.sum_schedpallets
                                                   ,rbl.po_due_date
                                                   ,rbl.po_date
                                                   ,rbl.ordered_cases
                                                   ,rbl.ordered_pallets
                                                   ,'$currentTime' upsert_timestamp
                                              FROM rds_backhaul_loads rbl
                                              LEFT OUTER JOIN rcv_unplanned_po_by_warehouse rupw
                                                ON rbl.warehouse_id = rupw.warehouse_id
                                                  AND rbl.ops_date = rupw.ops_date
                                                  AND rbl.po_number = regexp_replace( LTRIM( regexp_replace( TRIM( rupw.po_number ), '0', ' ' ) ), ' ', '0' )
                                             WHERE ( rupw.po_number IS NULL OR NVL( carry_forwarded, 'N' ) = 'Y' ) 
                                         """
               logger.debug( " - backhaulBaseQuery - " + backhaulBaseQuery )
               val backhaulBase = sqlContext.sql(backhaulBaseQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - backhaulBase Count - " + backhaulBase.count() )  
                 backhaulBase.show()
               }
               
               logger.debug( " - backhaulsPOQuery saved in unplanned" )
               backhaulBase.unpersist()*/
               //LEFT OUTER 
               //,NVL( plid.shift_date, '$currentTime' ) ops_date
               /*
                *                                        ,plid.po_number priority_po_number
                                                         ,plid.priority_num
                                                         ,'$currentTime' upsert_timestamp 
                                                         ,'$po_pending' po_status 
                */
               val unplannedBackhaulsPOQuery = s""" SELECT rbl.warehouse_id
                                                         ,plid.ops_date
                                                         ,rbl.load_id
                                                         ,rbl.rds_load_id
                                                         ,rbl.appt_time start_time
                                                         ,regexp_replace( LTRIM( regexp_replace( TRIM( rbl.po_number ), '0', ' ' ) ), ' ', '0' ) po_number
                                                         ,'T' load_cpubackhaul_flag
                                                         ,rbl.vendor_code
                                                         ,rbl.vendor_name
                                                         ,rbl.sum_schedcases
                                                         ,rbl.sum_schedpallets
                                                         ,rbl.po_date
                                                         ,rbl.po_id
                                                         ,rbl.po_due_date
                                                         ,rbl.ordered_cases
                                                         ,rbl.ordered_pallets
                                                     FROM rcv_unplanned_po_by_warehouse rbl
                                                     JOIN priority_list_ing_details plid
                                                       ON rbl.warehouse_id = plid.warehouse_id
                                                        AND rbl.ops_date = plid.ops_date
                                                        AND regexp_replace( LTRIM( regexp_replace( TRIM( rbl.po_number ), '0', ' ' ) ), ' ', '0' )
                                                                  = regexp_replace( LTRIM( regexp_replace( TRIM( plid.po_number ), '0', ' ' ) ), ' ', '0' ) 
                                                        AND plid.active_flag = 'Y'  
                                              """
               //WHERE NVL( rbl.delete_flag, 'N' ) = 'N'  Removed on 10/23/2017 - 
               // If PO came in Priority list in the middle of day then it will be marked as Deleted and in the next
               // run it wouldn't be picked up if we keep where clause. "where" clause is removed so that this PO is  
               // picked up for whole ops day.
               
               logger.debug( " - unplannedBackhaulsPOQuery - " + unplannedBackhaulsPOQuery )
               val unplannedBackhaulsPO = sqlContext.sql(unplannedBackhaulsPOQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - unplannedBackhaulsPO Count - " + unplannedBackhaulsPO.count() )  
                 unplannedBackhaulsPO.show()
               }
               
               if ( saveDataFrame == "Y" ) {
                  unplannedBackhaulsPO
                   .coalesce(1)          
                   .write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                   .mode("overwrite")
                   .save(filePath + "/UnplannedBackhaulsPriority/" )
               }
               
                 //Added on 10/31/2017 
                /*var existingRows: List[Row] = null  
                case class loadIds(warehouse_id: String, ops_date: Timestamp, load_id: String, rds_load_id: String
                                  ,start_time: Timestamp, po_number: String, load_cpubackhaul_flag: String
                                  ,vendor_code: String, vendor_name: String, sum_schedcases: String
                                  ,sum_schedpallets: String, po_date: String, po_id: String
                                  ,po_due_date: String, ordered_cases: String, ordered_pallets: String
                                  )*/
                                  
                /*val rcvDashboardStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("load_id", StringType, nullable=false),
                                                        StructField("rds_load_id", StringType, nullable=false),
                                                        StructField("start_time", TimestampType, nullable=true),
                                                        StructField("po_number", StringType, nullable=false),
                                                        StructField("load_cpubackhaul_flag", StringType, nullable=true),
                                                        StructField("vendor_code", StringType, nullable=true),
                                                        StructField("vendor_name", StringType, nullable=true),
                                                        StructField("sum_schedcases", StringType, nullable=true),
                                                        StructField("sum_schedpallets", StringType, nullable=true),
                                                        StructField("po_date", TimestampType, nullable=true),
                                                        StructField("po_id", StringType, nullable=true),
                                                        StructField("po_due_date", TimestampType, nullable=true),
                                                        StructField("ordered_cases", StringType, nullable=true),
                                                        StructField("ordered_pallets", StringType, nullable=true)
                                                       )
                                               )                                  
                
                /*                                             
                                                               // load_cpubackhaul_flag, vendor_code, sum_schedcases
                                                               ,null, null, null
                                                               //sum_schedpallets, po_date, po_id
                                                                ,null, null, null
                                                                //po_due_date, ordered_cases, ordered_pallets
                                                                ,null, null, null    
               */
              val dfRcvDashboard = dfRcvDashboardToday
                                         .filter(s" carry_forwarded = 'Y' AND status <> '$ticket_closed' ")
                                         .select("warehouse_id", "ops_date", "load_id", "rds_load_id", "appt_time"
                                                ,"po_number_list",  "load_cpubackhaul_flag"
                                               )
                                         .map { x => ( if ( x.get(5) == null ) null 
                                                else x.getAs[Map[String, String]](5) )
                                                .map { y => Row( //warehouse_id, ops_date, load_id
                                                                 x.getString(0), x.getTimestamp(1), x.getString(2)
                                                                 //rds_load_id, start_time, po_number, load_cpubackhaul_flag
                                                                ,x.getString(3), ( if( x.get(4) == null ) null else x.getTimestamp(4) ), y._1,  null //( if( x.get(6) == null ) null else x.getString(6) )
                                                                //vendor_code, vendor_name, sum_schedcases
                                                                ,null, y._2.split(",").apply(1).dropRight(1), null
                                                                //sum_schedpallets, po_date, po_id
                                                               ,null, null, null
                                                               //po_due_date, ordered_cases, ordered_pallets
                                                               ,null, null, null  
                                                               ) 
                                                    }
                                              }
             
             val dfRcvDashboardFlat = dfRcvDashboard.toArray().flatten
                
             //dfRcvDashboard.map { x => Row(x:_*) }            
             /*val dfRcvDashboardRDD = dfRcvDashboard.map { case List(x) => 
                                      Row( //warehouse_id, ops_date, load_id
                                            x.getString(0), x.getTimestamp(1), x.getString(2)
                                           //rds_load_id, start_time, po_number, load_cpubackhaul_flag
                                           ,x.getString(3), ( if( x.get(4) == null ) null else x.getTimestamp(4) ), x.getString(5), null
                                           //vendor_code, vendor_name, sum_schedcases
                                           ,null, ( if( x.get(6) == null ) null else x.getString(6) ), null
                                            //sum_schedpallets, po_date, po_id
                                           ,null, null, null
                                           //po_due_date, ordered_cases, ordered_pallets
                                           ,null, null, null  
                                          )
                               }*/
                          
             //dfRcvDashboardFinal1.toDF()
             val rcvDashboardPOFinal = sqlContext.createDataFrame(sc.parallelize(dfRcvDashboardFlat), rcvDashboardStruct)
             
             /*logger.debug( " dfRcvDashboard POList created " );
             if ( loggingEnabled == "Y" ) {
                 dfRcvDashboard.foreach { println }
             }
             
             for ( i <- 0 to ( dfRcvDashboard.count().toInt - 1 ) )  {
                 val listValue = dfRcvDashboard.collect().apply(i)
                 existingRows.++=( getRowRcv(listValue) )
             }
             
             logger.debug( " existingRows " );
             if ( loggingEnabled == "Y" ) {
               existingRows.foreach { println }
             }
             
             val rcvDashboardPOFinal = sqlContext.createDataFrame(sc.parallelize(existingRows.toList), rcvDashboardStruct)
             */
             
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rcvDashboardPOFinal - " + rcvDashboardPOFinal.count() )     
                 rcvDashboardPOFinal.show(rcvDashboardPOFinal.count().toInt)
             }          
             //Ended on 10/31/2017                 
             */
             val rdsCommonsLoadsAll = rdsCommonsLoad.unionAll(unplannedBackhaulsPO)  //.unionAll(rcvDashboardPOFinal)
             //union(rcvDashboardPOFinal) Added on 10/31/2017
               
            rdsCommonsLoadsAll.registerTempTable("rds_commons_loads_all")
            sqlContext.cacheTable("rds_commons_loads_all")
            logger.debug(" - rds_commons_loads_all is registered and cached" )
           
            if ( saveDataFrame == "Y" ) {
            rdsCommonsLoadsAll
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RDSCommonsUnionized/" )    
            }
               /*unplannedBackhaulsPO
               .select("warehouse_id", "ops_date", "load_id", "appt_time", "vendor_name", "rds_load_id")
               .write
               .format("org.apache.spark.sql.cassandra")
               .options( Map("keyspace" -> "wip_rcv_prs"
                           ,"table" -> "rcv_dashboard_by_whs_ops_date"))
               .mode("append")
               .save()   
               logger.debug( " - backhaulsPOQuery base saved in RCV Dashboard" )
                
              // Priority List 
               val backhaulPOsMap =  unplannedBackhaulsPO
                                       .map { x => ( ( x.getString(0), x.getTimestamp(1), x.getString(2) ) 
                                                    //rds_po_number, priority_num
                                                    ,getMappedData( x.getString(5), x.getInt(17).toString() )
                                                    //,getMappedData( x.getString(6), x.getString(17) )
                                                   )  
                                            }
                                       .groupByKey()
                                       .map(x => Row( x._1._1, x._1._2, x._1._3, x._2.toArray.flatten.toMap, currentTime ) )
                                                                   
                  
               val backhaulsPOStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("load_id", StringType, nullable=false),
                                                        StructField("priority_po_list", MapType(StringType, StringType, true), nullable=true),
                                                        StructField("upsert_timestamp", TimestampType, nullable=false)
                                                       )
                                               )
         
               val backhaulPOsFinal = sqlContext.createDataFrame(backhaulPOsMap, backhaulsPOStruct)

               if ( loggingEnabled == "Y" ) {
                  logger.debug(  " - backhaulPOsFinal - " )
                  backhaulPOsFinal.printSchema()
                  backhaulPOsFinal.show (backhaulPOsFinal.count().toInt)
               }
                                       
                backhaulPOsFinal       
               .write
               .format("org.apache.spark.sql.cassandra")
               .options( Map("keyspace" -> "wip_rcv_prs"
                           ,"table" -> "rcv_dashboard_by_whs_ops_date"))
               .mode("append")
               .save()   
               logger.debug( " - backhaulPOsFinal Priority List saved in RCV Dashboard" )
               
              // PO List                
             val backhaulPOListMap =  unplannedBackhaulsPO
                                     //.filter( "priority_po_number IS NOT NULL" )
                                       .map { x => ( ( x.getString(0), x.getTimestamp(1), x.getString(2) ) 
                                                    //rds_po_number, po_status
                                                     ,getMappedData( x.getString(5), x.getString(19) )
                                                   )  
                                            }
                                       .groupByKey()
                                       .map(x => Row( x._1._1, x._1._2, x._1._3, x._2.toArray.flatten.toMap, currentTime ) )
                                                                   
                  
               val backhaulPOListStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("load_id", StringType, nullable=false),
                                                        StructField("po_number_list", MapType(StringType, StringType, true), nullable=true),
                                                        StructField("upsert_timestamp", TimestampType, nullable=false)
                                                       )
                                               )
         
               val backhaulPOListFinal = sqlContext.createDataFrame(backhaulPOListMap, backhaulPOListStruct)

               if ( loggingEnabled == "Y" ) {
                  logger.debug(  " - backhaulPOListFinal - " )
                  backhaulPOListFinal.printSchema()
                  backhaulPOListFinal.show( backhaulPOListFinal.count().toInt )
               }
                                       
                backhaulPOListFinal       
               .write
               .format("org.apache.spark.sql.cassandra")
               .options( Map("keyspace" -> "wip_rcv_prs"
                           ,"table" -> "rcv_dashboard_by_whs_ops_date"))
               .mode("append")
               .save()   
               logger.debug( " - backhaulPOListFinal PO List saved in RCV Dashboard" )               
               */
               
               unplannedBackhaulsPO.unpersist()
               logger.debug( " - backhaulsPOQuery marked delete in unplanned" )   
               
           }
           catch{
             case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing backhaulsPOLoad at : " +  e.getMessage + " " + e.printStackTrace() )
          }
  }
  
  // Added on 11/20/2017 
  def removePOsProcessedPrevDay(sc: SparkContext, sqlContext: SQLContext): String = {
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "           removePOsProcessedPrevDay                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
         val currentTime = getCurrentTime()
         var returnResult = "0"
         //val poExistsSecondPassUDF = udf(poExistsSecondPass _)
         logger.debug( " - currentTime - " + currentTime )
         try {
                        
             //Remove from RDS commons
             val rdsCommonsQuery = s""" SELECT rcl.*
                                         FROM rds_commons_loads_all rcl
                                         LEFT OUTER JOIN rcv_dashboard_three_hr_po rdpp
                                           ON rcl.warehouse_id = rdpp.warehouse_id
                                            AND rcl.ops_date = rdpp.ops_date_processed
                                            AND rcl.po_number = rdpp.po_number
                                        WHERE ( rdpp.po_number IS NULL )
                                   """
             logger.debug ( " - rdsCommonsQuery - " +  rdsCommonsQuery )
             val rdsCommonsAllFinal = sqlContext.sql(rdsCommonsQuery).persist()
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rdsCommonsAllFinal Count - " + rdsCommonsAllFinal.count() )  
                 rdsCommonsAllFinal.show( rdsCommonsAllFinal.count().toInt )
               }
             rdsCommonsAllFinal.registerTempTable("rds_commons_final")
             sqlContext.cacheTable("rds_commons_final")
             
             /*if ( saveDataFrame.equalsIgnoreCase("Y") ){
                 saveDataFrame("file", rdsCommonsAllFinal, "warehouse_id, ops_date, load_id", "RDSCommonsFinal", "", "", "")
               }*/                           
                      
             if ( saveDataFrame == "Y" ) {
               rdsCommonsAllFinal
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RDSCommonsFinal/" ) 
               }
             
             //Remove from Second Pass
             // Modified on 12/06/2017 - To consider POs for below scenario:
             // PO - 105751 came from RDS for ops day 12/03 so it entered in Dashboard for 12/03 with Pending 
             // now the same PO came from WICS at 12/05 17:05 i.e. ops day 12/04 since OR condition is not 
             // present it is removing this PO from secondPass data as it is present i Previous ops day
             // But it should consider this PO after Previous Ops day processing is done             
             val secondPassQuery = s""" SELECT sbd.*
                                         FROM secondpass_base_data sbd
                                         LEFT OUTER JOIN rcv_dashboard_three_hr_po rdpp
                                           ON sbd.warehouse_id = rdpp.warehouse_id
                                            AND sbd.ops_date = rdpp.ops_date_processed
                                            AND sbd.po_number = rdpp.po_number
                                        WHERE rdpp.po_number IS NULL
                                   """
             logger.debug ( " - secondPassQuery - " +  secondPassQuery )
             val secondPassFinal = sqlContext.sql(secondPassQuery).persist()
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - secondPassFinal Count - " + secondPassFinal.count() )  
                 secondPassFinal.show( secondPassFinal.count().toInt )
             }
             secondPassFinal.registerTempTable("second_pass_final")                      
             sqlContext.cacheTable("second_pass_final")
             
             if ( saveDataFrame == "Y" ) {
               secondPassFinal
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/WICSFinal/" ) 
               }
             
             //Remove from Lumperlink
             // Modified on 12/06/2017 - Same reason as in secondPassQuery              
             val LumperLinkQuery = s""" SELECT lcd.*
                                         FROM lumperlink_complete_data lcd
                                         LEFT OUTER JOIN rcv_dashboard_three_hr_po rdpp
                                           ON lcd.warehouse_id = rdpp.warehouse_id
                                            AND lcd.ops_date = rdpp.ops_date_processed
                                            AND lcd.po_number = rdpp.po_number
                                        WHERE rdpp.po_number IS NULL
                                   """
             logger.debug ( " - LumperLinkQuery - " +  LumperLinkQuery )
             val LumperLinkFinal = sqlContext.sql(LumperLinkQuery).persist()
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - LumperLinkFinal Count - " + LumperLinkFinal.count() )  
                 LumperLinkFinal.show( LumperLinkFinal.count().toInt )
               }
             LumperLinkFinal.registerTempTable("lumperlink_final")
             sqlContext.cacheTable("lumperlink_final")
             if ( saveDataFrame == "Y" ) {
               LumperLinkFinal
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/LumperLinkFinal/" ) 
               }             
         }
         catch{
             case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
               returnResult = "1" 
               logger.error("Error while executing removePOsProcessedPrevDay at : " +  e.getMessage + " " + e.printStackTrace() )
          }
        
         return returnResult;
  }
  // Ended on 11/20/2017

  
     def basePerformanceData(sqlContext: SQLContext) = {
         logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "               basePerformanceData                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " ) 
           
           try {
              
               // This is fetched in separate query because of following reason
               // In case there are multiple records for team_id and po_number in warehouse_recv_secondpass_data
               // then both records will be joined with employee_details_by_warehouse in above query and result in 
               // double sum of actual_today_hours column
               /*val totalTeamHoursQuery = s""" SELECT sbd.warehouse_id
                                                    ,sbd.ops_date
                                                    ,sbd.team_id
                                                    ,NVL( edbw.team_type, '$teamType' ) team_type
                                                    ,SUM( NVL( edbw.actual_today_hours, 0 ) ) total_team_hrs
                                                FROM secondpass_base_data sbd
                                                LEFT OUTER JOIN employee_details_by_warehouse edbw
                                                        ON sbd.warehouse_id = edbw.warehouse_id 
                                                          AND sbd.max_entry_date_timestamp 
                                                              BETWEEN ( edbw.start_time - INTERVAL '1' HOUR ) AND ( edbw.end_time + INTERVAL '4' HOUR )
                                                          AND sbd.team_id = edbw.team
                                            GROUP BY sbd.warehouse_id
                                                    ,sbd.ops_date
                                                    ,sbd.team_id
                                                    ,NVL( edbw.team_type, '$teamType' )
                                          """*/
               val totalTeamHoursQuery = s""" SELECT wot.facility warehouse_id
                                                    ,edbw.shift_date ops_date
                                                    ,edbw.team team_id
                                                    ,NVL( edbw.team_type, '$teamType' ) team_type
                                                    ,SUM( NVL( edbw.actual_today_hours, 0 ) ) total_team_hrs
                                                FROM employee_details_by_warehouse edbw
                                                JOIN warehouse_operation_time wot
                                                   ON CAST( edbw.warehouse_id as Int) = wot.facility
                                                   AND edbw.shift_date = wot.ops_date                                                       
                                            GROUP BY wot.facility
                                                    ,edbw.shift_date
                                                    ,edbw.team
                                                    ,NVL( edbw.team_type, '$teamType' )
                                          """              
               
               logger.debug( " - totalTeamHoursQuery - " + totalTeamHoursQuery )
               val totalTeamHours = sqlContext.sql(totalTeamHoursQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - totalTeamHours Count - " + totalTeamHours.count() )  
                 totalTeamHours.show( totalTeamHours.count().toInt )
               }
               
               totalTeamHours.registerTempTable("total_team_hours")
               sqlContext.cacheTable("total_team_hours")
               saveDataFrameM("file", totalTeamHours, "warehouse_id, ops_date, team_id", "TotalTeamHours", "", "", "")
               logger.debug( " - total_team_hours table registered and cached - " )
                            
               val rcvDahsboardWICSQuery = s""" SELECT DISTINCT rdbw.warehouse_id
                                                  ,rdbw.ops_date
                                                  ,rdbw.team
                                                  ,rdbw.wics_team
                                                  ,NVL( tth.team_type, '$teamType' ) team_type
                                                  ,NVL( tth.total_team_hrs, 0 ) total_team_hrs
                                              FROM rcv_dashboard_by_whs_ops_date_all rdbw
                                              JOIN warehouse_operation_time wot
                                                ON CAST( rdbw.warehouse_id as Int) = wot.facility
                                                  AND rdbw.ops_date = wot.ops_date 
                                              LEFT OUTER JOIN total_team_hours tth
                                                ON CAST( rdbw.warehouse_id as Int) = tth.warehouse_id
                                                  AND rdbw.ops_date = tth.ops_date
                                                  AND rdbw.team = tth.team_id
                                             WHERE rdbw.wics_team IS NOT NULL
                                        """
               
               logger.debug( " - rcvDahsboardWICSQuery - " + rcvDahsboardWICSQuery )
               val rcvDahsboardWICS = sqlContext.sql(rcvDahsboardWICSQuery).persist()
               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rcvDahsboardWICS Count - " + rcvDahsboardWICS.count() )  
                 rcvDahsboardWICS.show( rcvDahsboardWICS.count().toInt )
               }
               
               rcvDahsboardWICS.registerTempTable("rcv_dashboard_by_whs_ops_date_wics")
               sqlContext.cacheTable("rcv_dashboard_by_whs_ops_date_wics")
               saveDataFrameM("file", rcvDahsboardWICS, "warehouse_id, ops_date, team", "RCVDashboradWICS", "", "", "")
               logger.debug( " - rcv_dashboard_by_whs_ops_date_wics table registered and cached - " )
               
               // Modified on 11/20/2017 - Modified to calculate Lumperlink columns from Dashboard table only 
               // total_pallet_qty and damage_qty are added
               // ticket_number alias added for lumperlink_ticket_number               
               val rcvDahsboardLumperLinkQuery = s""" SELECT DISTINCT rdbw.warehouse_id
                                                  ,rdbw.ops_date
                                                  ,rdbw.team
                                                  ,rdbw.lumperlink_team
                                                  ,rdbw.lumperlink_ticket_number ticket_number
                                                  ,NVL( tth.team_type, '$teamType' ) team_type
                                                  ,NVL( tth.total_team_hrs, 0 ) total_team_hrs
                                                  ,NVL( rdbw.lumperlink_ending_pallets, 0 ) total_pallet_qty
                                                  ,NVL( rdbw.lumperlink_damage_qty, 0 ) damage_qty
                                              FROM rcv_dashboard_by_whs_ops_date_all rdbw
                                              JOIN warehouse_operation_time wot
                                                ON CAST( rdbw.warehouse_id as Int) = wot.facility
                                                  AND rdbw.ops_date = wot.ops_date 
                                              LEFT OUTER JOIN total_team_hours tth
                                                ON CAST( rdbw.warehouse_id as Int) = tth.warehouse_id
                                                  AND rdbw.ops_date = tth.ops_date
                                                  AND rdbw.team = tth.team_id
                                        """
                                        //WHERE LENGTH( TRIM(rdbw.lumperlink_team) ) > 0 this is removed on 03/11/2017 as it is coming as null
               
               logger.debug( " - rcvDahsboardLumperLinkQuery - " + rcvDahsboardLumperLinkQuery )
               val rcvDahsboardLumperLink = sqlContext.sql(rcvDahsboardLumperLinkQuery).persist()               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rcvDahsboardLumperLink Count - " + rcvDahsboardLumperLink.count() )  
                 rcvDahsboardLumperLink.show( rcvDahsboardLumperLink.count().toInt )
               }
               rcvDahsboardLumperLink.registerTempTable("rcv_dashboard_by_whs_ops_date_ll")
               sqlContext.cacheTable("rcv_dashboard_by_whs_ops_date_ll")
               saveDataFrameM("file", rcvDahsboardLumperLink, "warehouse_id, ops_date, team", "RCVDashboradLL", "", "", "")
               logger.debug( " - rcv_dashboard_by_whs_ops_date_ll table registered and cached - " )
               
                                                      //AND br.team_id = '$teamIdTesting'  
               // Modified on 11/08/2017 - ops_date is added
               val secondPassBaseDataQuery = s""" WITH base_rows AS 
                                                   ( SELECT wrsd.destination warehouse_id
                                                           ,wrsd.team_id
                                                           ,wrsd.po_number
                                                           ,MAX( wrsd.entry_date_timestamp ) OVER ( PARTITION BY wrsd.destination                                                                                                        
                                                                                                          ,wrsd.team_id
                                                                                                          ,wrsd.po_number
                                                                                            ) max_entry_date_timestamp
                                                           ,wrsd.entry_date_timestamp                                                           
                                                           ,wrsd.back_common
                                                           ,wrsd.damage_qty
                                                           ,wrsd.recv_qty
                                                           ,wrsd.shippers
                                                           ,wrsd.type
                                                           ,wot.ops_date
                                                    FROM warehouse_recv_secondpass_data wrsd
                                                    JOIN warehouse_operation_time wot
                                                       ON wrsd.destination = wot.facility
                                                        AND wrsd.entry_date_timestamp BETWEEN wot.start_time AND wot.end_time                                                        
                                                    )
                                                   SELECT br.warehouse_id
                                                         ,br.team_id
                                                         ,br.po_number
                                                         ,br.max_entry_date_timestamp
                                                         ,br.back_common
                                                         ,br.damage_qty
                                                         ,br.recv_qty
                                                         ,br.shippers
                                                         ,br.type
                                                         ,br.ops_date
                                                     FROM base_rows br
                                                    WHERE max_entry_date_timestamp = entry_date_timestamp
                                             """     
                  
               logger.debug( " - secondPassBaseDataQuery - " + secondPassBaseDataQuery )
               val secondPassBaseData = sqlContext.sql(secondPassBaseDataQuery).persist()               
               if ( loggingEnabled == "Y" ) {
                 logger.debug( " - secondPassBaseData Count - " + secondPassBaseData.count() )  
                 secondPassBaseData.filter(s" ( po_number IN ($poNumber) ) ").show()
                 //( $poNumber ) IS NULL OR ( ( $poNumber ) IS NOT NULL AND 
               }               
               secondPassBaseData.registerTempTable("secondpass_base_data")
               sqlContext.cacheTable("secondpass_base_data")
               saveDataFrameM("file", secondPassBaseData, "warehouse_id, team_id, po_number", "SecondPassPerf", "", "", "")
               logger.debug( " - secondpass_base_data table registered and cached - " )
               //                                                            AND team_id = '$teamIdTesting'
               
               // Modified on 11/15/2017 - Join with whs_mapping_details added
               // Modified on 11/20/2017 - Below code is commented as Lumperlink column are calculated from Dashboard only               
               /*val LumperLinkBaseCompleteDataQuery = s""" WITH base_rows AS 
                                                             ( SELECT wot.facility warehouse_id
                                                                     ,wot.ops_date 
                                                                     ,llbw.ticketnumber ticket_number
                                                                     ,llbw.team team_id
                                                                     ,llbw.endingpallets total_pallet_qty
                                                                     ,llbw.damage damage_qty
                                                                     ,llbw.status
                                                                     ,llbw.entrydatetimestamp update_time
                                                                     ,MAX( llbw.entrydatetimestamp ) OVER (PARTITION BY llbw.facility, llbw.ticketnumber) max_update_time
                                                                FROM lumperlink_data_by_warehouse llbw
                                                                JOIN whs_mapping_details wmd
                                                                  ON TRIM(llbw.facility) = CAST( wmd.firstpass_warehouse_id as int )
                                                                   AND wmd.enabled_flag = 'Y'
                                                                JOIN warehouse_operation_time wot
                                                                  ON CAST( wmd.warehouse_id as int ) = wot.facility
                                                                    AND llbw.totalloadstarttime BETWEEN wot.start_time AND wot.end_time      
                                                              )
                                                              SELECT warehouse_id
                                                                    ,ops_date
                                                                    ,ticket_number
                                                                    ,team_id
                                                                    ,MAX( total_pallet_qty ) total_pallet_qty
                                                                    ,MAX( damage_qty ) damage_qty
                                                                    ,MAX( max_update_time ) max_update_time
                                                                FROM base_rows 
                                                               WHERE max_update_time = update_time
                                                            GROUP BY warehouse_id
                                                                    ,ops_date
                                                                    ,ticket_number
                                                                    ,team_id                                                                    
                                                     """
                   logger.debug( " - LumperLinkBaseCompleteDataQuery - " + LumperLinkBaseCompleteDataQuery )
                   val LumperLinkBaseCompleteData = sqlContext.sql(LumperLinkBaseCompleteDataQuery).persist()                   
                   if ( loggingEnabled == "Y" ) {
                     logger.debug( " - LumperLinkBaseCompleteData Count - " + LumperLinkBaseCompleteData.count() )  
                     LumperLinkBaseCompleteData.show()
                   }                   
                   LumperLinkBaseCompleteData.registerTempTable("lumperlink_complete_data")
                   sqlContext.cacheTable("lumperlink_complete_data")
                   saveDataFrame("file", LumperLinkBaseCompleteData, "warehouse_id, ops_date, ticket_number", "LumperLinkPerf", "", "", "")
                   logger.debug( " - lumperlink_base_data table registered and cached - " )
                    */
           }
           catch{
             case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing baseData at : " +  e.getMessage + " " + e.printStackTrace() )
           }
     }
     
    // Added on 11/20/2017 
    def removePOsPerfProcessedPrevDay( sqlContext: SQLContext): String = {
      //sc: SparkContext,, dfRcvDashboardPrev: DataFrame
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "           removePOsProcessedPrevDay                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )          
         try {
           
             /*val rcvDashboardStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                             StructField("ops_date", TimestampType, nullable=false),
                                                             StructField("po_number", StringType, nullable=false),
                                                             StructField("po_status", StringType, nullable=true),    // Added on 12/06/2017
                                                             StructField("status", StringType, nullable=true)        // Added on 12/06/2017                                                             
                                                           )
                                                    )
                
              val dfRcvDashboardPrevPO = dfRcvDashboardPrev
                                         //.join(dfWarehouseOperationTimeToday.alias("wot"), ( dfRcvDashboardPrev("warehouse_id") === dfWarehouseOperationTimeToday("facility") ), "inner" )
                                         .select( dfRcvDashboardPrev("warehouse_id"), dfRcvDashboardPrev("ops_date"), dfRcvDashboardPrev("po_number_list") 
                                                 ,dfRcvDashboardPrev("status") )
                                         .map { x => ( if ( x.get(2) == null ) null 
                                                     else x.getAs[Map[String, String]](2) )
                                                    .map { y => Row( //warehouse_id, ops_date
                                                                      x.getString(0), x.getTimestamp(1)
                                                                     //po_number, po_status
                                                                     ,y._1, y._2.toString().split(",").apply(0).drop(1)
                                                                     //status 
                                                                     ,x.getString(3)
                                                                   ) 
                                                        }
                                              }
             
             val dfRcvDashboardPrevPOFlat = dfRcvDashboardPrevPO.toArray().flatten
             logger.debug(" - dfRcvDashboardFlat flattened " )
             
             val rcvDashboardPOFinal = sqlContext.createDataFrame(sc.parallelize(dfRcvDashboardPrevPOFlat), rcvDashboardStruct).persist()
             logger.debug(" - rcvDashboardPOFinal created " )          
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - rcvDashboardPOFinal - " + rcvDashboardPOFinal.count() )     
                 rcvDashboardPOFinal.show(rcvDashboardPOFinal.count().toInt)
             }          
             rcvDashboardPOFinal.registerTempTable("rcv_dashboard_prev_po")
             sqlContext.cacheTable("rcv_dashboard_prev_po")   
             saveDataFrameM("file", rcvDashboardPOFinal, "warehouse_id, team_id, po_number", "RCVPerformPrevPOs", "", "", "")
                                                     
             //Remove from Second Pass
             val secondPassQuery = s""" SELECT sbd.warehouse_id
                                             ,sbd.team_id
                                             ,sbd.po_number
                                             ,sbd.max_entry_date_timestamp
                                             ,sbd.back_common
                                             ,sbd.damage_qty
                                             ,sbd.recv_qty
                                             ,sbd.shippers
                                             ,sbd.type
                                             ,CASE WHEN rdpp.po_number IS NULL THEN 
                                                        sbd.ops_date
                                                   WHEN rdpp.status = '$ticket_pending' AND unix_timestamp(rdpp.ops_date) < unix_timestamp(sbd.ops_date) THEN
                                                        sbd.ops_date
                                                   ELSE rdpp.ops_date
                                               END ops_date
                                            ,unix_timestamp(rdpp.ops_date) 
                                            ,unix_timestamp(sbd.ops_date)
                                            ,rdpp.ops_date
                                            ,sbd.ops_date
                                            ,rdpp.status
                                            ,CASE WHEN unix_timestamp(rdpp.ops_date) < unix_timestamp(sbd.ops_date) THEN 1 ELSE 0 END verify_check
                                         FROM secondpass_base_data sbd
                                         LEFT OUTER JOIN rcv_dashboard_prev_po rdpp
                                           ON sbd.warehouse_id = rdpp.warehouse_id
                                            AND sbd.po_number = rdpp.po_number
                                   """
             logger.debug ( " - secondPassQuery - " +  secondPassQuery )
             val secondPassFinal = sqlContext.sql(secondPassQuery).persist()
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - secondPassFinal Count - " + secondPassFinal.count() )  
                 secondPassFinal.filter("po_number = '105557' ").show( secondPassFinal.count().toInt )
                 //.filter(s" ( po_number IN ($poNumber) ) ")
                 //($poNumber) IS NULL OR ( ( $poNumber ) IS NOT NULL AND 
             }
             secondPassFinal.registerTempTable("second_pass_final")                      
             sqlContext.cacheTable("second_pass_final")
             saveDataFrameM("file", secondPassFinal, "warehouse_id, team_id, po_number", "SecondPassPerfFinal", "", "", "")
            */
             //Remove from Second Pass
             // CASE for rdpp.status = '$ticket_pending' AND rdpp.ops_date < sbd.ops_date added on 12/06/2017             
             val secondPassQuery = s""" SELECT DISTINCT sbd.warehouse_id
                                             ,sbd.team_id
                                             ,sbd.po_number
                                             ,sbd.max_entry_date_timestamp
                                             ,sbd.back_common
                                             ,sbd.damage_qty
                                             ,sbd.recv_qty
                                             ,sbd.shippers
                                             ,sbd.type
                                             ,CASE WHEN rdpp.po_number IS NOT NULL THEN 
                                                        rdpp.ops_date
                                                   ELSE sbd.ops_date
                                               END ops_date
                                         FROM secondpass_base_data sbd
                                         LEFT OUTER JOIN rcv_dashboard_three_hr_po rdpp
                                           ON sbd.warehouse_id = rdpp.warehouse_id
                                            AND sbd.ops_date = rdpp.ops_date_processed
                                            AND sbd.po_number = rdpp.po_number
                                   """
             logger.debug ( " - secondPassQuery - " +  secondPassQuery )
             val secondPassFinal = sqlContext.sql(secondPassQuery).persist()
             if ( loggingEnabled == "Y" ) {
                 logger.debug( " - secondPassFinal Count - " + secondPassFinal.count() )  
                 secondPassFinal.filter(s" ( po_number IN ($poNumber) ) ").show( secondPassFinal.count().toInt )
                 //($poNumber) IS NULL OR ( ( $poNumber ) IS NOT NULL AND 
             }
             secondPassFinal.registerTempTable("second_pass_final")                      
             sqlContext.cacheTable("second_pass_final")
             saveDataFrameM("file", secondPassFinal, "warehouse_id, team_id, po_number", "SecondPassPerfFinal", "", "", "")           

         }
         catch{
             case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
               logger.error("Error while executing removePOsProcessedPrevDay at : " +  e.getMessage + " " + e.printStackTrace() )
               "1"
          }
        
         "0";
   }
    
  def checkLumperLinkData(sqlContext: SQLContext) = {
         logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                  checkLumperLinkData               " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )  
      val currentTime = getCurrentTime()                
      sqlContext.udf.register( "getLoadId", getLoadId _)                
      try{
             val matchRDSLumperLinkQuery = s""" WITH base_rows AS 
                                                 ( SELECT rsl.warehouse_id 
                                                         ,rsl.ops_date
                                                         ,getLoadId(rsl.load_id) load_id
                                                         ,rsl.load_id rds_load_id
                                                         ,rsl.start_time appt_time
                                                         ,rsl.vendor_name
                                                         ,rsl.po_number rds_po_number
                                                         ,lcd.team_id lumperlink_team
                                                         ,lcd.ticket_number lumperlink_ticket_number
                                                         ,lcd.totalloadstarttime from_time
                                                         ,lcd.ending_pallets lumperlink_ending_pallets
                                                         ,lcd.status lumperlink_status
                                                         ,lcd.damage_qty lumperlink_damage_qty
                                                         ,lcd.door_number
                                                         ,lcd.team_id team
                                                         ,lcd.po_number lumperlink_po_number
                                                     FROM rds_commons_loads rsl
                                                     JOIN lumperlink_complete_data lcd
                                                        ON rsl.warehouse_id = lcd.warehouse_id
                                                        AND  rsl.po_number = lcd.po_number
                                                ) 
                                            SELECT warehouse_id
                                                  ,ops_date
                                                  ,load_id
                                                  ,rds_load_id
                                                  ,appt_time
                                                  ,vendor_name
                                                  ,lumperlink_team
                                                  ,lumperlink_ticket_number
                                                  ,from_time since
                                                  ,round( unix_timestamp('$currentTime') - unix_timestamp(from_time) ) * 60 duration
                                                  ,door_number door_no
                                                  ,from_time
                                                  ,team
                                                  ,lumperlink_ending_pallets
                                                  ,lumperlink_status
                                                  ,rds_po_number
                                                  ,CASE WHEN ( lumperlink_po_number IS NOT NULL AND from_time IS NOT NULL ) THEN 
                                                             '1st Pass'
                                                        ELSE 'Pending'
                                                    END po_status
                                                  ,'$currentTime' upsert_timestamp
                                                  ,lumperlink_damage_qty
                                             FROM base_rows
                                        """
             logger.debug( " - commonsPlannedPOQuery - " + matchRDSLumperLinkQuery )
             val matchRDSLumperLink = sqlContext.sql(matchRDSLumperLinkQuery).persist()
             
             if ( loggingEnabled == "Y" ) {
               logger.debug( " - matchRDSLumperLink Count - " + matchRDSLumperLink.count() )  
               matchRDSLumperLink.show()
             }
             
             matchRDSLumperLink
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RDSLumperLinkData/" )  
        }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkLumperLinkData at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def checkWicsData(sqlContext: SQLContext) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    checkWicsData                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )     
      val currentTime = getCurrentTime()                
      sqlContext.udf.register( "getLoadId", getLoadId _)     
      try{
           val matchRDSWICSQuery = s""" WITH base_rows AS 
                                               ( SELECT rsl.warehouse_id 
                                                       ,rsl.ops_date
                                                       ,getLoadId(rsl.load_id) load_id
                                                       ,rsl.load_id rds_load_id
                                                       ,rsl.start_time appt_time
                                                       ,rsl.vendor_name
                                                       ,rsl.po_number rds_po_number
                                                       ,sbd.team_id wics_team
                                                       ,sbd.team_id team
                                                       ,sbd.po_number wics_po_number
                                                   FROM rds_commons_loads rsl
                                                   JOIN secondpass_base_data sbd
                                                      ON rsl.warehouse_id = sbd.warehouse_id
                                                      AND  rsl.po_number = sbd.po_number
                                              ) 
                                          SELECT warehouse_id
                                                ,ops_date
                                                ,load_id
                                                ,rds_load_id
                                                ,appt_time
                                                ,vendor_name
                                                ,wics_team
                                                ,team
                                                ,rds_po_number
                                                ,CASE WHEN wics_po_number IS NOT NULL THEN 
                                                           '2nd Pass'
                                                      ELSE 'Pending'
                                                  END po_status
                                                ,'$currentTime' upsert_timestamp
                                           FROM base_rows
                                      """
           logger.debug( " - matchRDSWICSQuery - " + matchRDSWICSQuery )
           val matchRDSWICS = sqlContext.sql(matchRDSWICSQuery).persist()
           
           if ( loggingEnabled == "Y" ) {
             logger.debug( " - matchRDSWICS Count - " + matchRDSWICS.count() )  
             matchRDSWICS.show()
           }        
             
           matchRDSWICS
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RDSWICSData/" ) 
               
        }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkWicsData at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
   def updateTimeLumperLink(sqlContext: SQLContext, dfLumperLink: DataFrame) = {
       val currentTime = getCurrentTime()
       dfLumperLink.withColumn("entrydatetimestamp", lit(currentTime))
             .write       
             .format("org.apache.spark.sql.cassandra")
             .options( Map("keyspace" -> "wip_rcv_ing"
                           ,"table" -> "lumperlink_data_by_warehouse"))
             .mode("append")
             .save()
   }
   
   def checkDashboard(sc: SparkContext, sqlContext: SQLContext ) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    checkDashboard                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )     
      val currentTime = getCurrentTime()               
      var existingRows = new ListBuffer[Row]()   
      sqlContext.udf.register( "getLoadId", getLoadId _)     
      try{
             // WHERE LENGTH( TRIM( rdbw.team ) ) > 0
             val rcvDashboardQuery = s""" SELECT rdbw.* 
                                           FROM rcv_dashboard_by_whs_ops_date rdbw
                                               ,warehouse_operation_time wot
                                          WHERE rdbw.warehouse_id = wot.facility
                                            AND rdbw.ops_date = wot.ops_date
                                        ORDER BY rdbw.warehouse_id, rdbw.ops_date, rdbw.load_id
                                     """
             logger.debug( " - rcvDashboardQuery - " + rcvDashboardQuery )
             val rcvDashboard = sqlContext.sql(rcvDashboardQuery).persist()
             
             if ( loggingEnabled == "Y" ) {
               logger.debug( " - rcvDashboard Count - " + rcvDashboard.count() )  
               rcvDashboard.show(100)
             }  
             
             rcvDashboard
              .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RcvDashboard/" ) 
               
            val rcvDashboardStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("load_id", StringType, nullable=false),
                                                        StructField("po_number", StringType, nullable=false),
                                                        StructField("po_status", StringType, nullable=true),
                                                        StructField("vendor_name", StringType, nullable=true),
                                                        StructField("appt_time", TimestampType, nullable=true),
                                                        StructField("rds_load_id", StringType, nullable=true),
                                                        StructField("team", StringType, nullable=true),
                                                        StructField("wics_team", StringType, nullable=true),
                                                        StructField("lumperlink_team", StringType, nullable=true),
                                                        StructField("lumperlink_ticket_number", StringType, nullable=true),
                                                        StructField("carry_forwarded", StringType, nullable=true),
                                                        StructField("status", StringType, nullable=true),
                                                        StructField("source_dataframe", StringType, nullable=true),
                                                        StructField("load_cpubackhaul_flag", StringType, nullable=true)
                                                       )
                                               )
             
            val rcvDashboardPOList = rcvDashboard
            .select("warehouse_id", "ops_date", "load_id", "po_number_list", "appt_time","rds_load_id"
                   ,"team", "wics_team", "lumperlink_team", "lumperlink_ticket_number", "carry_forwarded"
                   ,"status", "source_dataframe", "load_cpubackhaul_flag"
                   )
            .map { x => ( if ( x.get(3) == null ) null 
                        else x.getAs[Map[String, String]](3) )
                        .map { y => Row( //warehouse_id, ops_date, load_id
                                         x.getString(0), x.getTimestamp(1), x.getString(2)
                                         //po_number, po_status, vendor_name, appt_time, rds_load_id
                                        ,y._1, y._2.split(",").apply(0).drop(1), y._2.split(",").apply(1).dropRight(1), x.getTimestamp(4), x.getString(5)
                                        // team, wics_team, lumperlink_team
                                        ,x.getString(6), x.getString(7), x.getString(8)
                                        // lumperlink_ticket_number, carry_forwarded, status, source_dataframe, load_cpubackhaul_flag
                                        ,x.getString(9), x.getString(10), x.getString(11), x.getString(12) , x.getString(13)                                       
                                       ) 
                            }
                }
             
             logger.debug( " rcvDashboardPOList created " );
             //rcvDashboardPOList.foreach { println }
             
             /*rcvDashboardPOList.coalesce(1)
             .saveAsTextFile(filePath + "RcvDashboardPOList/")
             logger.debug( " rcvDashboardPOList saved " );
             */
             
             val rcvDashboardPOListFlatten = rcvDashboardPOList.toArray().flatten
             /*val rcvDashboardPOListRDD = rcvDashboardPOList.map { case List(x) => 
                                      Row( //warehouse_id, ops_date, load_id
                                            x.getString(0), x.getTimestamp(1), x.getString(2)
                                           //po_number, po_status, vendor_name, appt_time, rds_load_id
                                           ,x.getString(3), x.getString(4), x.getString(5), ( if( x.get(6) == null ) null else x.getTimestamp(6) ), x.getString(7)
                                           // team, wics_team, lumperlink_team
                                           ,x.getString(8), ( if( x.get(9) == null ) null else x.getString(9) ), ( if( x.get(10) == null ) null else x.getString(10) )
                                            // lumperlink_ticket_number
                                           ,( if( x.get(11) == null ) null else x.getString(11) )
                                          )
                               }*/
             
             /*for ( i <- 0 to ( rcvDashboardPOList.count().toInt - 1 ) )  {
                 val listValue = rcvDashboardPOList.collect().apply(i)
                 existingRows.++=(
                 getRowRcv( listValue )
                 )
             }
             
             logger.debug( " existingRows " );
              //combinedRow.foreach { println }
             existingRows.foreach { println }
            */
             
             //val rcvDashboardPOFinal = sqlContext.createDataFrame(sc.parallelize(existingRows.toList), rcvDashboardStruct)
             //val rcvDashboardPOFinal = sqlContext.createDataFrame(rcvDashboardPOListRDD, rcvDashboardStruct)
             val rcvDashboardPOFinal = sqlContext.createDataFrame(sc.parallelize(rcvDashboardPOListFlatten), rcvDashboardStruct)
             
             logger.debug( " - rcvDashboardPOFinal - " )     
             rcvDashboardPOFinal.show(40)
             
             rcvDashboardPOFinal.coalesce(1).write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             .mode("overwrite")
             .save(filePath + "RcvDashboardPOList/")
      }
     
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkDashboard at : " +  e.getMessage + " " + e.printStackTrace() )
     }
   }
   
   def checkUnplannedPO(sqlContext: SQLContext ) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    checkUnplannedPO                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )     
      val currentTime = getCurrentTime()                
      sqlContext.udf.register( "getLoadId", getLoadId _)     
      try{
             val unplannedPOQuery = s""" SELECT rdbw.* 
                                           FROM rcv_unplanned_po_by_warehouse rdbw 
                                               ,warehouse_operation_time wot
                                          WHERE rdbw.warehouse_id = wot.facility
                                            AND rdbw.ops_date = wot.ops_date
                                       ORDER BY rdbw.warehouse_id, rdbw.ops_date, rdbw.po_number
                                      """
             logger.debug( " - unplannedPOQuery - " + unplannedPOQuery )
             val unplannedPO = sqlContext.sql(unplannedPOQuery).persist()
             
             if ( loggingEnabled == "Y" ) {
               logger.debug( " - unplannedPO Count - " + unplannedPO.count() )  
               unplannedPO.show(100)
             }    

             unplannedPO
              .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RcvUnplannedPO/" )              
      }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkUnplannedPO at : " +  e.getMessage + " " + e.printStackTrace() )
     }
   }
   
   def checkRcvPerformance(sqlContext: SQLContext ) = {
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    checkRcvPerformance             " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )     
      val currentTime = getCurrentTime()                
      sqlContext.udf.register( "getLoadId", getLoadId _)     
      try{
             val rcvPerformanceQuery = s""" SELECT rdbw.* 
                                           FROM team_performance_by_warehouse rdbw 
                                               ,warehouse_operation_time wot
                                          WHERE rdbw.warehouse_id = wot.facility
                                            AND rdbw.ops_date = wot.ops_date
                                       ORDER BY rdbw.warehouse_id, rdbw.ops_date, rdbw.team_id
                                      """
             logger.debug( " - rcvPerformanceQuery - " + rcvPerformanceQuery )
             val rcvPerformance = sqlContext.sql(rcvPerformanceQuery).persist()
             
             if ( loggingEnabled == "Y" ) {
               logger.debug( " - rcvPerformance Count - " + rcvPerformance.count() )  
               rcvPerformance.show(100)
             }    
             
             rcvPerformance
              .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/RcvPerformance/" )                  
      }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing checkRcvPerformance at : " +  e.getMessage + " " + e.printStackTrace() )
     }
   }   
    
}