package com.cswg.testing

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.slf4j.LoggerFactory
import org.slf4j.MDC

import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import java.sql.Timestamp
import java.util.stream.Collectors._

import com.typesafe.config.ConfigFactory
import akka.dispatch.Foreach

class LiftTesting {
}

object LiftTesting {
  var logger = LoggerFactory.getLogger("Examples");
  //val logger = LoggerFactory.getLogger(this.getClass())
  MDC.put("fileName","M5999_WCLFTTesting")
  //var filePath = "/opt/cassandra/dse-5.0.5/dataFiles/LiftDetails/LiftBaseQueries/" 
  var filePath = "C:/opt/cassandra/default/dataFiles/LiftDetails/"
  var loggingEnabled = "N"
  var productivitySteamingTime = getCurrentTime()
  
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
   
  def getPreviousDate(numberOfDays: Int) : String = {
    val now = Calendar.getInstance()
    now.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd")     
    return sdf.format(now.getTime)
  }
  
  def getEnvironmentName(environment: String): (String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/environment.conf"))
      val conf = ConfigFactory.load(parsedFile)
      ///opt/cassandra/default/resources/spark/conf/
      ///opt/cassandra/default/FileMove/
      val oracleEnvName  = conf.getString("environment." + environment + ".oracleEnv")
      val cassandraEnvName  = conf.getString("environment." + environment + ".cassandraEnv")
      logger.debug("oracleEnvName - " + oracleEnvName + "\n" + "cassandraEnvName - " + cassandraEnvName)
      
      (oracleEnvName, cassandraEnvName)
    }
   
   def getCassandraConnectionProperties(environment: String): (String, String, String) = {
       val parsedFile = ConfigFactory.parseFile(new File("/opt/cassandra/default/Properties/cassandra_properties.conf"))
       val conf = ConfigFactory.load(parsedFile)
       ///opt/cassandra/default/resources/spark/conf/
       ///opt/cassandra/default/FileMove/
       val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
       
       val connectionHost = conf.getString("cassandra." + cassandraEnv + ".host")
       val userName = conf.getString("cassandra." + cassandraEnv + ".username")
       val password = conf.getString("cassandra." + cassandraEnv + ".password")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
       (connectionHost, userName, password)
   }
     
   // 0 -> File Id
   // 1 -> Job Id
   // 2 -> Environment Name
   // 3 -> Logging -> Y or N
  def main(args: Array[String]) {    
     logger.debug("Main for Lift Testing")
     
     val environment = if ( args(2) == null ) "dev" else args(2).toString 
     logger.info( " - environment - " + environment )
     val (hostName, userName, password) = getCassandraConnectionProperties(environment)
         
     val conf = new SparkConf()
      .setAppName("M5999_WCLFTTesting")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      //.set("spark.cassandra.output.ignoreNulls","true")
      //.set("spark.eventLog.enabled", "true" )
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]")  // This is required for IDE run
     
     val sc = new SparkContext(conf)
     val sqlContext: SQLContext = new SQLContext(sc)
     val hiveContext: SQLContext = new HiveContext(sc)
     val currentTime = getCurrentTime()
     val host = sc.getConf.get("spark.cassandra.connection.host")
    // val cluster = conf.get("cluster")
     //val currentDate = getCurrentDate()
     //val previousDate = getPreviousDate(-2)
     //val previousTime = getPreviousTime(-4)
     
     val fileId = if ( args(0) == null ) 1 else args(0).toInt 
     val jobId = if ( args(1) == null ) 4 else args(1).toString()  //"3" "1"
     loggingEnabled = if ( args(3) == null ) "N" else args(3).toString()
     
     import sqlContext.implicits._
     
     logger.debug( " - Conf Declared - ")      
     
     try{
         logger.debug( "\n" + " - Current Time - " + currentTime  + "\n"
                     + " - file Id - " + fileId + " - Job Id - " + jobId  + "\n"
                     + " - filePath - " + filePath  + "\n"
                     + " - loggingEnabled - " + loggingEnabled + "\n"
                     + " - host - " + host
                    // + " - cluster - "  + cluster
                    );
         
         /*val pmodTestingQuery = """SELECT PMOD( 3,2) """
         val pmodTesting = sqlContext.sql(pmodTestingQuery)
         pmodTesting.show */
         
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
         
         val dfWarehouseOperationTime = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_operation_time"
                                        ,"keyspace" -> "wip_shipping_ing" ) )
                          .load()
                          .filter(s"facility IN ( $warehouseId ) AND ( '$currentTime' BETWEEN start_time AND end_time ) ")   
                          //AND ( '$currentTime' BETWEEN start_time AND end_time )  Commented for Testing
                                 
         val dfLiftProductivity = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_productivity_data"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          //.filter(s"destination IN ( $warehouseId ) ")  
         
         //logger.debug("Productivity Warehouse")
         //dfLiftProductivity.select("destination").distinct().show()                
                          
         val dfLiftOperator = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_operator_data"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          .filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          
        /*val dfLiftProductivity1 = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_productivity_data_1"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          .filter(s"destination IN ( $warehouseId ) ")*/
                          
         val dfLiftWorkAssignment = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "warehouse_lift_work_transactions_data"
                                        ,"keyspace" -> "wip_lift_ing" ) )
                          .load()
                          .filter(s"destination IN ( $warehouseId ) ") 
                          
        /*val dfSelPerfUptime = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "wip_sel_perf_uptime_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")*/
                          
         val dfEmployeeDetails = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_details_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ") 
                          
         val dfEmployeeDetailsHistory = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_details_by_warehouse_history"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")                           
                          
         val dfUnPlannedEmployeeDetails = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "unplanned_employee_details_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")                           
                          
         val dfEmployeePunches = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_punches_by_warehouse"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")  
                          
         val dfEmployeeImages = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "employee_images_by_id"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")   

         val dfEmployeeAttendancePlan = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "attendance_plan_by_shift"
                                        ,"keyspace" -> "wip_warehouse_data" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ")   

         val dfLiftOperatorDetails = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "operator_details_by_warehouse"
                                        ,"keyspace" -> "wip_lift_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ") 
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          
         val dfLiftAisles = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "aisle_summary_by_warehouse"
                                        ,"keyspace" -> "wip_lift_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ") 
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                          
         val dfLiftAisleDetails = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "aisle_priority_details_by_warehouse"
                                        ,"keyspace" -> "wip_lift_prs" ) )
                          .load()
                          .filter(s"warehouse_id IN ( $warehouseId ) ") 
                          //.filter(s" SUBSTR( dest_id, LENGTH(dest_id) - 1 ) IN ( $warehouseId ) ") 
                                                    
         logger.debug( " - DataFrame Declared")
         
         dfWarehouseOperationTime.registerTempTable("warehouse_operation_time")
         dfLiftProductivity.registerTempTable("warehouse_lift_productivity_data")
         dfLiftOperator.registerTempTable("warehouse_lift_operator_data")
         dfLiftWorkAssignment.registerTempTable("warehouse_lift_work_transactions_data")
         //dfSelPerfUptime.registerTempTable("wip_sel_perf_uptime_by_warehouse")
         dfEmployeeDetails.registerTempTable("employee_details_by_warehouse")
         dfEmployeeDetailsHistory.registerTempTable("employee_details_by_warehouse_history")
         dfUnPlannedEmployeeDetails.registerTempTable("unplanned_employee_details_by_warehouse")
         dfEmployeePunches.registerTempTable("employee_punches_by_warehouse")
         dfEmployeeImages.registerTempTable("employee_images_by_id")
         dfEmployeeAttendancePlan.registerTempTable("attendance_plan_by_shift")
         dfLiftOperatorDetails.registerTempTable("operator_details_by_warehouse")
         dfLiftAisles.registerTempTable("aisle_summary_by_warehouse")
         dfLiftAisleDetails.registerTempTable("aisle_priority_details_by_warehouse")
         //dfLiftProductivity1.registerTempTable("warehouse_lift_productivity_data_1")
         logger.debug( " - Temp Tables Registered")
         
         val dfSparkStreaming = hiveContext.read 
                          .format("org.apache.spark.sql.cassandra")
                          .options( Map( "table" -> "spark_streaming_table_config"
                                        ,"keyspace" -> "wip_configurations" ) )
                          .load()
                          .filter(s" UPPER(source_system_name) = UPPER('mframe_prod_dbsvrt4p') AND UPPER(source_table_name) = 'WICS_PRODUCTIVITY' AND active = 'Y' ") 
         
         /* if ( !(dfSparkStreaming.rdd.isEmpty()) ){   //if Added on 07/11/2017
               productivitySteamingTime = getStringtoDate( dfSparkStreaming.select("last_run_timestamp").first().getString(0) )
          }
          logger.debug("productivitySteamingTime - "  +  productivitySteamingTime )
          */
          
         val warehouseOperationDateQuery = s""" SELECT wot.*
                                                     ,unix_timestamp('2017-12-13 22:05:23.0') - unix_timestamp('2017-12-13 06:53:28') diff 
                                                     ,unix_timestamp('2017-12-13 22:05:23.0') current_time
                                                     ,unix_timestamp('2017-12-13 06:53:28')  max_time 
                                                     ,CASE WHEN unix_timestamp('2017-12-13 22:05:23.0') - unix_timestamp('2017-12-13 06:53:28') <= 3600 THEN 1 ELSE 0 END test
                                                  FROM warehouse_operation_time wot
                                              """
         //WHERE '$currentTime' BETWEEN start_time AND end_time
         logger.debug( " - warehouseOperationDateQuery - " + warehouseOperationDateQuery )
         val warehouseOperationDate = hiveContext.sql(warehouseOperationDateQuery)
         warehouseOperationDate.show()   
         
         //hiveContext.sql( s""" SELECT from_unixtime(1504817259, 'dd/MM/yyyy HH:mm:ss') FROM warehouse_operation_time WHERE facility = '29' """).show()
         
         dfEmployeeDetailsHistory
               .filter(s" warehouse_id = '29' AND shift_date = '2017-12-16' AND shift_id = 'S1' AND employee_number = '102702' ")
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/EmployeeDetialsHistory_139232/" )
         /*val adhocQuery = s""" SELECT unix_timestamp('2017-12-13 22:05:23.0') - unix_timestamp('2017-12-13 06:53:28') diff 
                                     ,unix_timestamp('2017-12-13 22:05:23.0') current_time
                                     ,unix_timestamp('2017-12-13 06:53:28')  max_time
                                 FROM  warehouse_operation_time WHERE facility = '29' AND ops_date = '2017-12-14 00:00:00' """
         logger.debug( " - adhocQuery - "  + adhocQuery)
         val adhoc = hiveContext.sql(adhocQuery).show()      */
         //adhocQueries(hiveContext)
         //getSnapshot(hiveContext)
         //sqlContext.udf.register( "getAlternateAisles", getAlternateAisles _)
         //getLatestDates(hiveContext)
         //dateTimeConversion(hiveContext)
         //timeDiff(hiveContext)
         //getEmployeePunches(hiveContext, dfEmployeePunches) 
         //baseData(hiveContext)
         //queries(hiveContext)
         //liftProductivity(hiveContext)
         //getAlternateAisles("00000000000000")
         //copyData(hiveContext)
         //priorityCount(hiveContext)
         //splitString("warehouse_id, ops_date, employee_number")
         //mappingConv("Map(current_uips -> 0, total_priority_count -> 0, P1_count -> 0, problem_pallets_count -> 0, mia_count -> 0, delinquent_uips -> 0, P2_count -> 0, P3_count -> 0, P4_others_count -> 0, skippy_count -> 0) ")        
         /*logger.debug( " - Uncache Tables")
         if ( sqlContext.isCached("productivity_base_data") ){
           sqlContext.uncacheTable("productivity_base_data")
         }
         logger.debug( " - Uncached productivity_base_data")
                            
         if ( sqlContext.isCached("operator_base_data") ){
           sqlContext.uncacheTable("operator_base_data")
         }
         logger.debug( " - Uncached operator_base_data")
         */
         logger.debug( " - Program Completed")
     }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
     }
     
   }
  
  def timeDiff(sqlContext: SQLContext) = {
      val currentTime = getCurrentTime()   
      val timeDiffQuery = s""" WITH base_rows AS 
                                      (SELECT wot.facility warehouse_id
                                     ,wot.ops_date 
                                     ,wlpd.operid employee_number
                                     ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) entry_datetime
                                  FROM warehouse_lift_productivity_data wlpd
                                  JOIN warehouse_operation_time wot
                                       ON wot.facility = wlpd.destination
                                         AND from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) )
                                              BETWEEN wot.start_time AND wot.end_time
                                 WHERE wot.facility = '29'
                                   AND wot.ops_date = '2017-04-23'
                                   AND wlpd.operid = '100035'
                                   AND proc_type IN ( 'D', 'B', 'P', 'M', 'Z', 'Y', 'V', 'W' ) 
                                    )
                                SELECT warehouse_id
                                      ,br.ops_date
                                      ,br.employee_number
                                      ,( unix_timestamp( MAX( br.entry_datetime ) 
                                                     OVER ( PARTITION BY br.warehouse_id
                                                                        ,br.ops_date
                                                                        ,br.employee_number
                                                           ) ) -  unix_timestamp('$currentTime')  ) / 60 lapse_time
                                      ,MAX( br.entry_datetime ) 
                                                     OVER ( PARTITION BY br.warehouse_id
                                                                        ,br.ops_date
                                                                        ,br.employee_number
                                                           )  entry_datetime
                                      ,'$currentTime' current_time
                                 FROM base_rows br
                              """
       logger.debug( " - timeDiffQuery - " + timeDiffQuery )
        val timeDiff = sqlContext.sql(timeDiffQuery)
        
        //timeDiff.withColumn("time_lapse", datediff(timeDiff("current_time"), timeDiff("entry_datetime") ) )
        
        timeDiff.show()
        
        /*
         * ,datdiff( '$currentTime', MAX( br.entry_datetime ) 
                                                     OVER ( PARTITION BY br.warehouse_id
                                                                        ,br.ops_date
                                                                        ,br.employee_number
                                                           ) ) lapse_time
         */
    }

    def adhocQueries(sqlContext: SQLContext) = {
      val currentTime = getCurrentTime()   
      val previousTime = getPreviousDate(-1)
      val timeDiffQuery = s""" SELECT (unix_timestamp('$currentTime') - greatest( unix_timestamp('$currentTime'),  unix_timestamp(lift_latest_ops_time) ) ) / 60 lapse
                                      ,greatest( unix_timestamp('$currentTime'),  unix_timestamp(lift_latest_ops_time) ) greatest_value
                                 FROM employee_details_by_warehouse
                                WHERE warehouse_id = '29'
                                  AND shift_date = '2017-07-02'
                                  AND employee_number = '93878'
                              """
       
        
       val productivityQuery = s""" SELECT destination, operid, proc_type, aisle, '$productivitySteamingTime' productivityStreamingTime
                                           ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) entry_datetime 
                                      FROM warehouse_lift_productivity_data wlpd
                                    WHERE entry_date = '20170907'
                                      AND from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) >= '$productivitySteamingTime'
                                """
       
       val negCummulativeLapseQuery = s""" SELECT warehouse_id, shift_date, shift_id, employee_number, employee_name, employee_type, actual_status
                                        ,job_code, lift_latest_ops_time, lift_max_ops_time, lift_current_lapse, lift_cummulative_lapse, lift_lapse_percent, lift_operator_status
                                        ,lift_backout_count, lift_drop_count, lift_putaway_count, lift_move_count, lift_mia_count
                                        ,lift_mia_released_count, lift_skippy_count, lift_skippy_released_count  
                                    	  ,work_hours, actual_today_hours, scheduled_hours, start_time, end_time, plan_percent, actual_percent, actual_uptime_percent
                                    	  ,home_supervisor_name, champion, lapse_time, last_absent_date
                                    	  ,latest_punch, overtime_hours, plan_hours, projected_hours, planned_uptime, planned_status, attendance_status
                                    	  ,shift_name, weekly_cases_selected
                                    	  ,aba_hours, actual_hours , cases_selected  
                                    	  ,update_time 
                                   FROM employee_details_by_warehouse
                                  WHERE shift_date >= '$previousTime'
                                    AND ( lift_cummulative_lapse < 0 OR lift_lapse_percent < 0 ) 
                               ORDER BY warehouse_id, shift_date, employee_number
                             """                                        
       
       val employeeHistoryQuery = s""" SELECT warehouse_id, shift_date, shift_id, employee_number, employee_name, employee_type, actual_status
                                        ,job_code, lift_latest_ops_time, lift_max_ops_time, lift_current_lapse, lift_cummulative_lapse, lift_lapse_percent, lift_operator_status
                                        ,lift_backout_count, lift_drop_count, lift_putaway_count, lift_move_count, lift_mia_count
                                        ,lift_mia_released_count, lift_skippy_count, lift_skippy_released_count  
                                    	  ,work_hours, actual_today_hours, scheduled_hours, start_time, end_time, plan_percent, actual_percent, actual_uptime_percent
                                    	  ,home_supervisor_name, champion, lapse_time, last_absent_date
                                    	  ,latest_punch, overtime_hours, plan_hours, projected_hours, planned_uptime, planned_status, attendance_status
                                    	  ,shift_name, weekly_cases_selected
                                    	  ,aba_hours, actual_hours , cases_selected  
                                    	  ,update_time 
                                   FROM employee_details_by_warehouse 
                                  WHERE shift_date >= '$previousTime'
                               ORDER BY warehouse_id, shift_date, employee_number
                             """  
        //AND employee_number IN ('2912','5317','7105','18347','69156','100035')
                                    
       val  productivityHistoryQuery = s""" SELECT wot.facility warehouse_id
                                             ,apbs.shift_date
                                             ,apbs.shift_id
                                             ,wot.ops_date 
                                             ,cast(wlpd.operid as Int) employee_number
                                             ,apbs.employee_id employee_number_chk
                                             ,apbs.employee_name
                                             ,apbs.employee_type
                                             ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                             ,edbw.lift_cummulative_lapse
                                             ,apbs.work_hours
                                             ,TRIM(wlpd.proc_type) proc_type
                                             ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) entry_datetime
                                             ,wlpd.program
                                             ,wlpd.priority
                                             ,wlpd.seq
                                             ,wlpd.spec_id
                                             ,wlpd.term
                                             ,wlpd.wc_ext_time
                                             ,wlpd.action_flag
                                             ,SUM( CASE WHEN TRIM(proc_type) = 'O' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) sign_on_count
                                             ,SUM( CASE WHEN TRIM(proc_type) = 'F' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) sign_off_count
                                             ,SUM( CASE WHEN TRIM(proc_type) = 'W' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) abndn_cnt
                                             ,edbw.lift_current_lapse
                                             ,edbw.actual_status
                                             ,edbw.lift_lapse_percent
                                             ,edbw.lift_max_ops_time
                                             ,edbw.actual_today_hours
                                             ,edbw.scheduled_hours
                                         FROM warehouse_lift_productivity_data wlpd
                                           JOIN warehouse_operation_time wot
                                                ON wot.facility = wlpd.destination
                                                  AND from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) )
                                                       BETWEEN wot.start_time AND wot.end_time
                                           LEFT OUTER JOIN attendance_plan_by_shift apbs
                                                ON apbs.warehouse_id = wlpd.destination
                                                  AND apbs.shift_date = wot.ops_date
                                                  AND apbs.employee_id = cast(wlpd.operid as Int) 
                                           LEFT OUTER JOIN employee_details_by_warehouse edbw
                                                ON edbw.warehouse_id = wlpd.destination
                                                  AND edbw.shift_date = wot.ops_date
                                                  AND edbw.employee_number = cast(wlpd.operid as Int)  
                                   """
                                             
       logger.debug( " - negCummulativeLapseQuery - " + negCummulativeLapseQuery )
        val negCummulativeLapse = sqlContext.sql(negCummulativeLapseQuery )        
        negCummulativeLapse.show()
        
        negCummulativeLapse
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/LiftNegCummulativeLapse/" )  
               
       logger.debug( " - employeeHistoryQuery - " + employeeHistoryQuery )
        val employeeHistory = sqlContext.sql(employeeHistoryQuery )        
        employeeHistory.show()
        
        employeeHistory
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/LiftEmployeeHistoryData/" )       
               
      logger.debug( " - employeeHistory - " + productivityHistoryQuery )
        val productivityHistory = sqlContext.sql(productivityHistoryQuery)        
        //productivityHistory.show()
        
        productivityHistory
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/LiftProductivityHistoryData/" )                 
    }
  
  def splitString(orderByClause: String) = {
    val orderBySeq =  orderByClause.split(",").toSeq.map { x => "\"" + x.trim() + "\"" }.mkString(",").split(",").toSeq
                     
      logger.debug( "Order By Seq - " + orderBySeq )
  }
  
   def mappingConv(data: String) = {  //: Map[String,Long]
        val mappedString = data.substring(4, data.length() - 1).split(",").map{ x => x.split("->") }.flatMap { case Array(k,v) => Map(k -> v) }  //collect(Collectors.toMap( a-> a[0], a->a[1])
        mappedString.foreach { println }
    }
      
  def getEmployeePunches(sqlContext: SQLContext, dfEmployeePunches: DataFrame ) {
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "               getEmployeePunches                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val warehouseTesting = "29"
      val currentTime = getCurrentTime()
      try{
            val dfEmployeePunchesData = dfEmployeePunches
                                       .select( dfEmployeePunches("warehouse_id"),dfEmployeePunches("report_date"),dfEmployeePunches("employee_id"),dfEmployeePunches("punches") )
                                       .filter( "warehouse_id = '29' AND report_date = '2017-02-23' " ) 
                                       .map { x => Row( x.getString(0), x.getTimestamp(1), x.getString(2), x.getString(3).split(",").size  ) }
            
           dfEmployeePunchesData.foreach( println )
      }
     catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getEmployeePunches at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def dateTimeConversion(sqlContext: SQLContext) {
    logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "               dateTimeConversion                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val currentTime = getCurrentTime()
      val currentDate = "20170418"
      try{
                val latestProductivityDateQuery = s"""
                                                  SELECT DISTINCT destination
                                                        ,wlpd.entry_date
                                                        ,wlpd.entry_time
                                                        ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', substr( wlpd.entry_time, 0, 6) ), 'yyyyMMdd HHmmss' ) )  max_entry_datetime
                                                        ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', wlpd.entry_time ), 'yyyyMMdd HHmmss' ) )  max_entry_datetime_ms
                                                        ,substr( wlpd.entry_time, 0, 6) sub_entry_time
                                                        ,substr( wlpd.entry_time, 1, 6) sub_entry_time_2
                                                        ,from_unixtime( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) )  entry_date
                                                        ,CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) entry_time
                                                        ,unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' )  entry_time_wotstmp
                                                        ,TO_DATE( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) ) ) date_entry_datetime
                                                        ,TO_DATE( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) ) ) date_entry_datetime_1
                                                        ,concat( wlpd.entry_date, ' ', substr( wlpd.entry_time, 0, 6) ) concat_entrydatetime
                                                        ,concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) ) concat_entrydatetime_1
                                                        ,concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) ) concat_entrydatetime_2
                                                        ,unix_timestamp( concat( wlpd.entry_date, ' ', substr( wlpd.entry_time, 0, 6) ), 'yyyyMMdd HHmmss' ) unix_entrydatetime
                                                        ,unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) ) , 'yyyy-MM-dd HH:mm:ss' ) unix_entrydatetime_1
                                                        ,unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) ) ) unix_entrydatetime_2
                                                        ,unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) ) ) unix_entrydatetime_3
                                                        ,from_unixtime( unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) ) , 'yyyy-MM-dd HH:mm:ss' ) ) max_entry_datetime_1
                                                        ,from_unixtime( unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', CAST( unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' ) as Timestamp ) ) ) ) max_entry_datetime_2
                                                        ,from_unixtime( unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' )  ) ) ) max_entry_datetime_3
                                                        ,from_unixtime( unix_timestamp( concat( CAST( unix_timestamp( wlpd.entry_date, 'yyyyMMdd' ) as Timestamp ), ' ', unix_timestamp( substr( wlpd.entry_time, 0, 6), 'HHmmss' )  ) , 'yyyy-MM-dd HH:mm:ss' ) ) max_entry_datetime_4
                                                   FROM warehouse_lift_productivity_data wlpd
                                                  WHERE wlpd.entry_date = '$currentDate'
                                                    AND operid = '100035'
                                                ORDER BY destination, max_entry_datetime DESC
                                              """
           logger.debug( " - latestProductivityDateQuery - " + latestProductivityDateQuery )
           val latestProductivityDate = sqlContext.sql(latestProductivityDateQuery)
           latestProductivityDate.show() 
      }
      catch{
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
            logger.error("Error while executing dateTimeConversion at : " +  e.getMessage + " " + e.printStackTrace() )
       }
  }
  
  def getLatestDates(sqlContext: SQLContext){
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    getLatestDates                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val warehouseTesting = "29"
      val currentTime = getCurrentTime()
      //val currentDate = getCurrentDate()
      val currentDate = "20170417"
      try{
            /*val latestProductivityDateQuery = s""" WITH base_rows AS 
                                                      ( SELECT DISTINCT destination
                                                               ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', wlpd.entry_time ), 'yyyyMMdd HHmmss' ) )  max_entry_datetime
                                                           FROM warehouse_lift_productivity_data wlpd
                                                          WHERE wlpd.entry_date = '$currentDate'
                                                      )
                                                   SELECT destination
                                                         ,MAX( max_entry_datetime ) max_entry_datetime
                                                     FROM base_rows
                                                 GROUP BY destination
                                                    UNION
                                                    SELECT destination
                                                          ,MAX( from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', wlpd.entry_time ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                     FROM warehouse_lift_productivity_data wlpd
                                                 GROUP BY destination
                                                 ORDER BY destination, max_entry_datetime DESC
                                          """*/
            val latestProductivityDateQuery = s""" SELECT destination
                                                         ,MAX( from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                     FROM warehouse_lift_productivity_data wlpd
                                                    WHERE wlpd.entry_date = '$currentDate'
                                                 GROUP BY destination
                                                    UNION
                                                    SELECT destination
                                                          ,MAX( from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                     FROM warehouse_lift_productivity_data wlpd
                                                 GROUP BY destination
                                                 ORDER BY destination, max_entry_datetime DESC
                                          """                                                          
            /*val latestOperatorDateQuery = s""" WITH base_rows AS 
                                                      ( SELECT DISTINCT dest_id
                                                              ,ext_timestamp
                                                           FROM warehouse_lift_operator_data 
                                                          WHERE run_date = '$currentDate'
                                                      )
                                               SELECT dest_id
                                                     ,MAX( ext_timestamp ) max_ext_timestamp
                                                FROM base_rows
                                            GROUP BY dest_id
                                               UNION
                                              SELECT dest_id
                                                    ,MAX( ext_timestamp ) max_ext_timestamp
                                                FROM warehouse_lift_operator_data
                                            GROUP BY dest_id
                                            ORDER BY dest_id, max_ext_timestamp DESC
                                          """*/
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
           /* val latestWorkAssignmentsQuery = s"""WITH base_rows AS 
                                                      ( SELECT DISTINCT destination
                                                               ,from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', SUBSTR( wlwd.entry_time, 0 , 6) ), 'yyyyMMdd HHmmss' ) )  max_entry_datetime
                                                           FROM warehouse_lift_work_transactions_data wlwd
                                                          WHERE wlwd.entry_date = '$currentDate'
                                                      )
                                                 SELECT destination
                                                       ,MAX( max_entry_datetime ) max_entry_datetime
                                                   FROM base_rows
                                               GROUP BY destination
                                                  UNION
                                                   SELECT destination
                                                        ,MAX( from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', SUBSTR( wlwd.entry_time, 0 , 6) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                   FROM warehouse_lift_work_transactions_data wlwd
                                               GROUP BY destination
                                             ORDER BY destination, max_entry_datetime DESC
                                          """ 
                                          */
            val latestWorkAssignmentsQuery = s"""SELECT destination
                                                       ,MAX( from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', substr( wlwd.entry_time, 0, 6) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                   FROM warehouse_lift_work_transactions_data wlwd
                                                  WHERE wlwd.entry_date = '$currentDate'
                                               GROUP BY destination
                                                  UNION
                                                   SELECT destination
                                                        ,MAX( from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', substr( wlwd.entry_time, 0, 6) ), 'yyyyMMdd HHmmss' ) ) ) max_entry_datetime
                                                   FROM warehouse_lift_work_transactions_data wlwd
                                               GROUP BY destination
                                             ORDER BY destination, max_entry_datetime DESC
                                          """                                                 

           logger.debug( " - latestProductivityDateQuery - " + latestProductivityDateQuery )
           val latestProductivityDate = sqlContext.sql(latestProductivityDateQuery)
           latestProductivityDate.show()
           
           logger.debug( " - latestOperatorDateQuery - " + latestOperatorDateQuery )
           val latestOperatorDate = sqlContext.sql(latestOperatorDateQuery)
           latestOperatorDate.show()
           
           logger.debug( " - latestWorkAssignmentsQuery - " + latestWorkAssignmentsQuery )
           val latestWorkAssignments = sqlContext.sql(latestWorkAssignmentsQuery)
           latestWorkAssignments.show()
      }
      catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getLatestDates at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }
  
  def getSnapshot(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      getSnapshot                   " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{ 
         val getOperatorDetailsQuery = """ SELECT odw.* 
                                             FROM operator_details_by_warehouse odw
                                                 ,warehouse_operation_time wot
                                            WHERE odw.warehouse_id = wot.facility
                                              AND odw.ops_date = wot.ops_date
                                         ORDER BY odw.warehouse_id, odw.ops_date, odw.operator_id
                                       """
         
         val getEmployeeDetailsQuery = """ SELECT edw.warehouse_id
                                             ,wot.ops_date
                                             ,edw.shift_id
                                             ,edw.employee_number
                                             ,apbs.employee_id employee_number_chk
                                             ,edw.employee_name
                                             ,apbs.employee_name attendance_employee_name
                                             ,edw.employee_type
                                             ,apbs.employee_type attendance_employee_type
                                             ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                             ,edw.work_hours
                                             ,apbs.work_hours attendance_work_hours
                                             ,edw.actual_status
                                              ,edw.job_code, edw.lift_backout_count, edw.lift_drop_count, edw.lift_putaway_count, edw.lift_move_count, edw.lift_mia_count
                                              ,edw.lift_mia_released_count, edw.lift_skippy_count, edw.lift_skippy_released_count, edw.lift_lapse_percent  
                                              ,edw.lift_current_lapse, edw.lift_cummulative_lapse, edw.lift_latest_ops_time, edw.lift_operator_status
                                          	  , edw.plan_percent, edw.actual_percent, edw.actual_uptime_percent
                                          	  ,edw.home_supervisor_name, edw.champion, edw.lapse_time, edw.last_absent_date
                                          	  ,edw.latest_punch, edw.overtime_hours, edw.plan_hours, edw.projected_hours, edw.planned_uptime, edw.planned_status
                                          	  ,edw.shift_name, edw.weekly_cases_selected
                                          	  ,edw.aba_hours, edw.actual_hours, edw.active_loads, edw.closed_loads, edw.cases_selected, edw.pallets_per_hour
                                          	  ,edw.update_time
                                             FROM employee_details_by_warehouse edw
                                             JOIN warehouse_operation_time wot
                                               ON edw.warehouse_id = wot.facility
                                                 AND edw.shift_date = wot.ops_date
                                             LEFT OUTER JOIN attendance_plan_by_shift apbs
                                              ON apbs.warehouse_id = edw.warehouse_id
                                                  AND apbs.shift_date = edw.shift_date
                                                  AND apbs.employee_id = edw.employee_number
                                         ORDER BY edw.warehouse_id, wot.ops_date, edw.lift_lapse_percent DESC, edw.employee_name, edw.employee_number
                                       """
         
         logger.debug( " - Query - " + getEmployeeDetailsQuery );
         val getEmployeeDetails = sqlContext.sql(getEmployeeDetailsQuery)     
         logger.debug( " - Query fired " )   
         
         if ( loggingEnabled == "Y" ) {
               getEmployeeDetails.show()
               getEmployeeDetails
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "/LiftBaseQueries/LiftEmployeeData/" )
           }
         
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing getSnapshot at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }  
  
  def baseData(sqlContext: SQLContext){
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                        baseData                    " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val warehouseTesting = "29"
      val currentTime = getCurrentTime()
      try{
               val productivityBaseDataQuery = s""" SELECT wot.facility warehouse_id
                                             ,apbs.shift_date
                                             ,apbs.shift_id
                                             ,wot.ops_date 
                                             ,wlpd.operid employee_number
                                             ,apbs.employee_id employee_number_chk
                                             ,apbs.employee_name
                                             ,apbs.employee_type
                                             ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                             ,apbs.work_hours
                                             ,wlpd.proc_type
                                             ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', wlpd.entry_time ), 'yyyyMMdd HHmmss' ) ) entry_datetime
                                             ,wlpd.program
                                             ,wlpd.aisle
                                             ,wlpd.error_type
                                             ,wlpd.priority
                                             ,wlpd.seq
                                             ,wlpd.spec_id
                                             ,wlpd.term
                                             ,wlpd.wc_ext_time
                                             ,wlpd.action_flag
                                             ,wlpd.item_code
                                             ,wlpd.license_plate
                                             ,wlpd.pallet_qty
                                             ,wlpd.backout_qty
                                             ,wlpd.pick_address
                                             ,wlpd.resv_address
                                             ,wlpd.rpt_ext_date
                                             ,wlpd.rpt_ext_time
                                             ,eibi.employee_id
                                             ,concat( eibi.first_name, ' ', eibi.last_name ) employee_name_images
                                             ,eibi.date_hired
                                             ,eibi.emp_account_unit
                                             ,eibi.emp_class_desc
                                             ,eibi.dept_name
                                             ,eibi.badge_id
                                             ,eibi.location
                                             ,eibi.location_description
                                             ,eibi.warehouse_name
                                             ,eibi.position
                                             ,concat( eibi.supervisor_firstname, ' ', eibi.supervisor_lastname ) supervisor_name
                                             ,eibi.work_type
                                             ,edbw.lift_cummulative_lapse
                                             ,SUM( CASE WHEN proc_type = 'O' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) sign_on_count
                                             ,SUM( CASE WHEN proc_type = 'F' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) sign_off_count
                                             ,MAX( pbd.entry_datetime ) 
                                                        OVER ( PARTITION BY pbd.warehouse_id
                                                                          ,pbd.ops_date
                                                                          ,pbd.employee_number
                                                             ) latest_ops_time
                                         FROM warehouse_lift_productivity_data wlpd
                                           JOIN warehouse_operation_time wot
                                                ON wot.facility = wlpd.destination
                                                  AND from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) )
                                                       BETWEEN wot.start_time AND wot.end_time
                                           LEFT OUTER JOIN attendance_plan_by_shift apbs
                                                ON apbs.warehouse_id = wlpd.destination
                                                  AND apbs.shift_date = wot.ops_date
                                                  AND apbs.employee_id = wlpd.operid 
                                           LEFT OUTER JOIN employee_details_by_warehouse edbw
                                                ON edbw.warehouse_id = wlpd.destination
                                                  AND edbw.shift_date = wot.ops_date
                                                  AND edbw.employee_number = wlpd.operid 
                                           LEFT OUTER JOIN employee_images_by_id eibi
                                                ON eibi.warehouse_id = wlpd.destination
                                                  AND eibi.employee_id = wlpd.operid      
                                           ORDER BY warehouse_id, ops_date, employee_number  
                                   """
              //WHERE eibi.employee_id IS NULL 
           logger.debug( " - productivityBaseDataQuery - " + productivityBaseDataQuery )
           val productivityBaseData = sqlContext.sql(productivityBaseDataQuery)
           //productivityBaseData.show()
           
           productivityBaseData.registerTempTable("productivity_base_data")
           sqlContext.cacheTable("productivity_base_data")
           logger.debug( " - productivity_base_data table registered and cached - " )
           
           if ( loggingEnabled == "Y" ) {
               productivityBaseData.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "LiftProductivityData/" + currentTime ) 
           }
               
             val latestSnapshotIdQuery = s""" SELECT DISTINCT snapshot_id, ext_timestamp
                                                FROM warehouse_lift_operator_data wlod
                                            ORDER BY ext_timestamp DESC
                                          """
           /*
            *                                                 JOIN warehouse_operation_time wot
                                                  ON wot.facility = SUBSTR( dest_id, LENGTH(dest_id) - 1 )
                                                    AND wlod.ext_timestamp
                                                         BETWEEN wot.start_time AND wot.end_time
            */
           logger.debug( " - latestSnapshotIdQuery - " + latestSnapshotIdQuery )
           val latestSnapshotId = sqlContext.sql(latestSnapshotIdQuery)
           //latestSnapshotId.show()

           val latestSnapshotIdValue = latestSnapshotId.select("snapshot_id")
                                      .collect().apply(0).get(0)
           
           logger.debug( " - latestSnapshotIdValue - " + latestSnapshotIdValue )
           
           val operatorBaseDataQuery = s""" WITH base_data AS 
                                           ( SELECT DISTINCT wot.facility warehouse_id
                                                  ,apbs.shift_date
                                                  ,apbs.shift_id
                                                  ,wot.ops_date  
                                                  ,wot.start_time
                                                  ,wot.end_time
                                                  ,wlod.opertr employee_number 
                                                  ,apbs.employee_id employee_number_chk
                                                  ,NVL( wlod.name, apbs.employee_name ) employee_name
                                                  ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                                  ,wlod.sw_14 msg_lock
                                                  ,wlod.termnl terminal
                                                  ,wlod.opr_frm opr_from
                                                  ,wlod.opr_to
                                                  ,wlod.team_frm team_from
                                                  ,wlod.team_to
                                                  ,wlod.oper_xaisles
                                                  ,wlod.sw_14 is_msg_sent
                                                  ,wlod.sw_2 is_pts_allowed
                                                  ,wlod.sw_3 is_drp_allowed 
                                                  ,wlod.sw_4 is_ptp_allowed
                                                  ,wlod.sw_5 is_fp_allowed
                                                  ,wlod.sw_6 is_xfr_allowed
                                                  ,wlod.sw_17 active_aisle
                                                  ,SUM( CASE WHEN TRIM(termnl) IS NOT NULL AND TRIM(termnl) != '' THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) terminal_not_null
                                                  ,dense_rank() OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date
                                                                      ,wlod.opertr
                                                              ORDER BY wot.facility
                                                                      ,wot.ops_date
                                                                      ,wlod.opertr
                                                                      ,wlod.termnl DESC
                                                          ) rank_num
                                              FROM warehouse_lift_operator_data wlod
                                              JOIN warehouse_operation_time wot
                                                  ON wot.facility = SUBSTR( dest_id, LENGTH(dest_id) - 1 )
                                                    AND wlod.ext_timestamp
                                                         BETWEEN wot.start_time AND wot.end_time
                                                    AND wlod.snapshot_id = '$latestSnapshotIdValue'
                                              LEFT OUTER JOIN attendance_plan_by_shift apbs
                                                ON apbs.warehouse_id = SUBSTR( dest_id, LENGTH(dest_id) - 1 )
                                                  AND apbs.shift_date = wot.ops_date
                                                  AND apbs.employee_id = wlod.opertr
                                                )
                                           SELECT bd.warehouse_id
                                                 ,bd.shift_date
                                                 ,bd.shift_id
                                                 ,bd.ops_date 
                                                 ,bd.start_time
                                                 ,bd.end_time
                                                 ,bd.employee_number
                                                 ,bd.employee_name
                                                 ,bd.shift_start_time
                                                 ,bd.msg_lock
                                                 ,bd.terminal
                                                 ,bd.opr_from
                                                 ,bd.opr_to
                                                 ,bd.team_from
                                                 ,bd.team_to
                                                 ,bd.oper_xaisles
                                                 ,bd.is_msg_sent
                                                 ,bd.is_pts_allowed
                                                 ,bd.is_drp_allowed 
                                                 ,bd.is_ptp_allowed
                                                 ,bd.is_fp_allowed
                                                 ,bd.is_xfr_allowed
                                                 ,bd.active_aisle
                                                 ,bd.terminal_not_null
                                                 ,bd.employee_number_chk
                                             FROM base_data bd
                                            WHERE ( bd.terminal_not_null > 0 
                                                  OR ( bd.terminal_not_null = 0 AND rank_num = 1 )
                                                  ) 
                                          """
           /*WHERE  employee_id IS NULL
            *  ( bd.terminal_not_null > 0 
                                                  OR ( bd.terminal_not_null = 0 AND rank_num = 1 )
                                                  ) 
                                              AND
                                              * 
                                              */
           logger.debug( " - operatorBaseDataQuery - " + operatorBaseDataQuery )
           val operatorBaseData = sqlContext.sql(operatorBaseDataQuery)
           //operatorBaseData.show()
           
           val attendancePlanByShiftQuery = s""" SELECT wot.facility warehouse_id
                                                     ,apbs.shift_date
                                                     ,apbs.shift_id
                                                     ,wot.ops_date 
                                                     ,wot.start_time
                                                     ,wot.end_time
                                                     ,apbs.employee_id employee_number
                                                     ,apbs.employee_name
                                                     ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                                     ,wlod.sw_14 msg_lock
                                                     ,wlod.termnl terminal
                                                     ,wlod.opr_frm opr_from
                                                     ,wlod.opr_to
                                                     ,wlod.team_frm team_from
                                                     ,wlod.team_to
                                                     ,wlod.oper_xaisles
                                                     ,wlod.sw_14 is_msg_sent
                                                     ,wlod.sw_2 is_pts_allowed
                                                     ,wlod.sw_3 is_drp_allowed 
                                                     ,wlod.sw_4 is_ptp_allowed
                                                     ,wlod.sw_5 is_fp_allowed
                                                     ,wlod.sw_6 is_xfr_allowed
                                                     ,wlod.sw_17 active_aisle
                                                     ,SUM( CASE WHEN TRIM(termnl) IS NOT NULL AND TRIM(termnl) != '' THEN 1 ELSE 0 END ) OVER 
                                                          ( PARTITION BY wot.facility
                                                                        ,wot.ops_date 
                                                                        ,wlod.opertr
                                                          ) terminal_not_null
                                                     ,wlod.opertr  employee_number_chk
                                               FROM attendance_plan_by_shift apbs
                                               JOIN warehouse_operation_time wot
                                                      ON wot.facility = apbs.warehouse_id
                                                        AND apbs.shift_date = wot.ops_date
                                               LEFT OUTER JOIN warehouse_lift_operator_data wlod
                                                     ON apbs.warehouse_id = SUBSTR( wlod.dest_id, LENGTH(wlod.dest_id) - 1 )
                                                        AND apbs.shift_date = wot.ops_date
                                                        AND apbs.employee_id = wlod.opertr
                                              WHERE wlod.opertr IS NULL 
                                       """
           logger.debug( " - attendancePlanByShiftQuery - " + attendancePlanByShiftQuery )
           val attendancePlanByShift = sqlContext.sql(attendancePlanByShiftQuery)
                      
           val activeOperatorsData = operatorBaseData.filter("terminal_not_null > 0 ")
           
           val attendancePlanByShiftWOOperator = 
                            attendancePlanByShift
                           .filter("employee_number_chk IS NULL")
           
           val operatorFinalData = operatorBaseData.unionAll(attendancePlanByShiftWOOperator)
           val activeOperatorsFinalData = activeOperatorsData.unionAll(attendancePlanByShiftWOOperator)
           
           //operatorFinalData.registerTempTable("operator_base_data")
           //sqlContext.cacheTable("operator_base_data")
           //logger.debug( " - operator_base_data table registered and cached - " )
                      
           //activeOperatorsFinalData.registerTempTable("active_operator_base_data")
           //sqlContext.cacheTable("active_operator_base_data")
           //logger.debug( " - active_operator_base_data table registered and cached - " )
           
           if ( loggingEnabled == "Y" ) {
               operatorFinalData.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "LiftOperatorData/" + currentTime )
           }
             
           if ( loggingEnabled == "Y" ) {
               activeOperatorsFinalData
               .coalesce(1)
               .orderBy("warehouse_id", "ops_date", "employee_number" )           
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "LiftActiveOperatorData/" + currentTime ) 
           }
           
          val liftWorkTransactionsQuery = s""" SELECT wot.facility warehouse_id
                                                     ,wot.ops_date 
                                                     ,wlwt.work_type 
                                                     ,wlwt.entry_date
                                                     ,wlwt.entry_time
                                                     ,wlwt.aisle
                                                     ,wlwt.bar_code_lpn
                                                     ,wlwt.description
                                                     ,wlwt.item_code
                                                     ,wlwt.action_flag
                                                     ,wlwt.status_flag
                                                     ,wlwt.priority
                                                     ,wlwt.assignment_number
                                                     ,wlwt.completed_date
                                                     ,wlwt.completed_time
                                                     ,wlwt.completed_operator
                                                     ,wlwt.last_updated_date
                                                     ,wlwt.last_updated_time
                                                     ,wlwt.last_updated_operator
                                                     ,NVL( wlwt.completed_operator, wlwt.last_updated_operator ) operator_id
                                                     ,wlwt.pallet_last_updated_date
                                                     ,wlwt.pallet_quantity
                                                     ,wlwt.pick_slot
                                                     ,wlwt.reserve_slot
                                                     ,wlwt.po_number
                                                     ,wlwt.po_seq
                                                     ,wlwt.receive_date
                                                     ,wlwt.requestor
                                                     ,wlwt.seq
                                                     ,wlwt.wc_ext_date
                                                     ,wlwt.wc_ext_time
                                                     ,wlwt.wc_ext_flag
                                                     ,wlwt.invoice_site
                                                     ,wlwt.print_site
                                                     ,wlwt.no_more_pallets_alert
                                                     ,dense_rank() OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date
                                                                      ,wlwt.work_type 
                                                                      ,wlwt.aisle
                                                                      ,wlwt.bar_code_lpn
                                                              ORDER BY wot.facility
                                                                      ,wot.ops_date
                                                                      ,wlwt.work_type 
                                                                      ,wlwt.aisle
                                                                      ,wlwt.bar_code_lpn
                                                                      ,wlwt.entry_date DESC
                                                                      ,wlwt.entry_time DESC
                                                          ) rank_num
                                                 FROM warehouse_lift_work_transactions_data wlwt
                                                   JOIN warehouse_operation_time wot
                                                        ON wot.facility = wlwt.destination
                                                          AND from_unixtime( unix_timestamp( concat( wlwt.entry_date, ' ', SUBSTR( wlwt.entry_time, 0 , 6)  ), 'yyyyMMdd HHmmss' ) )
                                                               BETWEEN wot.start_time AND wot.end_time  
                                              ORDER BY warehouse_id, ops_date, work_type, aisle, bar_code_lpn, rank_num                   
                                   """
           logger.debug( " - liftWorkTransactionsQuery - " + liftWorkTransactionsQuery )
           val liftWorkTransactions = sqlContext.sql(liftWorkTransactionsQuery)
           //liftWorkTransactions.show()
           
           if ( loggingEnabled == "Y" ) {
               liftWorkTransactions.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "LiftWorkAssignments/" + currentTime)
           }
           
      }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing baseData at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }  
  
  def liftProductivity(sqlContext: SQLContext ) {
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    liftProductivity                " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val warehouseTesting = "'29'"
      val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
      val endTime =  getCurrentDate      
      val currentTime = getCurrentTime()
      sqlContext.udf.register( "getAlternateAisles", getAlternateAisles _)
      try{
          val productivityBaseDataQuery = s""" SELECT wot.facility warehouse_id
                                             ,apbs.shift_date
                                             ,apbs.shift_id
                                             ,wot.ops_date 
                                             ,cast(wlpd.operid as Int) employee_number
                                             ,apbs.employee_id employee_number_chk
                                             ,apbs.employee_name
                                             ,apbs.employee_type
                                             ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                             ,edbw.lift_cummulative_lapse
                                             ,apbs.work_hours
                                             ,TRIM(wlpd.proc_type) proc_type
                                             ,from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) entry_datetime
                                             ,wlpd.program
                                             ,wlpd.priority
                                             ,wlpd.seq
                                             ,wlpd.spec_id
                                             ,wlpd.term
                                             ,wlpd.wc_ext_time
                                             ,wlpd.action_flag
                                             ,SUM( CASE WHEN TRIM(proc_type) = 'O' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) sign_on_count
                                             ,SUM( CASE WHEN proc_type = 'F' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) sign_off_count
                                             ,SUM( CASE WHEN proc_type = 'W' THEN 1 ELSE 0 END  )  OVER
                                                             ( PARTITION BY wot.facility
                                                                           ,wot.ops_date      
                                                                           ,wlpd.operid  
                                                             ) abndn_cnt
                                             ,MAX( from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) ) 
                                                        OVER ( PARTITION BY wot.facility 
                                                                          ,wot.ops_date 
                                                                          ,cast(wlpd.operid as Int)
                                                             ) latest_ops_time
                                         FROM warehouse_lift_productivity_data wlpd
                                           JOIN warehouse_operation_time wot
                                                ON wot.facility = wlpd.destination
                                                  AND from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) )
                                                       BETWEEN wot.start_time AND wot.end_time
                                           LEFT OUTER JOIN attendance_plan_by_shift apbs
                                                ON apbs.warehouse_id = wlpd.destination
                                                  AND apbs.shift_date = wot.ops_date
                                                  AND apbs.employee_id = cast(wlpd.operid as Int)
                                           LEFT OUTER JOIN employee_details_by_warehouse edbw
                                                ON edbw.warehouse_id = wlpd.destination
                                                  AND edbw.shift_date = wot.ops_date
                                                  AND edbw.employee_number = cast(wlpd.operid as Int) 
                                          WHERE wot.facility = $warehouseTesting
                                            AND apbs.shift_date >= '$startTime'
                                            AND apbs.shift_date <= '$endTime'                        
                                      """ 
           logger.debug( " - liftWorkTransactionsQuery - " + productivityBaseDataQuery )
           val productivityBaseData = sqlContext.sql(productivityBaseDataQuery)
           //liftWorkTransactions.show()
           
           if ( loggingEnabled == "Y" ) {
               productivityBaseData.coalesce(1).write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + "LiftProductivityBaseData/")
           }
          
      }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing liftProductivity at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }  
  
  def queries(sqlContext: SQLContext ) {
      logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      queries                       " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      val warehouseTesting = "'29'"
      val startTime = addDaystoCurrentDatePv(-1)  //getCurrentDate
      val endTime =  getCurrentDate      
      val currentTime = getCurrentTime()
      sqlContext.udf.register( "getAlternateAisles", getAlternateAisles _)
      try{
          val operXaisles = "00000000000000"
          val warehouseLiftOperatorQuery = s""" SELECT SUBSTR( wlod.dest_id, LENGTH(wlod.dest_id) - 1 ) warehouse_id 
                                                  ,wlod.dest_id
                                                  ,wot.facility
                                                  ,wot.ops_date
                                                  ,wlod.opertr employee_number 
                                                  ,wlod.team
                                                  ,wlod.name
                                                  ,wlod.abndn_cnt
                                                  ,wlod.termnl terminal
                                                  ,wlod.opr_frm opr_from
                                                  ,wlod.opr_to
                                                  ,wlod.team_frm team_from
                                                  ,wlod.team_to
                                                  ,wlod.oper_xaisles
                                                  ,wlod.sw_14 is_msg_sent
                                                  ,wlod.sw_2 is_pts_allowed
                                                  ,wlod.sw_3 is_drp_allowed 
                                                  ,wlod.sw_4 is_ptp_allowed
                                                  ,wlod.sw_5 is_fp_allowed
                                                  ,wlod.sw_6 is_xfr_allowed
                                                  ,wlod.sw_17 active_aisle
                                                  ,wlod.sw_14 msg_lock
                                                  ,wlod.ext_timestamp
                                                  ,wlod.snapshot_id
                                                  ,LENGTH(termnl) terminal_length
                                                  ,LENGTH(sw_14) msg_lock_length
                                                  ,SUM( CASE WHEN TRIM(termnl) IS NULL OR TRIM(termnl) = '' THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) terminal_null
                                                  ,SUM( CASE WHEN TRIM(termnl) IS NOT NULL AND TRIM(termnl) != '' THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) terminal_not_null
                                                  ,SUM( CASE WHEN TRIM(sw_14) IS NULL OR TRIM(sw_14) = '' THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) msg_lock_null
                                                  ,SUM( CASE WHEN TRIM(sw_14) IS NOT NULL AND TRIM(sw_14) != '' THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) msg_lock_not_null
                                             FROM warehouse_lift_operator_data wlod
                                             JOIN warehouse_operation_time wot
                                                  ON wot.facility = SUBSTR( dest_id, LENGTH(dest_id) - 1 )
                                                    AND wlod.ext_timestamp
                                                         BETWEEN wot.start_time AND wot.end_time
                                            where wlod.dest_id = '0076'
                                              AND wlod.opertr = '0795'
                                       """
          val getAlternateAislesQuery = s""" SELECT getAlternateAisles('$operXaisles')"""
          
          val distinctWhseProductivityQuery = """ SELECT DISTINCT destination FROM productivity_base_data"""
          
          val operatorDetailsQuery = """ SELECT * 
                                           FROM warehouse_lift_operator_data wlod
                                          WHERE wlod.dest_id = '76'
                                            AND opertr = '1471'
                                          """
          val operatorBaseDataQuery = """SELECT DISTINCT wot.facility warehouse_id
                                                  ,apbs.shift_date
                                                  ,apbs.shift_id
                                                  ,wot.ops_date  
                                                  ,wot.start_time
                                                  ,wot.end_time
                                                  ,wlod.opertr employee_number 
                                                  ,apbs.employee_id employee_number_chk
                                                  ,wlod.name employee_name
                                                  ,TO_DATE( apbs.shift_start_time ) shift_start_time
                                                  ,wlod.sw_14 msg_lock
                                                  ,wlod.termnl terminal
                                                  ,SUM( CASE WHEN termnl IS NULL THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) terminal_null
                                                  ,SUM( CASE WHEN termnl IS NOT NULL THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) terminal_not_null
                                                  ,SUM( CASE WHEN sw_14 IS NULL THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) msg_lock_null
                                                  ,SUM( CASE WHEN sw_14 IS NOT NULL THEN 1 ELSE 0 END ) OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date 
                                                                      ,wlod.opertr
                                                        ) msg_lock_not_null
                                                  ,wlod.opr_frm opr_from
                                                  ,wlod.opr_to
                                                  ,wlod.team_frm team_from
                                                  ,wlod.team_to
                                                  ,wlod.oper_xaisles
                                                  ,wlod.sw_14 is_msg_sent
                                                  ,wlod.sw_2 is_pts_allowed
                                                  ,wlod.sw_3 is_drp_allowed 
                                                  ,wlod.sw_4 is_ptp_allowed
                                                  ,wlod.sw_5 is_fp_allowed
                                                  ,wlod.sw_6 is_xfr_allowed
                                                  ,wlod.sw_17 active_aisle
                                                  ,dense_rank() OVER 
                                                        ( PARTITION BY wot.facility
                                                                      ,wot.ops_date
                                                                      ,wlod.opertr
                                                              ORDER BY wot.facility
                                                                      ,wot.ops_date
                                                                      ,wlod.opertr
                                                                      ,wlod.termnl DESC
                                                          ) rank_num
                                              FROM warehouse_lift_operator_data wlod
                                              JOIN warehouse_operation_time wot
                                                  ON wot.facility = SUBSTR( dest_id, LENGTH(dest_id) - 1 )
                                                    AND wlod.ext_timestamp
                                                         BETWEEN wot.start_time AND wot.end_time
                                              LEFT OUTER JOIN attendance_plan_by_shift apbs
                                                ON apbs.warehouse_id = SUBSTR( dest_id, LENGTH(dest_id) - 1 )
                                                  AND apbs.shift_date = wot.ops_date
                                                  AND apbs.employee_id = wlod.opertr"""
          
          //WHERE SUBSTR( wlod.dest_id, LENGTH(wlod.dest_id) - 1 )  IN ( '29','44','76' )
          
          
          val distinctProcTypeQuery = s""" SELECT DISTINCT wot.facility warehouse_id, wot.ops_date, proc_type 
                                                  , from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) ) entry_datetime
                                        FROM warehouse_lift_productivity_data wlpd
                                           JOIN warehouse_operation_time wot
                                                ON wot.facility = wlpd.destination
                                                  AND from_unixtime( unix_timestamp( concat( wlpd.entry_date, ' ', SUBSTR( wlpd.entry_time, 0, 6 ) ), 'yyyyMMdd HHmmss' ) )
                                                       BETWEEN wot.start_time AND wot.end_time
                                          WHERE proc_type IN ( 'Z', 'Y', '2', '3' ) 
                                        """
          val distinctWorkTypeQuery = s""" SELECT DISTINCT destination, entry_date, work_type 
                                             FROM warehouse_lift_work_transactions_data
                                           ORDER BY destination, entry_date DESC, work_type"""
          
          val employeeDetailsQuery = s"""SELECT warehouse_id, shift_date, shift_id, employee_number, employee_name, employee_type, actual_status  -- A -> Active, W -> Away, I -> Idle
                                                ,lift_backout_count, lift_drop_count, lift_putaway_count, lift_move_count, lift_mia_count
                                                ,lift_mia_released_count, lift_skippy_count, lift_skippy_released_count, lift_lapse_percent  
                                                ,lift_current_lapse, lift_cummulative_lapse, lift_latest_ops_time, lift_operator_status
                                          	  ,work_hours, plan_percent, actual_percent, actual_uptime_percent
                                          	  ,home_supervisor_name, job_code, champion, lapse_time, last_absent_date
                                          	  ,latest_punch, overtime_hours, plan_hours, projected_hours, planned_uptime, planned_status
                                          	  ,recent_punches, shift_name, weekly_cases_selected
                                          	  ,aba_hours, actual_hours, active_loads, closed_loads, cases_selected, pallets_per_hour
                                          	  ,update_time
                                            FROM employee_details_by_warehouse
                                          WHERE warehouse_id = $warehouseTesting
                                            AND shift_date >= '$startTime'
                                            AND shift_date <= '$endTime'                                          
                                      """
          
                                            //AND lift_drop_count IS NOT NULL
          val unplannedEmployeeDetailsQuery = s"""SELECT warehouse_id, shift_date, shift_id, employee_number, employee_name, employee_type, actual_status  -- A -> Active, W -> Away, I -> Idle
                                                        ,lift_backout_count, lift_drop_count, lift_putaway_count, lift_move_count, lift_mia_count
                                                        ,lift_mia_released_count, lift_skippy_count, lift_skippy_released_count, lift_lapse_percent  
                                                        ,lift_current_lapse, lift_cummulative_lapse, lift_latest_ops_time, lift_operator_status
                                                        ,attendance_status, planned_status, dob_day_month, hired_date, home_supervisor_name, home_supervisor_id
                                                        ,cell_number, latest_punch, recent_punches 
                                                  	  ,work_hours, plan_percent, actual_percent, actual_uptime_percent
                                                  	  ,job_code, champion, lapse_time, last_absent_date
                                                  	  ,overtime_hours, projected_hours, planned_uptime
                                                  	  ,shift_name, actual_hours, cases_selected 
                                                  	  ,update_time
                                                   FROM unplanned_employee_details_by_warehouse
                                                   WHERE warehouse_id = $warehouseTesting
                                                     AND shift_date >= '$startTime'
                                                     AND shift_date <= '$endTime'                                                  
                                               """
          
          logger.debug( " - query - " + employeeDetailsQuery )
          val employeeDetails = sqlContext.sql(employeeDetailsQuery)
          //distinctProcType.show(40)
          
          employeeDetails.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "EmployeeDetails/") 
         
          logger.debug( " - query - " + unplannedEmployeeDetailsQuery )
          val unplannedEmployeeDetails = sqlContext.sql(unplannedEmployeeDetailsQuery)
          //distinctProcType.show(40)
          
          unplannedEmployeeDetails.coalesce(1).write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
         .mode("overwrite")
         .save(filePath + "UnPlannedEmployeeDetails/")          
         
       }catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing queries at : " +  e.getMessage + " " + e.printStackTrace() )
     }
  }  
  
  def getAlternateAisles(alternateAisles: String): List[String] ={
     //val alternateAislesList = alternateAisles.List
     val alternateAislesList = alternateAisles.map { x => for ( index <- 0 to alternateAisles.length() - 2 by 2)
                                                          yield (
                                                                alternateAisles.substring(index, index+2)
                                                                )  
                                                    }.toList.apply(0).toList
     
     logger.debug( "alternateAislesList - " + alternateAislesList)
     //alternateAislesList.foreach(println)
     
     /*var alternateAislesList1 = "";
     var append = ""
     val alternateAisleslength = alternateAisles.length()
     for ( index <- 0 until alternateAisleslength - 2 by 2) yield {
        val alternateAislesData = alternateAisles.substring(index, index+2)
        if ( alternateAislesList1.equals("") ) {
           append = ""
        }else{
           append = ","
        }
        alternateAislesList1 = alternateAislesList1 + append + alternateAislesData
     }
     logger.debug( "alternateAislesList1 - " + alternateAislesList1)
     * 
     */
     alternateAislesList
  }
  
  def copyData(sqlContext: SQLContext) = {
    logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      copyData                      " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{ 
         val copyData1 = """ SELECT * FROM warehouse_lift_productivity_data_1"""
         val copyData2 = """ SELECT * FROM load_percent_by_warehouse WHERE update_status = 'Y'"""
         
         logger.debug( " - Query - " + copyData1 );
         val queryData = sqlContext.sql(copyData1)     
         logger.debug( " - Query fired " ) 
         queryData.show(40)
         
         queryData.write
         .format("org.apache.spark.sql.cassandra")
         .option("keyspace", "wip_lift_ing")
         .option("table", "warehouse_lift_productivity_data")
         .mode("append")
         .save()
         
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing copyData at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }  
  
  def priorityCount(sqlContext: SQLContext) {
      logger.debug( "\n"+ 
                  "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                      priorityCount                 " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
             
    val currentTime = getCurrentTime();
    try{
           val priorityCountQuery = s""" SELECT DISTINCT wlwd.destination warehouse_id
                                       ,wot.ops_date
                                       ,wlwd.completed_operator employee_number
                                       ,SUM( CASE WHEN status_flag = 'O' AND priority = 1 THEN 1 ELSE 0 END )
                                                           OVER ( PARTITION BY wlwd.destination 
                                                                              ,wot.ops_date
                                                                              ,wlwd.completed_operator ) p1_count
                                       ,SUM( CASE WHEN status_flag = 'O' AND priority = 2 THEN 1 ELSE 0 END )
                                                           OVER ( PARTITION BY wlwd.destination 
                                                                              ,wot.ops_date
                                                                              ,wlwd.completed_operator ) p2_count
                                       ,SUM( CASE WHEN status_flag = 'O' AND priority = 3 THEN 1 ELSE 0 END )
                                                           OVER ( PARTITION BY wlwd.destination 
                                                                              ,wot.ops_date
                                                                              ,wlwd.completed_operator ) p3_count
                                       ,SUM( CASE WHEN status_flag = 'O' AND priority >= 4 THEN 1 ELSE 0 END )
                                                           OVER ( PARTITION BY wlwd.destination 
                                                                              ,wot.ops_date
                                                                              ,wlwd.completed_operator ) p4_count
                                   FROM warehouse_lift_work_transactions_data wlwd
                                   JOIN warehouse_operation_time wot
                                      ON wot.facility = wlwd.destination
                                        AND from_unixtime( unix_timestamp( concat( wlwd.entry_date, ' ', SUBSTR( wlwd.entry_time, 0 , 6)  ), 'yyyyMMdd HHmmss' ) )
                                             BETWEEN wot.start_time AND wot.end_time
                                   WHERE wlwd.completed_operator IS NOT NULL
                             """
             logger.debug( " - priorityCountQuery - " + priorityCountQuery )
             val priorityCount = sqlContext.sql(priorityCountQuery)
             priorityCount.show()                  

             val priorityCountMap = priorityCount.map { x => Row( x.getString(0), x.getTimestamp(1), x.getString(2) 
                                         , Map( "P1" -> x.getLong(3), "P2" -> x.getLong(4), "P3" -> x.getLong(5), "P4" -> x.getLong(6) )
                                         ) }
             
             logger.debug( " - priorityCountMap - "  )
             //priorityCountMap.foreach { println }
             
             val priorityCountStruct = new StructType( Array( StructField("warehouse_id", StringType, nullable=false), 
                                                        StructField("ops_date", TimestampType, nullable=false),
                                                        StructField("operator_id", StringType, nullable=false),
                                                        StructField("priority_count", MapType(StringType, LongType), nullable=true)
                                                       )
                                               )
              
             val priorityCountDF = sqlContext.createDataFrame(priorityCountMap, priorityCountStruct )
             logger.debug( " - priorityCountDF - "  )
             priorityCountDF.foreach { println }
      }
      catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing priorityCount at : " +  e.getMessage + " "  +  e.printStackTrace() )
     }
  }
}