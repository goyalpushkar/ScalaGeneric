package com.cswg.testing

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.{LoggerFactory,Logger,MDC}

import java.io.FileNotFoundException
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType,StructField,IntegerType,StringType,DoubleType,LongType,FloatType,ByteType,TimestampType,BooleanType,DataType,ArrayType}
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.storage.StorageLevel

class AssignmentDetailsTesting {
  
}

object AssignmentDetailsTesting {
  var logger = LoggerFactory.getLogger("Examples");
  var currentExtTime: String = getCurrentTime.toString()  // Moved on 05/01/2017 
  var previousOpsDay: String = null //Added on 05/31/2017
  var cffPath = ""   // Declaration is moved here on 05/17/2017
  var skipFileProcessing = "0"   // Added on 05/17/2017
  var fileId = 1
  
  @throws(classOf[Exception])
  def main(args: Array[String]) {
    // logger file name 
    MDC.put("fileName", "M4030_WCSHAssignmentDetails")
    
    logger.debug("AssignmentDetailsTesting")
    
    val (hostName, userName, password) = getCassandraConnectionProperties("uat")
    
    val conf = new SparkConf()
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .set("spark.cassandra.input.fetch.size_in_rows", "10000")
      // Default is 1000
      .set("spark.cassandra.input.split.size_in_mb", "100000")
      // Default is 100000
      .set("textinputformat.record.delimiter", "\n")
      .set("spark.eventLog.enabled", "false")
      //.set("spark.cassandra.output.ignoreNulls", "true")
      .setAppName("M4030_WCSHAssignmentDetails")  //Commented on 05/17/2017 to set at spark-submit to handle different name for different Jobs
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]")  // This is required for IDE run      

    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new HiveContext(sc)
    import sqlContext.implicits._
 
    logger.debug("shipping file stream created ")

    val jobId = if (args.length > 0) args(0) else "0".toString()
    cffPath = if ( args(1) == null ) "/spark/MFT/inbound/SHIPPING/" else args(1).toString()     // if (args.length > 1) args(1).toString()
    val currentRunTime = if (args.length > 2) getCurrentTime(args(2)).toString() else getCurrentTime.toString()  
    
    var filename = "null"
    var errorMessage = ""
    var shippingDataWithTimeStamp: DataFrame = null
    var validationStatus = "Failed"
    var loadStatus = "Failed"
    var failedNoOfrows = 0
    var trailerNeedStatus = "Waiting"
    var trailerAvailabilityStatus = "Waiting"  // Added on 05/17/2017
    var hourlyStatus = "null"
    var fileDelimiter = "|"
    try {

      logger.debug("current run time " + currentRunTime + " - jobId => " + jobId)
      sc.broadcast(jobId)
      //sc.broadcast(cffPath)
      sc.broadcast(currentRunTime)
      
      sqlContext.udf.register("convertDateFormat", convertDateFormat _)
      sqlContext.udf.register("convertToLong", convertToLong _)
      sqlContext.udf.register("getSetBackTime", getSetBackTime _)
      sqlContext.udf.register("removeMinSecondsFromDate", removeMinSecondsFromDate _)
          
      //val getConcatenated = udf((col1: String, col2: String) => { col1 + " " + col2 })
      
      // Commented on 05/17/2017
      /*val shippingFileSchema = StructType(Array(
        StructField("whse_nbr", StringType, true),
        StructField("assgn_nbr", StringType, true),
        StructField("load_nbr", StringType, true),
        StructField("stop_nbr", StringType, true),
        StructField("disp_date", StringType, true),
        StructField("disp_time", StringType, true),
        StructField("trailr_type", StringType, true),
        StructField("ent_date", StringType, true),
        StructField("ent_time", StringType, true),
        StructField("del_date", StringType, true),
        StructField("del_time", StringType, true),
        StructField("bill_qty", IntegerType, true),
        StructField("cube", StringType, true),
        StructField("com_code", StringType, true),
        StructField("other_site", StringType, true),
        StructField("cust_nbr", StringType, true),
        StructField("trailr_nbr", StringType, true),
        StructField("assgn_start", StringType, true),
        StructField("assgn_end", StringType, true),
        StructField("closed_date", StringType, true),
        StructField("closed_time", StringType, true),
        StructField("sel_qty", IntegerType, true),
        StructField("scr_qty", IntegerType, true),
        StructField("door_nbr", StringType, true),
        StructField("emp_id", StringType, true),
        StructField("ext_date", StringType, true),
        StructField("ext_time", StringType, true),
        StructField("shift", StringType, true),
        StructField("chain", StringType, true),
        StructField("master_chain", StringType, true),
        StructField("chain_name", StringType, true)))
			*/
      
      // Load warehoused to be processed from control table
      var warehousesControlTable = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "shipping_job_control", "keyspace" -> "wip_shipping_ing"))
        .load()
        .filter(s" job_id = '$jobId' AND enabled = 'Y' ") ;  // Added on 03/28/2017 JobId is added on 05/17/2017
      // group_key = 'ASSIGNMENT_DETAILS' Group_key is removed and enabled is added on 05/17/2017
      
      fileId =  warehousesControlTable.select("file_id").distinct().first().apply(0).toString().toInt   //added on 05/17/2017
      logger.debug("fileId => " + fileId)
      
      // loads file schema 
      // this is moved here on 05/17/2017 
      /*var shippingControlTable = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "file_ingestion_controller", "keyspace" -> "wip_configurations"))
        .load()
        .filter(s" file_id = $fileId");   // Filter is added and job_id = $jobId is removed on 05/17/2017

      shippingControlTable.registerTempTable("shipping_control_table")      
      // Added on 05/17/2017
      try {
           fileDelimiter = shippingControlTable.select("file_delimiter").first().getString(0).trim()  
           cffPath = shippingControlTable.select("file_path").first().getString(0)
      } catch {
           case e @ (_ :Exception | _ :Error | _ :java.io.IOException | _:java.util.NoSuchElementException ) =>
             logger.error("Error while gettign file delimiter at : " + e.getMessage + " "  +  e.printStackTrace() )
             fileDelimiter = "|"
      }
      //sc.broadcast(cffPath)
      //Ended on 05/17/2017
     */
      var thisJobWarehouses = getCurrentWarehouses(warehousesControlTable, jobId)
      logger.debug("thisJobWarehouses => " + thisJobWarehouses)
      
      //  Loading existing Load details
      var loadDetails = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "load_details_by_warehouse", "keyspace" -> "wip_shipping_prs"))
        .load()
        .filter(s"warehouse_id IN ($thisJobWarehouses)")   // Added on 05/31/2017
      loadDetails.registerTempTable("existing_load_details")

      // Fetch warehuse operation time to determine active ops dates
      var operationalMasterDataAll = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "warehouse_operation_time", "keyspace" -> "wip_shipping_ing"))
        .load()
        .filter(s"facility IN ($thisJobWarehouses)")   // Added on 05/31/2017

      //var operationalMasterData = operationalMasterDataAll.filter("facility in ( " + thisJobWarehouses + " )") Commented on 05/31/2017
      operationalMasterDataAll.registerTempTable("warehouse_operational_master")
      //operationalMasterData.show()

      // This is moved from here to up on 05/17/2017

      // moving existing assignments to previous assignments 
      var existingAssignmentsAll = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "assignment_details_by_warehouse", "keyspace" -> "wip_shipping_prs"))
        .load()
        .filter(s"warehouse_id IN ($thisJobWarehouses)")   // Added on 05/31/2017
           
      // determine previous max ext time stamp using assignment previous table
      /*var previousAssignmentsAll = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "assignment_details_by_warehouse_prev", "keyspace" -> "wip_shipping_prs"))
        .load();*/  // Commented on 05/25/2017

      var warehouseShippngData = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "warehouse_shipping_data_history", "keyspace" -> "wip_shipping_hist"))
        .load()
        .filter("whse_nbr in (" + thisJobWarehouses + ") AND com_code NOT IN ('SHU','SH2') ");  //filter added on 05/23/2017
      //warehouseShippngData.registerTempTable("warehouse_shipping_data")  // commented on 05/31/2017
      
      var employeesImagesAll = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "employee_images_by_id", "keyspace" -> "wip_warehouse_data"))
        .load();
      var employeesImages = employeesImagesAll.filter("warehouse_id in (" + thisJobWarehouses + ") ")
                                              .select("warehouse_id", "employee_id", "first_name", "last_name").cache()
      employeesImages.registerTempTable("employee_images")
      
      // Load party site table to map customer name againt chain name
      var arPartySites = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "ar_party_sites_t", "keyspace" -> "ebs_master_data"))
        .load();
      arPartySites.registerTempTable("ar_party_site")
      
      var deletedAssignmentsAll = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "assignments_merged", "keyspace" -> "wip_shipping_ing"))
        .load()
        .filter("invoice_site in (" + thisJobWarehouses + ") ")
      //var deletedAssignments = deletedAssignmentsAll.filter("invoice_site in (" + thisJobWarehouses + ") ")  // Moved and Commented on 05/17/2017
      deletedAssignmentsAll.registerTempTable("deleted_assignments")   // deletedAssignments Commented on 05/17/2017      

      // Load file streaming tracker to load the least ingested file and update the job status
      var fileIngestionStatus = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "file_streaming_tracker", "keyspace" -> "wip_configurations"))
        .load()
        .filter(s" file_id = $fileId");   //Filter is added on 05/17/2017
      fileIngestionStatus.registerTempTable("shpping_files")
      logger.debug("Temp Tables Registered" )
      
      /*val latestFile = fileIngestionStatus.filter("validation_status IN ('Partial_Complete','Fully_Complete') ").groupBy("file_id").agg(max("creation_time").alias("creation_time"))
      latestFile.registerTempTable("shipping_file_tracking")
      val currentFile = sqlContext.sql(""" select st.file_id, st.file_name, st.snapshot_id, st.ext_timestamp, st.creation_time
                                             from shpping_files st, shipping_file_tracking sf 
                                            where sf.file_id=st.file_id 
                                              and sf.creation_time = st.creation_time """).cache()*/
      
      val currentFile = sqlContext.sql(""" SELECT st.file_id, st.file_name, st.snapshot_id, st.ext_timestamp, st.creation_time
                                             FROM shpping_files st
                                            WHERE snapshot_id = 'f54ad690-4714-11e7-98ae-bddb3bd318f0'
                                      """).cache
                                      
      filename = currentFile.first().get(1).toString()
      var snapShotId = currentFile.first().get(2).toString()
      currentExtTime = currentFile.first().get(3).toString()
      previousOpsDay = getNextDatePv(currentFile.first().getTimestamp(3), -1).toString()  // Added on 05/31/2017
      logger.debug("SnapshotId - " + snapShotId + "\n"
                 + "filename - " + filename  + "\n"
                 + "currentExtTime - " + currentExtTime + "\n"
                 + "previousOpsDay - " + previousOpsDay)
      
      /*var fileIngestedDF = fileIngestionStatus.filter("validation_status ='Ingested' ").groupBy("file_id").agg(min("creation_time").alias("creation_time"))
      //file_id=1 and is removed on 05/17/2017
      fileIngestedDF.registerTempTable("shipping_min_file")

      var ingestedFileSchema = sqlContext.sql("select sf.file_id,sf.creation_time ,st.file_schema  from shipping_control_table st, shipping_min_file sf "
        + " where st.file_id = sf.file_id ")    // and st.job_id= + jobIdModified on 05/17/2017
      ingestedFileSchema.registerTempTable("shipping_file_tracking")
      
      // matching schema for the ingested file
      var currentFile = sqlContext.sql("select st.file_id,st.file_name ,sf.file_schema ,st.snapshot_id,sf.creation_time from shpping_files st, shipping_file_tracking sf "
        + " where sf.file_id=st.file_id and sf.creation_time = st.creation_time ").cache()
     
      logger.debug("Current File extracted" )
      //currentFile.show() Commented on 05/17/2017
      
      if (currentFile.rdd.isEmpty) {
        logger.debug(" No files to process are in waiting status  at " + currentRunTime)
        // Modified on 05/17/2017 to handle more runs based on Job id for same file id
        // If file is already processed by a Job id then presentation tables should be processed for New Job id 
        //return
        fileIngestedDF = fileIngestionStatus.filter("validation_status IN ('Fully_Complete','Partial_Complete') ")
                                             .groupBy("file_id")
                                             .agg(max("creation_time").alias("creation_time"))
        fileIngestedDF.registerTempTable("shipping_max_file")
        val currentFileQuery = s""" SELECT sf.file_id, sf.file_name, sf.ext_timestamp, sf.snapshot_id, sf.creation_time
                                      FROM shpping_files sf
                                          ,shipping_max_file smf
                                     WHERE sf.file_id= smf.file_id
                                       and sf.creation_time = smf.creation_time
                                  """
        currentFile = sqlContext.sql(currentFileQuery)
        skipFileProcessing = "1"
        
      }      
      logger.debug(" Skip File processing =>  " + skipFileProcessing)  
      
      filename = currentFile.first().get(1).toString()
      var fileSchema = if ( skipFileProcessing.equals("0") ) currentFile.first().get(2).toString() else ""
      currentExtTime = if ( skipFileProcessing.equals("1") ) currentFile.first().getString(2) else ""
      var snapShotId = currentFile.first().get(3).toString()
      var fileCreationTime = currentFile.first().get(4).toString()

      try {

        if ( skipFileProcessing.equals("0") ) {   // if added on 05/17/2017
             logger.debug("Process File")     
             
             // 05/17/2017 - A code can be added here to mark file Status "In-Processing" to avoid another job for same file to 
             // process the file again
             // In case a program with job id 1 is running and still processing and another program with Job id 2 has started
             // then it shouldnt pick the same file as this file is being processed by Job id 1
             // Code is not written as it will result in 2 scenarios
             // 1. That Job id 1 is still running and hasnt inserted data into warehouse_shipping_data and Job id 2 will try to
             //  query data from warehouse_shipping_data table in code written under "skipFileProcessing else condition", 
             //    (assume "In-Processing" is addded in where clause for code under - currentFile.rdd.isEmpty )
             //  Then it wouldnt find any data as data hasn't been written yet.
             //    (assume if "In-Processing" is not addded in where clause for code under - currentFile.rdd.isEmpty ) 
             //  Then it will process data from old file, which is in "Fully_Complete or Partial_Complete", for Job id 2 
             // 2. A New file has inserted into file_streaming_tracker in Ingested status,  then Job Id 2 will process data from 
             // new file and Job id 1 will process data from Currently processing file and it will not process data for Job id warehouses
             // from New file
             // To avoid above 2 points code is kept as it is, as it will result in both Job id 1 and 2 to process current file parallely
             // which is not harmful.
             
             var textFileStream = sc.textFile(cffPath + "/" + filename)
      
              var columnLength = fileSchema.split(",").size
              val firstField = fileSchema.split(",").apply(0).split(":").apply(0)  // Added on 05/17/2017
              
             logger.debug("file name => " + filename + "\n"
                        + "fileSchema => " + fileSchema + "\n"
                        + "firstField => " + firstField  + "\n"
                        + "filePath => " + cffPath + "/" + filename + "\n"
                        + "columnLength => " + columnLength + "\n"
                        + "FileDelimiter => " + "\\"+ fileDelimiter
                   )
                         
              // convert shipping schema into structure array
              val shippingSchema = StructType(fileSchema.split(",").map(fieldName => StructField(fieldName.split(":")(0),
                                                                                     getCassandraTypeMapping(fieldName.split(":")(1)), true))
                                             )
              // validation file schema against expected schema
              val shippingFileRows = textFileStream.filter { line => !line.contains(firstField) }.flatMap(line => line.split("\n"))
                .map( line => (line.split("\\"+fileDelimiter).mkString(","), line.split("\\"+fileDelimiter).size) )  
                /// "\\|"  "\\|" Modified on 05/17/2017
                // "whse_nbr" is replaced with firstField on 05/17/2017
              //shippingFileRows.foreach(println)
              
              var shippingDF = shippingFileRows.toDF("line", "line_size")
              logger.debug("shippingDF")
              //shippingDF.show()
      
              // filtering records which are matchig with schema lenth
              var matchedRecords = shippingDF.filter(shippingDF("line_size") === columnLength)
              logger.debug("matchedRecords")
              //matchedRecords.show()          
      
              // convert file   into cassandra data types and create data frame using converted data
              var matchedRows = matchedRecords.rdd.repartition(1).map(row => {
      
                var convertedDataList: collection.mutable.Seq[Any] = collection.mutable.Seq.empty[Any]
                var index = 0
                var colValues = row(0).toString().split(",")
      
                colValues.foreach(value => {
                  var colType = shippingSchema.fields(index).dataType
                  var returnVal: Any = null
                  //print(value)
                  colType match {
                    case IntegerType   => returnVal = value.toString.toInt
                    case DoubleType    => returnVal = value.toString.toDouble
                    case LongType      => returnVal = value.toString.toLong
                    case FloatType     => returnVal = value.toString.toFloat
                    case ByteType      => returnVal = value.toString.toByte
                    case StringType    => returnVal = value.toString
                    case TimestampType => returnVal = value.toString
                  }
                  convertedDataList = convertedDataList :+ returnVal
                  index += 1
                })
                Row.fromSeq(convertedDataList)
              });
      
              var matchedDF = sqlContext.createDataFrame(matchedRows, shippingSchema)
      
              var updatedShipping = matchedDF.withColumn("snapshot_id", lit(snapShotId)).withColumn("creation_time", lit(fileCreationTime)).withColumn("update_time", lit(currentRunTime))
              updatedShipping.registerTempTable("shipping_data")
      
              // date formatting  closed , ext and disptch time stamps for the current snapshot data
              shippingDataWithTimeStamp = sqlContext.sql("select case when length(trim(sd.disp_time)) > 0  and sd.disp_time is not null then convertDateFormat(concat(sd.disp_date,' ',sd.disp_time,'00')) else null end  as dispatch_timestamp,case when length(trim(sd.closed_time)) > 0  and sd.closed_time is not null then convertDateFormat(concat(sd.closed_date,' ',sd.closed_time,'00')) else null end as closed_timestamp,case when length(trim(sd.ext_time)) > 0  and sd.ext_time is not null then  convertDateFormat(concat(sd.ext_date,' ',sd.ext_time)) else null end as ext_timestamp  "
                + " ,case when length(trim(sd.assgn_start_date)) > 0 and sd.assgn_start_date <> '00000000' and sd.assgn_start_date is not null then convertDateFormat(concat(sd.assgn_start_date,' ' ,sd.assgn_start) ) else null end  as assgn_start_time ,case when length(trim(sd.assgn_end_date)) > 0  and sd.assgn_end_date <> '00000000'  and sd.assgn_end_date is not null then convertDateFormat(concat(sd.assgn_end_date,' ',sd.assgn_end) ) else null end  as assgn_end_time,  sd.* from  shipping_data sd ")
              //case when length(trim(sd.assgn_start_date)) > 0  and sd.assgn_start_date is not null then date_format(sd.assgn_start_date,'yyyy-MM-dd' ) else null end  as assgn_start_date ,case when length(trim(sd.assgn_end_date)) > 0  and sd.assgn_end_date is not null then date_format(sd.assgn_end_date,'yyyy-MM-dd' ) else null end  as assgn_end_date 
      
              // return if empty file 
              if (shippingDataWithTimeStamp.rdd.isEmpty) {
                logger.debug("Encountered Empty file   " + filename + " at " + currentRunTime)
                validationStatus = "Failed"
                throw new Exception("Failed to process Invalid Schema File - " + filename + " at  " + currentRunTime)
              }
      
              //logger.debug("warehouse shipping data saved ")
              // save unmatched records to CFS
              var unMatchedRecords = shippingDF.filter(shippingDF("line_size") !== columnLength)
              failedNoOfrows = unMatchedRecords.count().toInt
      
              if (failedNoOfrows > 0)
                unMatchedRecords.repartition(1).rdd.saveAsTextFile(cffPath + "/" + "rejected_" + filename + "_" + ShippingUtil.getCurrentTime().getTime)
      
              if (failedNoOfrows > 0) {
                validationStatus = "Partial_Complete"
                //save failure status on the file
              } else {
                validationStatus = "Fully_Complete"
              }
      
              // ext time from current snapshot data
              currentExtTime = shippingDataWithTimeStamp.first().get(2).toString()
        }else {
            /*val shippingDataWithTimeStampQuery = s""" SELECT *
                                                        FROM warehouse_shipping_data wsd
                                                       WHERE snapshot_id = '$snapShotId'
                                                  """*/
          logger.debug("Skip file processing and Get Data from Shipping table directly")  
          shippingDataWithTimeStamp = warehouseShippngData.filter(s" snapshot_id = '$snapShotId' ")    //sqlContext.sql(shippingDataWithTimeStampQuery)
      
        }
        */
        try {
              val shippingCurrentSnapShotData = warehouseShippngData.filter(s" snapshot_id = '$snapShotId' ").persist()
              shippingCurrentSnapShotData.registerTempTable("shipping_snapshot_data")
              sc.broadcast(currentExtTime)
              
               //Not Required for Testing Pushkar
              /*var existingAssignments = existingAssignmentsAll.filter("ops_date >= date_format('" + getPreviousDate(currentExtTime.toString()) + "','yyyy-MM-dd')").cache() // warehouse_id in ( " + thisJobWarehouses + " ) and  Commented as it is already added on Dataframe above
              existingAssignments.registerTempTable("existing_assignments_all")  
              
              // updating merged assignments with existing assignments 
              
              var existingAssignmentsWithMerged  = sqlContext.sql(s"""select ea.warehouse_id,ea.ops_date,ea.load_number,ea.stop_number ,ea.assignment_id ,ea.assgn_end_time ,ea.assgn_start_time ,ea.assignment_type ,ea.bill_qty ,ea.cases_selected ,ea.closed_timestamp ,ea.customer_name ,ea.customer_number ,ea.dispatch_date_time ,ea.door_number ,ea.employee_id ,ea.employee_name ,ea.ext_timestamp ,ea.is_load_deferred ,ea.loaded_pct ,ea.number_of_cubes ,ea.print_site ,ea.scr_qty ,ea.selected_pct ,ea.snapshot_id ,ea.update_time, 
                                                                      case when ( da.assgn_nbr is not null AND da.ext_timestamp <= '$currentExtTime' ) then 'Y' else 'N' end as merged_status,
                                                                      case when ( ea.merged_status = 'Y' ) THEN 'Y' when ( da.assgn_nbr is not null AND da.ext_timestamp <= '$currentExtTime' AND ( hour(da.ext_timestamp) < CASE WHEN LPAD( hour('$currentExtTime'), 2, '0' ) = '00' THEN '24' ELSE hour('$currentExtTime') END ) ) then 'Y' else 'N' end as merged_status_prev,
                                                                      ea.merged_status existing_merged_status  
                                                                        from existing_assignments_all ea 
                                                                        left outer join deleted_assignments da 
                                                                          on ea.warehouse_id = da.invoice_site 
                                                                             and ea.ops_date = da.ops_date 
                                                                             and ea.assignment_id = da.assgn_nbr
                                                                      WHERE ea.merged_status = 'N'
                                                                    """)  //and ea.load_number=da.load_nbr Commented on 02/14/2017 as da.load_nbr is coming as '000000000000' da.ext_timestamp <= '$currentExtTime' is changed on 05/01/2017
                                                                     //merged_status_prev Added on 05/08/2017
                                                                        //ea.ext_timestamp is replaced with '$currentExtTime' and = is replaced with <= on 05/09/2017
                                                                        //when ea.merged_status = 'Y' THEN 'Y'  added on 05/10/2017
                                                                       // existing_merged_status and WHERE condition Added on 05/31/2017
                                                                    
              // added on 05/01/2017
              val existingAssignmentsWithMergedSt  = existingAssignmentsWithMerged.select(existingAssignmentsWithMerged("warehouse_id"), existingAssignmentsWithMerged("ops_date"), existingAssignmentsWithMerged("load_number")
                                                                                         ,existingAssignmentsWithMerged("stop_number"), existingAssignmentsWithMerged("assignment_id"), existingAssignmentsWithMerged("merged_status")
                                                                                         ,existingAssignmentsWithMerged("ext_timestamp"), existingAssignmentsWithMerged("customer_name"), existingAssignmentsWithMerged("customer_number")
                                                                                         ,existingAssignmentsWithMerged("dispatch_date_time"), existingAssignmentsWithMerged("door_number"), existingAssignmentsWithMerged("assignment_type")
                                                                                         ,existingAssignmentsWithMerged("closed_timestamp"), existingAssignmentsWithMerged("employee_id"), existingAssignmentsWithMerged("employee_name")
                                                                                         ,existingAssignmentsWithMerged("assgn_end_time"), existingAssignmentsWithMerged("assgn_start_time")
                                                                                         ,existingAssignmentsWithMerged("is_load_deferred"), existingAssignmentsWithMerged("cases_selected"), existingAssignmentsWithMerged("snapshot_id")
                                                                                         ,existingAssignmentsWithMerged("bill_qty"), existingAssignmentsWithMerged("loaded_pct"), existingAssignmentsWithMerged("number_of_cubes")
                                                                                         ,existingAssignmentsWithMerged("scr_qty"), existingAssignmentsWithMerged("selected_pct"), existingAssignmentsWithMerged("print_site")
                                                                                          )
                                                                                    .withColumn("update_time", lit(currentRunTime) )
                                                                                    .filter("merged_status='Y'")  
                                                                                    // Filter is moved here on 05/08/2017
                
               /*sqlContext.sql(s"""select ea.warehouse_id,ea.ops_date,ea.load_number,ea.stop_number ,ea.assignment_id, '$currentRunTime' update_time
                                                                      ,case when ( da.assgn_nbr is not null AND da.ext_timestamp <= '$currentExtTime' ) then 'Y' else 'N' end as merged_status 
                                                                      from existing_assignments_all ea left outer join deleted_assignments da 
                                                                        on ea.warehouse_id = da.invoice_site and ea.ops_date=ea.ops_date and ea.assignment_id = da.assgn_nbr""")*/  
               //logger.debug("Assignments marked as Merge")
               //val existingAssignmentsWithMergedStY = existingAssignmentsWithMergedSt.filter("merged_status='Y'") // Commented on 05/08/2017 and Moved above
               //existingAssignmentsWithMergedSt.filter("merged_status='Y'").show()   
                                                                                    
               saveDataFrame(existingAssignmentsWithMergedSt, "wip_shipping_prs", "assignment_details_by_warehouse")  //existingAssignmentsWithMergedStY  modified on 05/08/2017
                try {
                     //Only Assignments with Merge Flag as Y will be stored in history otherwise it will unnecessary put multiple records in history table on each run 
                     val assigmentMergedHistory = existingAssignmentsWithMergedSt.withColumn("snapshot_time", lit(currentRunTime)).cache()  //existingAssignmentsWithMergedStY modified on 05/08/2017
                     saveDataFrame(assigmentMergedHistory, "wip_shipping_hist", "assignment_details_by_warehouse_history")

                }catch{
                  case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                    logger.error("Error while executing takin Assignments history for assigmentMergedHistory at : " +  e.getMessage + " " + e.printStackTrace() )
                }         
              //Ended on 05/01/2017
              */ 
              //Not Required for Testing Pushkar
              //existingAssignmentsWithMerged.filter("merged_status='Y'").show()  Commented on 04/28/2017
              
              // Added - Select clause added and saved in new Val on 05/08/2017 to handle Merged Status Prev
              /*val existingAssignmentsWithMergedPrev = existingAssignmentsWithMerged
              .select(existingAssignmentsWithMerged("warehouse_id"), existingAssignmentsWithMerged("ops_date"), existingAssignmentsWithMerged("load_number")
                     ,existingAssignmentsWithMerged("stop_number"), existingAssignmentsWithMerged("assignment_id"), existingAssignmentsWithMerged("merged_status_prev").alias("merged_status")
                     ,existingAssignmentsWithMerged("ext_timestamp"), existingAssignmentsWithMerged("customer_name"), existingAssignmentsWithMerged("customer_number")
                     ,existingAssignmentsWithMerged("dispatch_date_time"), existingAssignmentsWithMerged("door_number"), existingAssignmentsWithMerged("assignment_type")
                     ,existingAssignmentsWithMerged("closed_timestamp"), existingAssignmentsWithMerged("employee_id"), existingAssignmentsWithMerged("employee_name")
                     ,existingAssignmentsWithMerged("assgn_end_time"), existingAssignmentsWithMerged("assgn_start_time")
                     ,existingAssignmentsWithMerged("is_load_deferred"), existingAssignmentsWithMerged("cases_selected"), existingAssignmentsWithMerged("snapshot_id")
                     ,existingAssignmentsWithMerged("bill_qty"), existingAssignmentsWithMerged("loaded_pct"), existingAssignmentsWithMerged("number_of_cubes")
                     ,existingAssignmentsWithMerged("scr_qty"), existingAssignmentsWithMerged("selected_pct"), existingAssignmentsWithMerged("print_site")
                      ).withColumn("update_time", lit(currentRunTime) )
                      */  // Commented on 05/25/2017
              // Ended - Select clause added and saved in new Val on 05/08/2017 to handle Merged Status Prev           
                
              //existingAssignmentsWithMerged.registerTempTable("existing_assignments")  Commented on 05/31/2017 as not required 
              
              //var shippingCurrentSnapShotData = shippingDataWithTimeStamp.filter(" whse_nbr in ( " + thisJobWarehouses + " ) AND com_code NOT IN ('SHU','SH2') ").cache()
              //com_code filter is added on 05/17/2017
              //shippingCurrentSnapShotData.registerTempTable("shipping_snapshot_data")
              
              // saving current snapshot data and current assignment to previous assignment table.
              //saveDataFrame(existingAssignmentsWithMergedPrev, "wip_shipping_prs", "assignment_details_by_warehouse_prev")  //existingAssignmentsWithMerged replaced on 05/08/2017
              // Commented on 05/25/2017
              /*if ( skipFileProcessing.equals("0") ) {   // if added on 05/17/2017
                  saveDataFrame(shippingDataWithTimeStamp, "wip_shipping_ing", "warehouse_shipping_data")
                  saveDataFrame(shippingDataWithTimeStamp, "wip_shipping_hist", "warehouse_shipping_data_history")
              }*/
              //logger.debug(" Existing Assignments marked as Merged ")
              
              //var previousAssignments = previousAssignmentsAll.filter("warehouse_id in ( " + thisJobWarehouses + " ) and ops_date >= date_format('" + ShippingUtil.getPreviousDate(currentExtTime.toString()) + "','yyyy-MM-dd')").cache()
              //previousAssignments.registerTempTable("previous_assignments")  Commented on 05/25/2017
      
              /*var previousMaxAssignment = sqlContext.sql("select ext_timestamp as max_ext_timestamp ,case when  snapshot_id is null then 'NA' else snapshot_id end as snapshot_id from previous_assignments order by ext_timestamp desc limit 1").cache()
              //previousMaxAssignment.show()  Commented on 05/17/2017
      
              var previousMaxExtTime = if (!previousMaxAssignment.rdd.isEmpty) previousMaxAssignment.first().get(0).toString() else null
              var previousSnapShotId = if (!previousMaxAssignment.rdd.isEmpty) previousMaxAssignment.first().get(1).toString() else null
              //maxPreviousAssignments.show()
      
              sc.broadcast(previousMaxExtTime)
              sc.broadcast(previousSnapShotId)
      
              logger.debug("previos ext time " + previousMaxExtTime + "\n"
                         + "current  ext time " + currentExtTime + "\n"
                         + "previousSnapShotId " + previousSnapShotId)
              */ //Commented on 05/25/2017
              
              // active warehouse ops dates
              var currentOpsDates = sqlContext.sql(" select wm.facility , wm.ops_date , wm.start_time from warehouse_operational_master wm where wm.start_time <= date_format('" + currentExtTime + "','yyyy-MM-dd HH:mm:ss') and wm.end_time >= date_format('" + currentExtTime + "','yyyy-MM-dd HH:mm:ss') ").cache()
              currentOpsDates.registerTempTable("current_warehouse_ops_dates")
              //currentOpsDates.show()
      
              // mapping  ops date for  ext times for current snap shot data
              var assignmentData = sqlContext.sql("select sd.* ,wm.ops_date , case when sd.closed_timestamp is not null and sd.closed_timestamp >= wm.start_time then 'Y' else 'N' end as closed_today  , case when sd.closed_timestamp is null  then 'Y' else 'N' end as open_loads from shipping_snapshot_data sd ,  current_warehouse_ops_dates wm where  sd.whse_nbr = wm.facility ").cache()
              assignmentData.persist() // Added on 05/25/2017
              //and  ( sd.com_code <> 'SHU' and  sd.com_code <> 'SH2' ) is already handled at Dataframe level. Removed on 05/17/2017
              //var activeAssignments = sqlContext.sql(" select ea.* from existing_assignments ea , current_warehouse_ops_dates wm where ea.warehouse_id = wm.facility and ea.ops_date = wm.ops_date ").cache()
              // processing delta assignment logic when current snapshot ext time is not equal to previous assignment mx time
      
              /*var prevDeltaAssignments = processDeltaAsignments(shippingCurrentSnapShotData, currentExtTime, previousMaxExtTime)
      
              // previous snapshot data with delta assignments processed when there is delta assignments wth different ops date
              if (prevDeltaAssignments != null && !prevDeltaAssignments.rdd.isEmpty) {
      
                prevDeltaAssignments.registerTempTable("prev_delta_assignments")
      
                //substracting previous snapshot data with delta assignments
                var previousAssignmentsWithoutDelta = sqlContext.sql(" select ea.whse_nbr,ea.snapshot_id,ea.ent_date,ea.load_nbr,ea.stop_nbr,ea.assgn_nbr,ea.assgn_end,ea.assgn_end_date,ea.assgn_end_time,ea.assgn_start,ea.assgn_start_date,ea.assgn_start_time,ea.bill_qty,ea.chain,ea.chain_name,ea.closed_date,ea.closed_time,ea.closed_timestamp,ea.com_code,ea.creation_time,ea.cube,ea.cust_nbr,ea.del_date,ea.del_time,ea.disp_date,ea.disp_time,ea.dispatch_timestamp,ea.door_nbr,ea.emp_id,ea.ent_time,ea.ext_date,ea.ext_time,ea.ext_timestamp,ea.master_chain,ea.other_site,ea.payroll_date,ea.print_site,ea.scr_qty,ea.sel_cube,ea.sel_qty,ea.shift,ea.status,ea.trailr_nbr,ea.trailr_type,ea.update_time "
                  + " from ( select ws.* from  warehouse_shipping_data ws , previous_warehouse_ops_dates  cw  where ws.whse_nbr=cw.facility and ws.snapshot_id = '" + previousSnapShotId + "' ) ea left join  prev_delta_assignments pd on ea.whse_nbr = pd.whse_nbr and ea.load_nbr = pd.load_nbr and ea.stop_nbr = pd.stop_nbr and ea.assgn_nbr = pd.assgn_nbr where pd.assgn_nbr is null ").cache()
      
                // merge delta assignments with previous snapshot data 
                previousAssignmentsWithoutDelta.unionAll(prevDeltaAssignments).registerTempTable("prev_delta_assignments_merged")
      
                // finding loads are open and closed today from previous snapshot data
                var deltaAssignmentsMerged = sqlContext.sql("select sd.* ,wm.ops_date , case when sd.closed_timestamp is not null and sd.closed_timestamp >= wm.start_time then 'Y' else 'N' end as closed_today  , case when sd.closed_timestamp is null  then 'Y' else 'N' end as open_loads from prev_delta_assignments_merged sd ,  previous_warehouse_ops_dates wm where  sd.whse_nbr = wm.facility ").cache()
      
                //logger.debug("delta assignments " + prevDeltaAssignments.count())  Commented on 05/17/2017
                //logger.debug("previous assignments without delta" + previousAssignmentsWithoutDelta.count()) Commented on 05/17/2017
      
                //prev_delta_assignments ps , previous_warehouse_ops_dates pw 
                processLoadStopAssignments(deltaAssignmentsMerged.filter("closed_today ='Y' or  open_loads ='Y'"))
      
              }*/   //Commented on 05/25/2017
      
              processLoadStopAssignments(assignmentData.filter("closed_today ='Y' or  open_loads ='Y'"))
      
              loadStatus = "Load_complete"
              hourlyStatus = "Waiting"
      
              if (assignmentData.rdd.isEmpty) {
                //validationStatus = "Partial_Complete"  // "No Data" Modified on 05/17/2017 as It is possible no data found for Warehouse Id which processes the file
                loadStatus = "No Data"
                hourlyStatus = "Waiting"  //"No Data" Modified on 05/17/2017 as It is possible no data found for Warehouse Id which processes the file
              }
      
              logger.debug("file load completed")

       }
      catch {
        case e: Exception => {
          e.printStackTrace()
          logger.error("ShppingFileStream  " + filename + " Error :  " + e)
          errorMessage = e.getMessage

        }
      }
      /*if (currentExtTime == null) {
        validationStatus = "Failed"
      }  
      
      if ( skipFileProcessing.equals("0") ) {   // if added on 05/17/2017
         var fileStatusRDD = sc.parallelize(Seq(Row( fileId, filename, snapShotId, failedNoOfrows, validationStatus, errorMessage, currentExtTime)))  //, loadStatus, trailerNeedStatus, hourlyStatus Removed on 05/17/2017
         var fileStatusDF = sqlContext.createDataFrame(fileStatusRDD, fileStatusSchema).toDF()
         saveDataFrame(fileStatusDF.withColumn("update_time", lit(ShippingUtil.getCurrentTime())), "wip_configurations", "file_streaming_tracker")
         logger.debug(" file " + filename + " processed  at " + ShippingUtil.getCurrentTime())
      }*/  // Commmented on 05/26/2017
      
      //val warehouseList = thisJobWarehouses.split(",")
      val fileTrackerStatusSchema = new StructType( Array( StructField("file_id", IntegerType, false ) 
                                                            ,StructField("file_name", StringType, false ) 
                                                            ,StructField("snapshot_id", StringType, false ) 
                                                            ,StructField("ext_timestamp", StringType, true ) 
                                                            ,StructField("job_id", StringType, false ) 
                                                            ,StructField("warehouses", ArrayType(StringType, true), true ) 
                                                            ,StructField("load_status", StringType, true )
                                                            ,StructField("hourly_cases_status", StringType, true )
                                                            ,StructField("trailer_need_status", StringType, true )
                                                            ,StructField("trailer_availability_status", StringType, true ) 
                                                            ,StructField("error", StringType, true ) 
                                                            ,StructField("update_time", TimestampType, true ) 
                                                            )
                                     )      
      val fileStatusRDD = sc.parallelize(Seq(Row( fileId, filename, snapShotId, currentExtTime, jobId, thisJobWarehouses.split(","), loadStatus, hourlyStatus, trailerNeedStatus, trailerAvailabilityStatus, errorMessage)))  //, loadStatus, trailerNeedStatus, hourlyStatus Removed on 05/17/2017
      val fileStatusDF = sqlContext.createDataFrame(fileStatusRDD, fileTrackerStatusSchema).toDF()
      saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_streaming_tracker_status")
      logger.debug(" file " + filename + "processed  at " + getCurrentTime())

      logger.debug("CSSparkJobStatus:Completed" )
      sqlContext.clearCache()
      
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logger.error("ShppingFileStream : " + filename + " Error : " + e)
        throw new Exception("Error while executing  ShppingFileStream " + filename + " Error  :" + e.getMessage + ": " + e.printStackTrace())
      }

    }

    @throws(classOf[Exception])
    def getCurrentJobFiles(warehousesControlTable: DataFrame, jobId: String): String = {
      warehousesControlTable.show()
      var warehousesForThisJobId = warehousesControlTable.filter(warehousesControlTable("job_id") === jobId)
      var warehousesFilteredList = warehousesForThisJobId.select("warehouse_id").flatMap { r => List(r.getString(0)) }
      return warehousesFilteredList.collect() map { "\'%s\'".format(_) } mkString ","
    }

    @throws(classOf[Exception])
    def getCurrentWarehouses(warehousesControlTable: DataFrame, jobId: String): String = {
      //warehousesControlTable.show()  Commented on 05/17/2017 to improve Performance
      var warehousesForThisJobId = warehousesControlTable.filter(warehousesControlTable("job_id") === jobId)
      var warehousesFilteredList = warehousesForThisJobId.select("warehouse_id").flatMap { r => List(r.getString(0)) }
      return warehousesFilteredList.collect() map { "\'%s\'".format(_) } mkString ","
    }

    def saveDataFrame(dataFrame: DataFrame, keyspacename: String, tablename: String): Boolean = {
      dataFrame.write.format("org.apache.spark.sql.cassandra")
        .mode("append").option("table", tablename)
        .option("keyspace", keyspacename).save()
      return true;
    }

    /*
     * processing delta assignments from current snapshot data against previos assignment data 
    */

    @throws(classOf[Exception])
    def processDeltaAsignments(currentSnapShotData: DataFrame, crExtTime: String, prExtTime: String): DataFrame = {
      var prevOpsDateAssignments = null: DataFrame
      try {
        // delta assignments are processed only when prev and current hours are differenr
        if (prExtTime != null && crExtTime != null && getCurrentHourFromDate(crExtTime) != getCurrentHourFromDate(prExtTime)) {
          logger.debug("processing  delta assignments previousMaxExtTime : " + prExtTime + " currentExtTime : " + crExtTime)

          // delta assignments - current snapshot assignment end tie > previous ext time or closed time > previos ext time or close time < current ext time 
          var deltaAssignments = sqlContext.sql("select sd.*  from shipping_snapshot_data sd where ( ( sd.assgn_end_time >= date_format('" + prExtTime + "','yyyy-MM-dd HH:mm:ss') and sd.assgn_end_time < removeMinSecondsFromDate('" + crExtTime + "') )  "
            + " or ( sd.closed_timestamp >= date_format('" + prExtTime + "','yyyy-MM-dd HH:mm:ss')  and sd.closed_timestamp < removeMinSecondsFromDate('" + crExtTime + "') ) ) ").cache()
          //and  ( sd.com_code <> 'SHU' and  sd.com_code <> 'SH2' ) is already handled at Dataframe level. Removed on 05/17/2017
          deltaAssignments.registerTempTable("delta_assignments")
          //deltaAssignments.select("whse_nbr", "assgn_end_time", "closed_timestamp").show() Commented on 05/17/2017
          
          // finding ops date for previos max ext times
          var previousOpsDates = sqlContext.sql(" select wm.facility , wm.ops_date , wm.start_time ,wm.end_time from warehouse_operational_master wm where wm.start_time <= date_format('" + prExtTime + "','yyyy-MM-dd HH:mm:ss') and wm.end_time >= date_format('" + prExtTime + "','yyyy-MM-dd HH:mm:ss') ").cache()
          previousOpsDates.registerTempTable("previous_warehouse_ops_dates")
           
          //Modified on 05/17/2017 Multiple Queries are combined to one query
          //overwriting delta assignments with previus ops date
          /*var deltaAssignmentsWithPrevOpsDates = sqlContext.sql("select sd.* ,wm.ops_date from delta_assignments sd , previous_warehouse_ops_dates wm where wm.facility = sd.whse_nbr ").withColumn("ext_timestamp", lit(prExtTime)).cache()
          deltaAssignmentsWithPrevOpsDates.registerTempTable("delta_assignments_prev_ops_dates")
          //deltaAssignmentsWithPrevOpsDates.select("whse_nbr", "assgn_end_time", "closed_timestamp", "ops_date").show() Commented on 05/17/2017
          
          // addding merged on the overwrittn previous date assignments 
          var deltaAssignmentsWithPrevOpsDatesAndMergedStatus = sqlContext.sql("select sd.* , case when da.assgn_nbr is not null then 'Y' else 'N' end as merged_status from delta_assignments_prev_ops_dates sd left outer join deleted_assignments da on sd.whse_nbr = da.invoice_site and sd.ops_date=da.ops_date and sd.assgn_nbr = da.assgn_nbr ").cache()  //and sd.load_nbr=da.load_nbr commented on 02/14/2017 as da.load_nbr is coming as '000000000000' 
          deltaAssignmentsWithPrevOpsDatesAndMergedStatus.registerTempTable("delta_assignments_prev_ops_dates_merged")
          
          // preparing delta assignments for assignment presentation data
          var deltaAssignmentsType = sqlContext.sql("select  sd.whse_nbr as warehouse_id , sd.ops_date, sd.load_nbr as load_number ,sd.stop_nbr as stop_number  ,sd.assgn_nbr as assignment_id ,sd.chain as customer_number,sd.chain_name as customer_name,sd.closed_timestamp,sd.bill_qty,sd.scr_qty, sd.ext_timestamp, sd.door_nbr as door_number,case when sd.com_code ='DRO' then 'DROP' else sd.com_code end as assignment_type ,sd.merged_status , sd.emp_id as employee_id ,concat(ei.last_name ,' ,',ei.first_name ) as employee_name , sd.update_time from delta_assignments_prev_ops_dates_merged sd left outer join employee_images ei on sd.whse_nbr=ei.warehouse_id and  cast(sd.emp_id as Int) =  ei.employee_id  ")
          */
          val deltaAssignmentsTypeQuery = s"""select sd.whse_nbr as warehouse_id
                                                    ,wm.ops_date ops_date
                                                    ,'$prExtTime' ext_timestamp
                                                    ,case when da.assgn_nbr is not null then 'Y' else 'N' end as merged_status
                                                    ,sd.load_nbr as load_number, sd.stop_nbr as stop_number, sd.assgn_nbr as assignment_id
                                                    ,sd.chain as customer_number, sd.chain_name as customer_name, sd.closed_timestamp
                                                    ,sd.bill_qty, sd.scr_qty, sd.door_nbr as door_number, case when sd.com_code ='DRO' then 'DROP' else sd.com_code end as assignment_type
                                                    ,sd.emp_id as employee_id, concat(ei.last_name ,' ,',ei.first_name ) as employee_name 
                                                    ,sd.update_time
                                                FROM delta_assignments sd 
                                                JOIN previous_warehouse_ops_dates wm 
                                                   ON wm.facility = sd.whse_nbr 
                                                LEFT OUTER JOIN deleted_assignments da 
                                                   ON sd.whse_nbr = da.invoice_site 
                                                     AND wm.ops_date = da.ops_date 
                                                     AND sd.assgn_nbr = da.assgn_nbr
                                                LEFT OUTER JOIN employee_images ei 
                                                    ON sd.whse_nbr = ei.warehouse_id 
                                                      AND  cast(sd.emp_id as Int) =  ei.employee_id
                                          """
          
          var deltaAssignmentsType = sqlContext.sql(deltaAssignmentsTypeQuery)
          //Modification ended on 05/17/2017 Multiple Queries are combined to one query
          
          saveDataFrame(deltaAssignmentsType, "wip_shipping_prs", "assignment_details_by_warehouse_prev")
          logger.debug(" delta assignments saved to assignment previous table")

          //filter warehouses which has different ops date  
          prevOpsDateAssignments = sqlContext.sql("select ea.whse_nbr,ea.snapshot_id,ea.ent_date,ea.load_nbr,ea.stop_nbr,ea.assgn_nbr,ea.assgn_end,ea.assgn_end_date,ea.assgn_end_time,ea.assgn_start,ea.assgn_start_date,ea.assgn_start_time,ea.bill_qty,ea.chain,ea.chain_name,ea.closed_date,ea.closed_time,ea.closed_timestamp,ea.com_code,ea.creation_time,ea.cube,ea.cust_nbr,ea.del_date,ea.del_time,ea.disp_date,ea.disp_time,ea.dispatch_timestamp,ea.door_nbr,ea.emp_id,ea.ent_time,ea.ext_date,ea.ext_time,ea.ext_timestamp,ea.master_chain,ea.other_site,ea.payroll_date,ea.print_site,ea.scr_qty,ea.sel_cube,ea.sel_qty,ea.shift,ea.status,ea.trailr_nbr,ea.trailr_type,ea.update_time  from delta_assignments ea ,( select pw.facility  from current_warehouse_ops_dates cw , previous_warehouse_ops_dates pw where cw.facility = pw.facility and cw.ops_date <> pw.ops_date ) dw where ea.whse_nbr = dw.facility ").cache()

        }

      } catch {
        case e: Exception => {
          logger.error("Error while processisng Previous Delta Assignment for   " + filename + "   at : " + currentRunTime + e)
          throw new Exception("Error while processisng Previous Delta Assignment for   " + filename + "   at : " + currentRunTime + e.printStackTrace())
        }

      }
      return prevOpsDateAssignments

    }

    /*
     * processing load stop assignments for the current snapshot data 
     */
    @throws(classOf[Exception])
    def processLoadStopAssignments(assignmentDF: DataFrame ): String = {
      try {
        
        val currrentTime = getCurrentTime.toString() // Added on 04/25/2017 to add Assignments history
        assignmentDF.registerTempTable("shipping_data")
        //activeAssignments.registerTempTable("active_assignments")

        // current assignments , adding merged assignments with Y
        var shippingDataByOpsDate = sqlContext.sql("select sdc.whse_nbr as warehouse_id,sdc.ops_date,sdc.chain as customer_number ,sdc.chain_name as customer_name ,sdc.door_nbr as door_number , sdc.assgn_nbr,sdc.shift as shift_id ,sdc.emp_id as employee_id ,sdc.stop_nbr ,sdc.trailr_type as trailer_type ,sdc.disp_time as dispatch_time ,sdc.closed_time ,sdc.cust_nbr,sdc.load_nbr  as load_number ,sdc.bill_qty  "
          + " , sdc.scr_qty ,sdc.sel_qty ,sdc.cube ,sdc.ext_timestamp, sdc.ent_date ,sdc.com_code "
          + " , sdc.sel_cube ,sdc.closed_timestamp , sdc.dispatch_timestamp ,sdc.update_time  "
          + " ,  sdc.assgn_start_time ,sdc.assgn_end_time,sdc.snapshot_id , case when ( da.assgn_nbr is not null AND da.ext_timestamp <= '" + currentExtTime + "' ) then 'Y' else 'N' end as merged_status  "  //da.ext_timestamp <= sdc.ext_timestamp Added on 04/28/2017 sdc.ext_timestamp is replaced with currentExtTime on 05/01/2017
          + " from shipping_data sdc left outer join deleted_assignments da " 
          + " on sdc.whse_nbr = da.invoice_site and sdc.ops_date=da.ops_date " 
          + " and sdc.assgn_nbr = da.assgn_nbr "
          + " WHERE sdc.load_nbr = '000000059618' " ).cache()  // and sdc.load_nbr=da.load_nbr commented on 02/14/2017 as da.load_nbr is coming as '000000000000'
        //where sdc.com_code <> 'SHU' and  sdc.com_code <> 'SH2' Removed as it is already handled in the Dataframe
          
        // filtering shutter assignments 
        shippingDataByOpsDate.registerTempTable("shipping_data_by_ops_date")
        sqlContext.cacheTable("shipping_data_by_ops_date")  // Added on 05/25/2017
        shippingDataByOpsDate.persist(StorageLevel.MEMORY_AND_DISK_SER)                     // Added on 05/25/2017
        shippingDataByOpsDate.show(40)
       
        //### Assignment Details Started
        // determining assignment types 
        var assigmentType = sqlContext.sql("select  sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_nbr as stop_number  ,sd.assgn_nbr as assignment_id ,sd.customer_number,sd.customer_name,sd.closed_timestamp,sd.bill_qty,sd.scr_qty, sd.ext_timestamp, sd.door_number,case when sd.com_code ='DRO' then 'DROP' else sd.com_code end as assignment_type ,sd.merged_status ,sd.employee_id ,concat(ei.last_name ,' ,',ei.first_name ) as employee_name , sd.update_time ,sd.snapshot_id from shipping_data_by_ops_date sd left outer join employee_images ei on sd.warehouse_id=ei.warehouse_id and  cast(sd.employee_id as Int) =  ei.employee_id  ")
        //assigmentType.show()
        assigmentType.persist(StorageLevel.MEMORY_AND_DISK_SER)  // Added on 05/25/2017
        assigmentType.show(40)
       // saveDataFrame(assigmentType, "wip_shipping_prs", "assignment_details_by_warehouse") //Not required for Testing Pushkar

      // Added on 04/25/2017 to take Assignments history
      /*try {
           val assigmentTypeHistory = assigmentType.withColumn("snapshot_time", lit(currrentTime)).cache()
          /*existingAssignmentsAll
             .select( "warehouse_id" , "ops_date", "load_number", "stop_number", "assignment_id"
                    , "assgn_end_time", "assgn_start_time", "assignment_type", "bill_qty"
                    , "cases_selected", "closed_timestamp", "customer_name", "customer_number", "dispatch_date_time", "door_number"
                    , "employee_id", "employee_name", "ext_timestamp", "is_assignment_suspended", "is_load_deferred", "loaded_pct"
                    , "merged_status", "number_of_cubes", "print_site", "scr_qty", "selected_pct", "snapshot_id", "update_time"
                    )*/
           saveDataFrame(assigmentTypeHistory, "wip_shipping_hist", "assignment_details_by_warehouse_history")

      }catch{
        case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing takin Assignments history for assigmentType at : " +  e.getMessage + " " + e.printStackTrace() )
      }*/  //Not required for Testing Pushkar
      // Ended on 04/25/2017 to take Assignments history
      
      logger.debug("Assignment details saved")
      
        // Change started on 05/31/2017
        // Below code Commented and assignmentDetailsQuery added to make a single query to check performance improvements
        /* 
        // aggregating cases , sel qty , scr qty at assignment level , filtering non merged assignments
        var assignmentDetailsAgg = sqlContext.sql("select sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_nbr  ,sd.assgn_nbr ,sum(sd.bill_qty) as sum_bill_qty , sum(sd.sel_qty)  as sum_sel_qty,  sum(sd.cube) as sum_cube_qty ,sum(sd.sel_cube) as sum_sel_cube_qty , sum(sd.scr_qty) as sum_scr_qty , min(sd.assgn_start_time) as assgn_start_time , min(sd.assgn_end_time) as assgn_end_time from shipping_data_by_ops_date sd where merged_status ='N' group by sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_nbr,sd.assgn_nbr ").cache()
        assignmentDetailsAgg.registerTempTable("aggignment_sum")
        sqlContext.cacheTable("aggignment_sum")   // Added on 05/25/2017

        // calculating  selected pct , actual pallets and expected pallets at assignment level
        var assignmentDetails = sqlContext.sql("select sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_nbr as stop_number ,sd.assgn_nbr as assignment_id , sd.sum_sel_qty as cases_selected ,sd.sum_bill_qty,sd.sum_sel_qty,sd.sum_scr_qty ,floor(sd.sum_bill_qty/70) as expected_pallets, floor(sd.sum_sel_qty/70)  as actual_pallets,case when sd.sum_bill_qty > 0 then  floor( ( ( sd.sum_sel_qty + sd.sum_scr_qty ) / sd.sum_bill_qty )*100 ) else 0 end  as selected_pct , sd.sum_cube_qty as number_of_cubes ,sd.sum_sel_cube_qty , sd.assgn_start_time,sd.assgn_end_time from aggignment_sum sd ").cache()
        //assignmentDetails.show()
        assignmentDetails.registerTempTable("assignment_details")
        sqlContext.cacheTable("assignment_details")  // Added on 05/31/2017
        */
        
      /* 
       *                                      ,dispatch_timestamp
                                              ,closed_timestamp
                                              ,ext_timestamp
                                              ,cust_nbr
                                              ,customer_name
                                              ,update_time
                                              ,customer_number
                                              ,door_number
                                              ,shift_id
                                              ,entry_date
                                              ,trailer_type
                                              ,dispatch_time
                                              ,closed_time
       */
        val assignmentDetailsQuery = """ WITH base_rows AS 
                                               ( select sd.warehouse_id , sd.ops_date, sd.load_number, sd.stop_nbr stop_number, sd.assgn_nbr assignment_id
                                                       ,sd.dispatch_timestamp, sd.closed_timestamp, sd.ext_timestamp, sd.cust_nbr, sd.customer_name, sd.update_time
                                                       ,sd.customer_number, sd.door_number, sd.shift_id, sd.ent_date as entry_date, sd.trailer_type, sd.dispatch_time, sd.closed_time  
                                                       ,sum(sd.bill_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as sum_bill_qty 
                                                       ,sum(sd.sel_qty)  OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as sum_sel_qty
                                                       ,sum(sd.cube) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as sum_cube_qty 
                                                       ,sum(sd.sel_cube) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as sum_sel_cube_qty 
                                                       ,sum(sd.scr_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as sum_scr_qty 
                                                       ,min(sd.assgn_start_time) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as assgn_start_time 
                                                       ,min(sd.assgn_end_time) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_nbr, sd.assgn_nbr) 
                                                              as assgn_end_time 
                                                   from shipping_data_by_ops_date sd 
                                                  where merged_status ='N' 
                                                  ) 
                                        SELECT warehouse_id, ops_date, load_number, stop_number, assignment_id
                                              ,sum_bill_qty
                                              ,sum_sel_qty
                                              ,sum_scr_qty
                                              ,sum_sel_qty cases_selected
                                              ,sum_cube_qty number_of_cubes
                                              ,sum_sel_cube_qty
                                              ,floor( sum_cube_qty/ 70 ) as expected_pallets
                                              ,floor( sum_sel_cube_qty/ 70) as actual_pallets
                                              ,case when sum_bill_qty > 0 then floor( ( ( sum_sel_qty + sum_scr_qty ) 
                                                                                       / sum_bill_qty 
                                                                                      ) * 100 ) 
                                                    else 0 
                                                 end as selected_pct 
                                              ,assgn_start_time
                                              ,assgn_end_time
                                          FROM base_rows
                                     """
        logger.debug( "Assignment Details Query - " + assignmentDetailsQuery )
        var assignmentDetails = sqlContext.sql(assignmentDetailsQuery)
        assignmentDetails.persist(StorageLevel.MEMORY_AND_DISK_SER)  // Added on 05/31/2017
        assignmentDetails.registerTempTable("assignment_details")
        sqlContext.cacheTable("assignment_details")
        assignmentDetails.show(40)  
        // Above code Commented and assignmentDetailsQuery added to make a single query to check performance improvements        
        // Change ended on 05/31/2017
        
        // Modification started on 04/25/2017 to take Assignments history
        //val assignmentDetailsSave = assignmentDetails.select("warehouse_id", "ops_date", "load_number", "stop_number", "assignment_id", "cases_selected", "selected_pct", "number_of_cubes", "assgn_start_time", "assgn_end_time")
        //saveDataFrame(assignmentDetailsSave, "wip_shipping_prs", "assignment_details_by_warehouse")  //Not required for Testing Pushkar
       // Modification started on 04/25/2017 to take Assignments history
        
        // Added on 04/25/2017 to take Assignments history
      /* try {
           val assignmentDetailsSaveHistory = assignmentDetailsSave.withColumn("snapshot_time", lit(currrentTime)).cache()
           saveDataFrame(assignmentDetailsSaveHistory, "wip_shipping_hist", "assignment_details_by_warehouse_history")

      }catch{
        case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
          logger.error("Error while executing takin Assignments history for assignmentDetails at : " +  e.getMessage + " " + e.printStackTrace() )
      }*/  //Not required for Testing Pushkar
      // Ended on 04/25/2017 to take Assignments history
      
        logger.debug("Assignment Aggregation details saved")
        
        //### Stop Details Started        
        // Change started on 05/31/2017
        // Below code Commented and stopDetailsQuery added to make a single query to check performance improvements     
        // Also unnecessary join with shipping_data_by_ops_date in stopDetailsWithCustDetails is avoided 
        /*
        // aggregating cases , sel qty , scr qty at stop level
        var stopDetails = sqlContext.sql("select distinct sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_number ,sum(sd.sum_bill_qty) as sum_bill_qty ,sum(sd.sum_sel_qty) as sum_sel_qty , sum(sd.cases_selected) as cases_selected ,sum(sd.sum_sel_cube_qty) as sum_sel_cube_qty,sum(sd.number_of_cubes) as sum_cube_qty , sum(sd.sum_scr_qty) as sum_scr_qty  from assignment_details sd group by sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_number ").cache()
        stopDetails.registerTempTable("stop_details")
        sqlContext.cacheTable("stop_details")   // Added on 05/25/2017
        //stopDetails.show()
        //logger.debug("stop details count" + stopDetails.count())
        // mapping customer details against stop data
        var stopDetailsWithCustDetails = sqlContext.sql("select distinct sdt.* ,sd.dispatch_timestamp, sd.closed_timestamp, sd.ext_timestamp, sd.cust_nbr , sd.customer_name ,sd.update_time from  shipping_data_by_ops_date sd , stop_details sdt where  sdt.warehouse_id = sd.warehouse_id and sdt.ops_date = sd.ops_date and sdt.load_number = sd.load_number and sdt.stop_number =sd.stop_nbr").cache()
        stopDetailsWithCustDetails.registerTempTable("stop_details_with_cust")
        sqlContext.cacheTable("stop_details_with_cust")   // Added on 05/25/2017
        
        logger.debug("stop details aggregated ")
        // calculating  selected pct , actual pallets and expected pallets at stop level
        var stopWithPartySiteDetails = sqlContext.sql("select distinct  sd.warehouse_id , sd.ops_date, sd.load_number ,sd.stop_number ,sd.stop_number as store_sequence,sd.cases_selected , round(sd.sum_cube_qty/70,0)  as expected_pallets, "
          + " round(sd.sum_sel_cube_qty/70,0) as actual_pallets ,case when sd.sum_bill_qty > 0 then floor(( ( sd.sum_sel_qty + sd.sum_scr_qty ) / sd.sum_bill_qty) * 100) else 0 end as  selected_pct , sd.sum_sel_cube_qty , sd.sum_cube_qty , sd.sum_sel_qty ,sd.sum_bill_qty,sd.sum_scr_qty ,ar.site_use_location as store_number ,ar.party_site_name as store_name ,sd.cust_nbr as cs_customer_number , "
          + " sd.customer_name as cs_customer_name,ar.address1 as store_address,ar.city as store_city ,ar.state as store_state,ar.postal_code as store_zip ,sd.dispatch_timestamp, sd.closed_timestamp, sd.ext_timestamp,  sd.update_time from  stop_details_with_cust sd left outer join ar_party_site ar on sd.cust_nbr = ar.party_site_number ").cache()
        //stopWithPartySiteDetails.show()
        stopWithPartySiteDetails.registerTempTable("stop_details_agg")
        sqlContext.cacheTable("stop_details_agg")   // Added on 05/25/2017
        */
        /*
         *                                      ,dispatch_timestamp
                                                ,closed_timestamp
                                                ,ext_timestamp
                                                ,cust_nbr
                                                ,customer_name
                                                ,update_time
                                                ,customer_number
                                                ,door_number
                                                ,shift_id
                                                ,entry_date
                                                ,trailer_type
                                                ,dispatch_time
                                                ,closed_time
         */
        val stopDetailsQuery = """ WITH base_rows AS 
                                        ( SELECT DISTINCT sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number 
                                                ,sum(sd.sum_bill_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number )
                                                      as sum_bill_qty 
                                                ,sum(sd.sum_sel_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number )
                                                      as sum_sel_qty
                                                ,sum(sd.cases_selected) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number )
                                                       as cases_selected 
                                                ,sum(sd.sum_sel_cube_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number )
                                                       as sum_sel_cube_qty
                                                ,sum(sd.number_of_cubes) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number )
                                                       as sum_cube_qty 
                                                ,sum(sd.sum_scr_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number )
                                                       as sum_scr_qty
                                           FROM assignment_details sd 
                                        )
                                 SELECT DISTINCT sd.warehouse_id, sd.ops_date, sd.load_number, sd.stop_number, sd.stop_number as store_sequence
                                       ,sd.cases_selected 
                                       ,round( sd.sum_cube_qty/ 70 , 0 )  as expected_pallets
                                       ,round( sd.sum_sel_cube_qty/ 70, 0 ) as actual_pallets 
                                       ,case when sd.sum_bill_qty > 0 then floor( ( ( sd.sum_sel_qty + sd.sum_scr_qty ) / sd.sum_bill_qty ) * 100)
                                             else 0 
                                         end as selected_pct 
                                       ,sd.sum_sel_cube_qty 
                                       ,sd.sum_cube_qty
                                       ,sd.sum_bill_qty 
                                       ,sd.sum_sel_qty 
                                       ,sd.sum_scr_qty 
                                       ,ar.site_use_location as store_number 
                                       ,ar.party_site_name as store_name 
                                       ,sdt.cust_nbr as cs_customer_number 
                                       ,sdt.customer_name as cs_customer_name
                                       ,ar.address1 as store_address
                                       ,ar.city as store_city 
                                       ,ar.state as store_state
                                       ,ar.postal_code as store_zip 
                                       ,sdt.dispatch_timestamp
                                       ,sdt.closed_timestamp
                                       ,sdt.ext_timestamp
                                       ,sdt.update_time 
                                       ,sdt.closed_time
                                       ,sdt.dispatch_time
                                   from base_rows sd 
                                   JOIN shipping_data_by_ops_date sdt
                                        ON sdt.warehouse_id = sd.warehouse_id
                                           and sdt.ops_date = sd.ops_date 
                                           and sdt.load_number = sd.load_number
                                           and sdt.stop_nbr = sd.stop_number 
                                   left outer join ar_party_site ar 
                                     on cust_nbr = ar.party_site_number
                              """
        logger.debug( "Stop Details Query - " + stopDetailsQuery )
        var stopWithPartySiteDetails = sqlContext.sql( stopDetailsQuery )
        stopWithPartySiteDetails.persist(StorageLevel.MEMORY_AND_DISK_SER)  // Added on 05/31/2017
        stopWithPartySiteDetails.registerTempTable("stop_details_agg")
        sqlContext.cacheTable("stop_details_agg")
        stopWithPartySiteDetails.show(40)
        // Above code Commented and stopDetailsQuery added to make a single query to check performance improvements     
        // Also unnecessary join with shipping_data_by_ops_date in stopDetailsWithCustDetails is avoided        
        // Change ended on 05/31/2017
        
        //saveDataFrame(stopWithPartySiteDetails.select("warehouse_id", "ops_date", "load_number", "stop_number", "store_sequence", "cases_selected", "expected_pallets", "actual_pallets", "selected_pct", "store_number", "store_name", "cs_customer_number", "cs_customer_name", "store_address", "store_city", "store_state", "store_zip", "update_time"), "wip_shipping_prs", "stop_details_by_warehouse") //Not required for Testing Pushkar
        logger.debug("Stop details saved ")
        
        //### Load Details Started      
        // Change started on 05/31/2017
        // Below code Commented and loadDetailsQuery added to make a single query to check performance improvements     
        // Also unnecessary join with shipping_data_by_ops_date in shippingByLoadNumber is avoided         
        /*
        // aggregating cases , sel qty , scr qty at Load level
        var loadsPalletsDetais = sqlContext.sql("select sd.warehouse_id , sd.ops_date, sd.load_number ,count(distinct sd.stop_number ) as number_of_stops, collect_set(sd.cs_customer_number) as store_stops, sum(sd.cases_selected) as total_cases, sum(sd.expected_pallets)  as expected_pallets, sum(sd.actual_pallets) as actual_pallets, floor( ( ( sum(sd.sum_sel_qty)  + sum(sd.sum_scr_qty) )/sum(sd.sum_bill_qty) ) * 100 ) as selected_pct "
          + " ,sum(sd.sum_cube_qty) as cube_qty, min(dispatch_timestamp) as min_dispatch_timestamp ,max(dispatch_timestamp) as max_dispatch_timestamp  from stop_details_agg sd group by sd.warehouse_id , sd.ops_date, sd.load_number ").cache()
        loadsPalletsDetais.registerTempTable("current_load_details")
        
        // calculates set back time
        var loadsWithDispatchTimesMerged = sqlContext.sql(" select cl.* , case when ed.dispatch_timestamp is null then cl.min_dispatch_timestamp else ed.dispatch_timestamp end as dispatch_timestamp , case when ed.dispatch_timestamp is null then getSetBackTime(cl.max_dispatch_timestamp ,cl.min_dispatch_timestamp) else getSetBackTime(cl.max_dispatch_timestamp ,ed.dispatch_timestamp) end as set_back_time   from current_load_details cl  left outer join existing_load_details ed on cl.warehouse_id = ed.warehouse_id and cl.ops_date =ed.ops_date and cl.load_number =  ed.load_number ")
        loadsWithDispatchTimesMerged.registerTempTable("current_load_details_with_setbacktimes")
        //loadsWithDispatchTimesMerged.show()

        // calculates gate status
        var shippingByLoadNumber = sqlContext.sql("select sl.* ,sd.customer_number,sd.door_number ,  sd.shift_id ,sd.ent_date as entry_date, sd.customer_name ,sd.trailer_type ,sd.dispatch_time ,sd.closed_time ,case when sl.set_back_time is null then  'OT'  else 'LT' end as gate_status  , sl.set_back_time as set_back_timestamp , sd.closed_timestamp, sd.update_time ,sd.ext_timestamp from  shipping_data_by_ops_date sd , "
          + " current_load_details_with_setbacktimes sl where  sd.warehouse_id = sl.warehouse_id and sd.ops_date = sl.ops_date and sd.load_number = sl.load_number   order by  sd.warehouse_id , sd.ops_date, sd.load_number  ").cache()
        //shippingByLoadNumber.show()
        shippingByLoadNumber.registerTempTable("shipping_loads")
        */
        
        //case when sl.set_back_time is null then  'OT'  else 'LT' end as gate_status 
        //count(distinct sd.stop_number ) as number_of_stops
        /*
         *                                         ,door_number
                                                   ,shift_id
                                                   ,entry_date
                                                   ,trailer_type
         */
        val loadDetailsQuery = s""" WITH base_rows AS  
                                           ( SELECT DISTINCT sd.warehouse_id, sd.ops_date, sd.load_number
                                                   ,collect_set(sd.cs_customer_number) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                                  as store_stops
                                                   ,sum(sd.cases_selected) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                                  as total_cases
                                                   ,sum(sd.sum_cube_qty) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                                   as cube_qty
                                                   ,sum(sd.expected_pallets) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                                  as expected_pallets
                                                   ,sum(sd.actual_pallets) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                                  as actual_pallets
                                                   ,case when sum_bill_qty > 0 then floor( ( ( sd.sum_sel_qty + sd.sum_scr_qty ) / sd.sum_bill_qty ) * 100)
                                                         else 0 
                                                     end as selected_pct 
                                                   ,min(dispatch_timestamp) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                         as min_dispatch_timestamp 
                                                   ,max(dispatch_timestamp) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                         as max_dispatch_timestamp 
                                                   ,min(dispatch_time) OVER ( PARTITION BY sd.warehouse_id, sd.ops_date, sd.load_number )
                                                         as min_dispatch_time
                                                   ,closed_timestamp
                                                   ,ext_timestamp
                                                   ,update_time 
                                                   ,cs_customer_name customer_name
                                                   ,closed_time
                                               FROM stop_details_agg sd 
                                            )
                                      SELECT DISTINCT cl.* 
                                            ,sdt.door_number
                                            ,sdt.shift_id
                                            ,sdt.ent_date entry_date
                                            ,sdt.trailer_type
                                            ,sdt.customer_number
                                            ,CASE WHEN ed.dispatch_timestamp is null then cl.min_dispatch_timestamp 
                                                  ELSE ed.dispatch_timestamp 
                                              END as dispatch_timestamp 
                                            ,CASE WHEN ed.dispatch_time is null then cl.min_dispatch_time 
                                                  ELSE ed.dispatch_time 
                                              END as dispatch_time 
                                            ,CASE WHEN ed.dispatch_timestamp is null then getSetBackTime(cl.max_dispatch_timestamp, cl.min_dispatch_timestamp) 
                                                  ELSE getSetBackTime(cl.max_dispatch_timestamp, ed.dispatch_timestamp) 
                                              END as set_back_timestamp   
                                        FROM base_rows cl  
                                        JOIN shipping_data_by_ops_date sdt
                                            ON sdt.warehouse_id = cl.warehouse_id
                                               and sdt.ops_date = cl.ops_date 
                                               and sdt.load_number = cl.load_number
                                        LEFT OUTER JOIN existing_load_details ed 
                                          ON cl.warehouse_id = ed.warehouse_id 
                                            AND cl.ops_date =ed.ops_date 
                                            AND cl.load_number =  ed.load_number
                                """        
        logger.debug( "Load Details Query - " + loadDetailsQuery )
         var shippingByLoadNumber = sqlContext.sql(loadDetailsQuery)
         shippingByLoadNumber.persist()
         shippingByLoadNumber.registerTempTable("shipping_loads")
         sqlContext.cacheTable("shipping_loads")
         shippingByLoadNumber.show(40)
        // Above code Commented and loadDetailsQuery added to make a single query to check performance improvements     
        // Also unnecessary join with shipping_data_by_ops_date in shippingByLoadNumber is avoided        
        // Change ended on 05/31/2017
        
        // Added on 05/31/2017
        // Calculate multiple Stop Numbers
        var shippingDataStopCounts = sqlContext.sql(""" SELECT sd.warehouse_id, sd.ops_date, sd.load_number
                                                              ,count(distinct sd.stop_number ) as number_of_stops
                                                          FROM stop_details_agg sd 
                                                      GROUP BY sd.warehouse_id , sd.ops_date, sd.load_number 
                                                    """
                                                    )
        shippingDataStopCounts.registerTempTable("shipping_stop_counts")
        sqlContext.cacheTable("shipping_stop_counts")
        shippingDataStopCounts.show(40)
        // Ended on 05/31/2017
   
        // calculate multiple customer loads
        var shippingDataLoadCounts = sqlContext.sql("select sl.warehouse_id,sl.ops_date,sl.load_number,count(distinct sl.customer_number) as customer_count from shipping_loads sl group by sl.warehouse_id,sl.ops_date,sl.load_number ")
        shippingDataLoadCounts.registerTempTable("shipping_load_counts")
        sqlContext.cacheTable("shipping_load_counts")
        shippingDataLoadCounts.show(40)
        
        // calculating  selected pct , actual pallets and expected pallets at stop level
        // adding ext time stamp to the load details -03/09/2017
        var shippingByLoads = sqlContext.sql(""" select DISTINCT sd.warehouse_id, sd.ops_date, sd.load_number, ssc.number_of_stops
                                                        ,sd.actual_pallets, sd.closed_time, sd.closed_timestamp, sd.cube_qty
                                                        ,case when sc.customer_count > 1 then -99 else sd.customer_number end as customer_number 
                                                        ,case when sc.customer_count > 1 then 'Multiple' else sd.customer_name end as customer_name
                                                        ,sd.dispatch_time, sd.dispatch_timestamp, sd.door_number, sd.entry_date, sd.expected_pallets
                                                        ,sd.ext_timestamp
                                                        ,case when sd.set_back_timestamp is null then  'OT' else 'LT' end as gate_status  
                                                        ,selected_pct, sd.set_back_timestamp set_back_time, sd.set_back_timestamp
                                                        ,sd.shift_id, sd.store_stops, sd.total_cases, sd.trailer_type
                                                        ,sd.update_time 
                                                   from shipping_loads sd 
                                                       ,shipping_load_counts sc 
                                                       ,shipping_stop_counts ssc
                                                  where sd.warehouse_id = sc.warehouse_id 
                                                    and sd.ops_date = sc.ops_date 
                                                    and sd.load_number= sc.load_number 
                                                    AND sd.warehouse_id = ssc.warehouse_id 
                                                    and sd.ops_date = ssc.ops_date 
                                                    and sd.load_number= ssc.load_number
                                              """
                                             ).cache()
        //logger.debug("job completed at before saving " + currentRunTime)
        //shippingByLoads.show()
        shippingByLoads.persist(StorageLevel.MEMORY_AND_DISK_SER) // Added on 05/25/2017
        shippingByLoads.show(40)
        //saveDataFrame(shippingByLoads, "wip_shipping_prs", "load_details_by_warehouse")  //Not required for Testing Pushkar
        
        //logger.debug("job completed at after saving " + currentRunTime)
        logger.debug("Load details saved ")
        
        return "success";

      } catch {
        case e: Exception => {

          logger.error("Error while processisng Load Stop Assignment for   " + filename + "   at : " + currentRunTime + e)
          throw new Exception("Error while processisng Load Stop Assignment for   " + filename + "   at : " + currentRunTime + e.printStackTrace())
        }

      }
    }
    // Cassandra data type mapping against scala types
    def getCassandraTypeMapping(columnType: String): DataType = {
      columnType match {
        case "int"       => return IntegerType
        case "double"    => return DoubleType
        case "long"      => return LongType
        case "float"     => return FloatType
        case "byte"      => return ByteType
        case "string"    => return StringType
        case "date"      => return TimestampType
        case "timestamp" => return StringType
        case "uuid"      => return StringType
        case "decimal"   => return DoubleType
        case "boolean"   => BooleanType
        case "counter"   => IntegerType
        case "bigint"    => IntegerType
        case "text"      => return StringType
        case "ascii"     => return StringType
        case "varchar"   => return StringType
        case "varint"    => return IntegerType
        case default     => return StringType
      }
    }
    
    def updateStatus(sc: SparkContext, sqlContext: SQLContext, status: String, fileName: String)  {
        logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                   updateStatus                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
        //val previousAssignmentsMerged =  sqlContext.sql("select  pa.warehouse_id , pa.ops_date , pa.load_number , pa.stop_number , pa.assignment_id , CASE WHEN ca.ext_timestamp <= '" + currentShippingExtTime + "' THEN 'Y' ELSE 'N' END as merged_status from  current_assignmens_deleted ca , previous_assignments pa where ca.invoice_site = pa.warehouse_id and ca.ops_date = pa.ops_date and ca.assgn_nbr =  pa.assignment_id ") // 'Y' is replaced with CASE Statement on 04/28/2017  //pa.ext_timestamp changed to currentShippingExtTime on 05/01/2017  
        //val currentAssignmentsMerged =  sqlContext.sql("select   pa.warehouse_id , pa.ops_date , pa.load_number , pa.stop_number , pa.assignment_id , CASE WHEN ca.ext_timestamp <= '" + currentShippingExtTime + "' THEN 'Y' ELSE 'N' END as merged_status  from current_assignmens_deleted ca , current_assignments pa where ca.invoice_site = pa.warehouse_id and ca.ops_date = pa.ops_date and ca.assgn_nbr =  pa.assignment_id ")  // 'Y' is replaced with CASE Statement on 04/28/2017  //pa.ext_timestamp changed to currentShippingExtTime on 05/01/2017
        
        //saveDataFrame(currentAssignmentsMerged, "wip_shipping_prs", "assignment_details_by_warehouse")
        //saveDataFrame(previousAssignmentsMerged, "wip_shipping_prs", "assignment_details_by_warehouse_prev")
        logger.debug("merged assignments status updated") 
         
    }
  }
}

