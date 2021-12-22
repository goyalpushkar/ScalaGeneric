package com.cswg.fileProcess

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.slf4j.MDC

import java.io.FileNotFoundException
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ StructType,StructField,IntegerType,StringType,DoubleType,LongType,FloatType,ByteType,TimestampType,BooleanType,DataType,ArrayType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.runtime.RichInt

import scala.collection.mutable.{ListBuffer => MListBuffer, ArrayBuffer => MArrayBuffer, Map => MMap, Seq => MSeq, LinkedHashMap => MLinkedHashMap}
import scala.collection.JavaConverters._

import com.datastax.driver.core.utils.UUIDs

import org.apache.spark.sql.functions.monotonicallyIncreasingId

import scala.util.parsing.json._
import scala.util.parsing.json.JSON
import scala.io.Source
import scala.xml._

import java.io.File
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.{Calendar, UUID, Date, Properties}

import com.datastax.spark.connector.cql.CassandraConnector
//import com.databricks.spark.xml._

class FileProcessing {
  
}

object FileProcessing {
  var logger = LoggerFactory.getLogger(this.getClass);    //"Examples"
  logger.debug(" - FileProcessing Started - ")
     
  var thisJobWarehouses = ""
  val currentRunTime = getCurrentTime()
  var jobId = 1
  var fileId = 1
  var sc: SparkContext = null 
  var conn: CassandraConnector = null
  var loggingEnabled = "N"
  var processFileName: String = ""
  
  val fileControllerSchema = StructType(Array(
                                              StructField("job_id", IntegerType, false),
                                              //StructField("file_name", StringType, false),
                                              StructField("file_id", IntegerType, false),                                              
                                              StructField("file_schema", StringType, true)
                                              )
                                        )
                                        
  val fileStatusSchema = StructType(Array(
                                              StructField("file_id", IntegerType, true),
                                              StructField("file_name", StringType, true),
                                              StructField("snapshot_id", StringType, true),
                                              StructField("no_of_rows_processed", IntegerType, true),
                                              StructField("rejected_rows", IntegerType, true),
                                              StructField("total_rows", IntegerType, true),
                                              StructField("validation_status_norm", StringType, true),
                                              StructField("error", StringType, true),
                                              StructField("ext_timestamp", TimestampType, true)
                                              )
                                        )
                                        
  // 0 - environment
  // 1 - Job Id 
  // 2 - File Id
  // 3 - ConfFilePath
  // 4 - Logging Enabled
  // 5 - File Name
  def main(args: Array[String]) {
    // logger file name 
    MDC.put("fileName", "M1999_FileProcessing_" + args(2).toString() )  //Jobid added on 09/08/2017
    
    val environment = if ( args(0) == null ) "dev" else args(0).toString 
    jobId = if (args.length > 0) args(1).toInt else 0
    fileId = if (args.length > 0) args(2).toInt else 0
    confFilePath = if (args.length > 0) args(3).toString else "/opt/cassandra/applicationstack/applications/wip/resources/"
    loggingEnabled = if (args.length > 0) args(4).toString else "N"
    processFileName = if (args.length > 0) args(5).toString else ""
    
    logger.info( " - environment - " + environment + "\n"
              + " - Job Id - " + jobId + "\n"
              + " - File Id - " + fileId + "\n"
              + " - confFilePath - " + confFilePath + "\n" 
              + " - loggingEnabled - " + loggingEnabled + "\n"
              + " - processFileName - " + processFileName
             )
     
    //val (hostName, userName, password) = getCassandraConnectionProperties(environment)
    //logger.info( " Host Details got" )
        
    val conf = new SparkConf()
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .set("spark.cassandra.input.fetch.size_in_rows", "10000")
      .set("spark.cassandra.input.split.size_in_mb", "100000")
      .set("textinputformat.record.delimiter", "\n")
      //.set("spark.eventLog.enabled", "false")  commented on 09/08/2017
      .setAppName("M1999_FileProcessing_" + args(2).toString())
      /*.set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]")*/     
    //.set("spark.cassandra.output.ignoreNulls", "true")
    
    conn = CassandraConnector(conf)     
    sc = new SparkContext(conf)
    val sqlContext: SQLContext = new HiveContext(sc)
    import sqlContext.implicits._
    
    sc.broadcast(jobId)
    sc.broadcast(fileId)
    sc.broadcast(processFileName)
    
    var callStatus = ""
    
    try {
            logger.debug( "jobId - " + jobId + "\n" 
                        + "fileId - " + fileId + "\n"
                        + "currentRunTime - " + currentRunTime + "\n"
                       )
           
            val dfWarehouseOperationMaster = sqlContext.read
                .format("org.apache.spark.sql.cassandra")
                .options( Map("table" -> "warehouse_operation_time", "keyspace" -> "wip_shipping_ing") )
                .load();
            //operationalMasterData.show()
              
            val dfFileController = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "file_ingestion_controller", "keyspace" -> "wip_configurations") )
            .load()
            .filter(s" file_id = $fileId and job_id = $jobId" );  
            
            val dfFileTracker = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "file_streaming_tracker", "keyspace" -> "wip_configurations") )
            .load()
            .filter(s" file_id = $fileId "); 
            
            dfDataTypeMapping = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "cassandra_datatype_mapping", "keyspace" -> "wip_configurations") )
            .load() 
            
            dfWarehouseOperationMaster.registerTempTable("warehouse_operation_time")
            dfFileController.registerTempTable("file_ingestion_controller")
            dfFileTracker.registerTempTable("file_streaming_tracker")
            dfDataTypeMapping.registerTempTable("cassandra_datatype_mapping")
            logger.debug( "Register Temp Tables" )
               
            if (dfFileController.rdd.isEmpty) {
                logger.debug(" No file schema is defined for Job id and File id combination " + currentRunTime)
                return
            }
            
            callStatus = 	fileProcessing(sqlContext, dfFileController)  //sc,  , conn
            logger.debug( "Status returned by fileProcessing - " + callStatus  )

            //pivotalAttributes(sqlContext, "item_flex_attr", "item_attr_details" )
            //pivotalAttributes(sqlContext, "item_attrgroupmany", "item_attrgroupmany_details" )
             
            //saveDataFrame(fileStatus DF.withColumn("update_time", lit(ShippingUtil.getCurrentTime())), "wip_configurations", "file_streaming_tracker")
            logger.debug("CSSparkJobStatus:Completed" )
            sqlContext.clearCache()

    } catch {
        case e @ ( _ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("Error in main at : " + e.getMessage + ": " + e.printStackTrace())
            //throw new Exception("Error in main at : " + e.getMessage + ": " + e.printStackTrace() )   
    }
    logger.debug("Stop Context")
    sc.stop()   
  }
      
    def fileProcessing( sqlContext: SQLContext, dfFileController: DataFrame): String = {
         //sc: SparkContext, , cassandraConnector: CassandraConnector
        logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                 fileProcessing                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      var status = "0"
      var callStatus = "0"
  
      try{                 
           //, 'Partial_Complete'
           val currentFileQuery = s""" WITH base_rows AS 
                                          ( SELECT ft.file_id, ft.file_name
                                                  ,ft.snapshot_id, ft.creation_time
                                                  ,fc.file_type
                                                  ,MAX( ft.creation_time ) OVER ( PARTITION BY ft.file_id ) max_creation_time
                                              FROM file_streaming_tracker ft
                                                  ,file_ingestion_controller fc
                                             WHERE ft.file_id = fc.file_id 
                                               AND ft.validation_status_norm IN ('Inserted', 'Failed', 'Ingested')
                                         ) 
                                         SELECT file_id, file_name, snapshot_id
                                               ,creation_time, file_type, max_creation_time
                                           FROM base_rows 
                                          WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )
                                        ORDER BY creation_time
                                        """
           //WHERE max_creation_time = creation_time
           //WHERE file_name = '1WS_CS_CIN.cin.LJ1NM5200000707.1000020473835'
           
            val currentFile = sqlContext.sql(currentFileQuery).persist()
            currentFile.show( currentFile.count().toInt )
            logger.debug("Current File extracted" )
            
            if (currentFile.rdd.isEmpty) {
                logger.debug(" No files to process are in waiting status  at " + currentRunTime)
                return status
            }
            
            val allFiles = currentFile.collect()
            for ( file <- allFiles )
            {
              if ( file.getString(4).equalsIgnoreCase("csv") || file.getString(4).equalsIgnoreCase("txt") )
                  callStatus = processCSVFile( sqlContext, dfFileController, file.getString(1), (if (file.get(2) == null ) null else file.getString(2)), (if (file.get(3) == null ) null else file.getTimestamp(3).toString() ) )  //sc,  
              else if ( file.getString(4).equalsIgnoreCase("json") )
                  callStatus = processingJSONFile( sqlContext, dfFileController, file.getString(1), (if (file.get(2) == null ) null else file.getString(2)), (if (file.get(3) == null ) null else file.getTimestamp(3).toString() ) )  //sc, 
              else if ( file.getString(4).equalsIgnoreCase("xml") )
                  callStatus = processingXMLFile( sqlContext, dfFileController, file.getString(1), (if (file.get(2) == null ) null else file.getString(2)), (if (file.get(3) == null ) null else file.getTimestamp(3).toString() ) )  //sc, 
              
              if ( status != "1" )
                status = callStatus
            }
            /* currentFile.foreach { x => logger.debug("Processing Started for - " + x.getString(1) )
                                       if ( x.getString(4).equalsIgnoreCase("csv") || x.getString(4).equalsIgnoreCase("txt") )
                                            processCSVFile( sqlContext, dfFileController, x.getString(1), x.getString(2), x.getTimestamp(3).toString() )  //sc,  
                                        else if ( x.getString(4).equalsIgnoreCase("json") )
                                            processingJSONFile( sqlContext, dfFileController, x.getString(1), x.getString(2), x.getTimestamp(3).toString() )  //sc, 
                                        else if ( x.getString(4).equalsIgnoreCase("xml") )
                                            processingXMLFile( sqlContext, dfFileController, x.getString(1), x.getString(2), x.getTimestamp(3).toString() )  //sc, 
                                }*/
           
            /*val fileName = currentFile.first().get(1).toString()
            val snapShotId = currentFile.first().get(2).toString()
            val fileCreationTime = currentFile.first().get(3).toString()
            val fileType = currentFile.first().get(4).toString()
            
             if ( fileType.equalsIgnoreCase("csv") || fileType.equalsIgnoreCase("txt") ) {
                processCSVFile( sc, sqlContext, dfFileController, fileName, snapShotId, fileCreationTime ) 
             }else if ( fileType.equalsIgnoreCase("json") ) {
                processingJSONFile( sc, sqlContext, dfFileController, fileName, snapShotId, fileCreationTime )
             }else if ( fileType.equalsIgnoreCase("xml") ) {
                processingXMLFile( sc, sqlContext, dfFileController, fileName, snapShotId, fileCreationTime, cassandraConnector )
             }*/
      
         }
     catch {
          case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("Error in fileProcessing at : " + e.getMessage + ": " + e.printStackTrace() )
            status = "1"
            //throw new Exception("Error in fileProcessing at : " + e.getMessage + ": " + e.printStackTrace())
         }
      return status
    }
    
   def processCSVFile( sqlContext: SQLContext, dfFileController: DataFrame, fileName: String, snapShotId: String, fileCreationTime: String ): String = {
       //sc: SparkContext, 
        logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                 processCSVFile                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
       var status = "0";
       var validationStatus = "Inserted"
       var errorMessage: String = null
       
       var fileDelimiter = "|" 
       var cffPath = ""
       var fileSchema = "" 
       var trimZeroes = "" 
       var intFieldIndexes = ""
      
       try {
         
           try {
                 fileDelimiter = dfFileController.select("file_delimiter").first().getString(0).trim()  
                 cffPath = dfFileController.select("file_path").first().getString(0)
                 fileSchema = dfFileController.select("file_schema").first().getString(0)
                 
                 //trimZeroes = dfFileController.select("trim_zeroes_columns").first().getString(0)
                 //intFieldIndexes = trimZeroes.split(",").map( x => x.split(":").apply(1) ).mkString(",")
                 //logger.debug(" trimZeroes - "  + trimZeroes + "\n" + "intFieldIndexes - " + intFieldIndexes )                       
              } catch {
                   case e @ (_ :Exception | _ :Error | _ :java.io.IOException | _:java.util.NoSuchElementException ) =>
                     logger.error("Error while gettign file schema at : " + e.getMessage + " "  +  e.printStackTrace() )
                     fileDelimiter = "|"
             }
           val noOfColumns = fileSchema.split(",").size
           val firstField = fileSchema.split(",").apply(0).split(":").apply(0) 
           
           logger.debug("file name => " + fileName + "\n"
                      + "fileSchema => " + fileSchema + "\n"
                      + "filePath => " + cffPath + "/" + fileName + "\n"
                      + "firstField => " + firstField  + "\n"                      
                      + "noOfColumns => " + noOfColumns + "\n"
                      + "FileDelimiter => " + "\\"+ fileDelimiter
                 )
                 
           val fileStruct = StructType(fileSchema.split(",").map(fieldName => StructField(fieldName.split(":")(0)
                                                                                          ,getCassandraTypeMapping(fieldName.split(":")(1))
                                                                                          ,true)
                                                                    )
                                           )
           
           logger.debug( " - fileStruct desclared - " )
           val textFileStream = sc.textFile(cffPath + "/" + fileName)
            
            // validation file schema against expected schema
            // RDD of { comma separated values of row, no of column values}                                  
            val fileRows = textFileStream
                                  .filter { line => !line.contains(firstField) } // Remove Header from the file
                                  .flatMap{ line => line.split("\n") }           // Split lines at new character
                                  .map{ line => ( line.split("\\"+fileDelimiter).mkString(","), line.split("\\"+fileDelimiter).size ) }  
            //fileRows.foreach(println)
            
            //val fileRowsDF = fileRows.toDF("line", "line_size").persist()
            //logger.debug(" - fileRowsDF created - ")
            //fileRowsDF.show()
    
            // filtering records which are matching with schema length
            //val matchedRecords = fileRowsDF.filter( fileRowsDF("line_size") === noOfColumns )
            //val unMatchedRecords = fileRowsDF.filter(shippingDF("line_size") !== columnLength)                                          
            val matchedRecords = fileRows.filter(f => f._2 == noOfColumns )
            val unMatchedRecords = fileRows.filter(f => f._2.!=(noOfColumns) )
            val processedCount = matchedRecords.count().toInt
            val failedCount = unMatchedRecords.count().toInt
            if (failedCount > 0){
               unMatchedRecords.repartition(1).saveAsTextFile(cffPath + "/" + "rejected_" + fileName + "_" + getCurrentTime().getTime)
               errorMessage = "No of column counts in some rows are not same as expected"
            }       
            
            logger.debug(" - matchedRecords and unMatchedRecords - "
                      + " - processedCount - " + processedCount
                      + " - failedCount - " + failedCount )
            //matchedRecords.show()          
    
            // convert file into cassandra data types and create data frame using converted data
            val matchedRows = matchedRecords.repartition(1)  //rdd
                             .map(row => {
                                          var convertedDataList: collection.mutable.Seq[Any] = collection.mutable.Seq.empty[Any]
                                          var index = 0
                                          val colValues = row._1.split(",")  //(0)
                                          colValues.foreach(value => {
                                                                      var returnVal: Any = null
                                                                      /*var actualValue = value
                                                                      // Check if current value index in IntFieldIndexes then convert value to int
                                                                      if ( intFieldIndexes.contains( index.toString() ) )
                                                                      {
                                                                        actualValue = actualValue.toInt.toString()
                                                                      }*/
                                                                      
                                                                      val colType = fileStruct.fields(index).dataType
                                                                      //print(value)
                                                                      colType match {
                                                                        case IntegerType   => returnVal = value.toString.toInt    //value is replaced with actualValue on 07/12/2017
                                                                        case DoubleType    => returnVal = value.toString.toDouble //value is replaced with actualValue on 07/12/2017
                                                                        case LongType      => returnVal = value.toString.toLong   //value is replaced with actualValue on 07/12/2017
                                                                        case FloatType     => returnVal = value.toString.toFloat  //value is replaced with actualValue on 07/12/2017
                                                                        case ByteType      => returnVal = value.toString.toByte   //value is replaced with actualValue on 07/12/2017
                                                                        case StringType    => returnVal = value.toString          //value is replaced with actualValue on 07/12/2017
                                                                        case TimestampType => returnVal = value.toString          //value is replaced with actualValue on 07/12/2017
                                                                      }
                                                                      convertedDataList = convertedDataList :+ returnVal
                                                                      index += 1
                                                                    }
                                                          )
                                          Row.fromSeq(convertedDataList)
                                        }
                                 );
    
            val matchedDF = sqlContext.createDataFrame(matchedRows, fileStruct)
            matchedDF.cache() // Added on 08/22/2017
            
            val finalDF = matchedDF
                                 .withColumn("snapshot_id", lit(snapShotId)) 
                                 .withColumn("creation_time", lit(fileCreationTime))
                                 .withColumn("update_time", lit(currentRunTime))
            finalDF.registerTempTable("text_file_data")
    
            // date formatting  closed , ext and disptch time stamps for the current snapshot data
            val fileDatawithTimeStampQuery = """select case when length(trim(sd.disp_time)) > 0 and sd.disp_time is not null then from_unixtime( unix_timestamp( concat( disp_date || ' ' || disp_time ), 'yyyyMMDdd HH24MISS') , 'yyyyMMDdd HH24MISS' ) else null end  as dispatch_timestamp
                                                      ,case when length(trim(sd.closed_time)) > 0  and sd.closed_time is not null then from_unixtime( unix_timestamp( concat( closed_date || ' ' || closed_time ), 'yyyyMMDdd HH24MISS') , 'yyyyMMDdd HH24MISS' ) else null end as closed_timestamp
                                                      ,case when length(trim(sd.ext_time)) > 0  and sd.ext_time is not null then  from_unixtime( unix_timestamp( concat( ext_date || ' ' || ext_time ), 'yyyyMMDdd HH24MISS') , 'yyyyMMDdd HH24MISS' ) else null end as ext_timestamp  
                                                      ,case when length(trim(sd.assgn_start_date)) > 0 and sd.assgn_start_date <> '00000000' and sd.assgn_start_date is not null then from_unixtime( unix_timestamp( concat( assgn_start_date || ' ' || assgn_start ), 'yyyyMMDdd HH24MISS') , 'yyyyMMDdd HH24MISS' ) else null end  as assgn_start_time 
                                                      ,case when length(trim(sd.assgn_end_date)) > 0  and sd.assgn_end_date <> '00000000'  and sd.assgn_end_date is not null then from_unixtime( unix_timestamp( concat( assgn_end_date || ' ' || assgn_end ), 'yyyyMMDdd HH24MISS') , 'yyyyMMDdd HH24MISS' ) else null end  as assgn_end_time
                                                      ,sd.* 
                                                  from shipping_data sd 
                                            """
            logger.debug( " - fileDatawithTimeStampQuery - " + fileDatawithTimeStampQuery )
            val fileDataWithTimeStamp = sqlContext.sql(fileDatawithTimeStampQuery).persist()
            if ( loggingEnabled.equalsIgnoreCase("Y") ){
                fileDataWithTimeStamp.show()
            }
           
            // return if empty file 
            if (fileDataWithTimeStamp.rdd.isEmpty) {
              logger.debug("Encountered Empty file   " + fileName + " at " + currentRunTime)
              validationStatus = "Failed"
              status = "1"
              errorMessage = "No records in the file"
              //throw new Exception("No records in the File - " + fileName + " at  " + currentRunTime)
            }else{
                if (failedCount > 0) {
                   validationStatus = "Partial_Complete"
                } else {
                   validationStatus = "Fully_Complete"
                }
            }
            
             saveDataFrame(fileDataWithTimeStamp, "wip_shipping_ing", "warehouse_shipping_data_test")
             //saveDataFrame(fileDataWithTimeStamp, "wip_shipping_hist", "warehouse_shipping_data_history")

             val currentExtTime = fileDataWithTimeStamp.first().get(2).toString()
        
             val fileStatusRDD = sc.parallelize( Seq( Row( fileId, fileName, snapShotId, processedCount, failedCount, (processedCount.toInt + failedCount.toInt), validationStatus, errorMessage, currentExtTime) ) ) 
             val fileStatusDF = sqlContext.createDataFrame( fileStatusRDD, fileStatusSchema ).toDF()
             saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_streaming_tracker")
             logger.debug(" file " + fileName + " processed  at " + getCurrentTime())
                                 
       }
       catch {
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("Text file processing error : " + e.getMessage )
            status = "1"
           // throw new Exception("Error while processing data File at : " + e.getMessage + ": " + e.printStackTrace())
       }
       
       return status
   }
   
   
  def processingJSONFile( sqlContext: SQLContext, dfFileController: DataFrame, fileName: String, snapShotId: String, fileCreationTime: String): String = {
     //sc: SparkContext,
    logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                 processingJSONFile                 " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
     var status = "0";
     var validationStatus = "Inserted"
     var errorMessage: String = null

     var cffPath = ""
     var fileSchema = "" 
     var trimZeroes = ""
     var intFieldIndexes = ""
     var explodeArray = ""
     var jsonSchema: StructType = new StructType()
     /*jsonSchema =  jsonSchema.add(StructField( "name"
                                 ,StringType
                                 ,true)
                                     )

     logger.debug(" - Print jsonSchema - " + jsonSchema.length + " = " + jsonSchema.size )
     jsonSchema.foreach { x => println( x.name + " - " + x.dataType) }
     jsonSchema.printTreeString()*/
     
     import sqlContext.implicits._
     
     try {
         
           try {
                 cffPath = dfFileController.select("file_path").first().getString(0)
                 fileSchema = dfFileController.select("file_schema").first().getString(0)
                 explodeArray = dfFileController.select("explode_array").first().getString(0)
               } catch {
                   case e @ (_ :Exception | _ :Error | _ :java.io.IOException | _:java.util.NoSuchElementException ) =>
                     logger.error("Error while gettign file schema at : " + e.getMessage + " "  +  e.printStackTrace() )
           }
               
          val noOfColumns = fileSchema.split(",").size
          val firstField = fileSchema.split(",").apply(0).split(":").apply(0) 
          
          logger.debug("file name => " + fileName + "\n"
                      + "fileSchema => " + fileSchema + "\n"
                      + "filePath => " + cffPath + "/" + fileName + "\n"
                      + "firstField => " + firstField  + "\n"                      
                      + "noOfColumns => " + noOfColumns + "\n"
                      //+ "FileDelimiter => " + "\\"+ fileDelimiter
                 )
               
           val fileStruct = StructType( fileSchema.split(",").map(fieldName => StructField(fieldName.split(":")(0)
                                                                                          ,getCassandraTypeMapping(fieldName.split(":")(1))
                                                                                          ,true)
                                                                 )
                                     )
          logger.debug( " - fileStruct declared - " )
          
          //val jsonFileStream = sqlContext.read.json(cffPath + "/" + fileName)
			    //val jsonFileStream = parser.parse((new FileReader(cffPath + "/" + fileName));
          //val jsonFileStream = Json.parse(new FileReader(cffPath + "/" + fileName).toString())
          
          val jsonFileStream = sqlContext.jsonFile(cffPath + "/" + fileName)
          logger.info("jsonFileStream - " + jsonFileStream);
          jsonFileStream.printSchema()
          if ( loggingEnabled.equalsIgnoreCase("Y") ){
              jsonFileStream.show()
          }
          /*val nameJson = jsonFileStream.map { x => x.getAs[String]("name") }
          nameJson.foreach { println }
          jsonFileStream.registerTempTable("json_raw_data")*/
          
          logger.debug(" - Create Struct Type from Json - ")  
          // Recursive function to get schema from stuct type field
          /*def findFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame, 
              val fieldName = dt.asInstanceOf[StructType].fields
              //var returnStruct: StructField = null
              for ( value <- fieldName ){
                   println( "Name - " + value.name + " Data Type - " + value.dataType )
                   if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     jsonSchema = jsonSchema.add(StructField( parentName +"."+value.name, value.dataType, value.nullable ))
                   }
              }   
              //returnStruct
          }*/
          
          def findFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame, 
              val fieldName = dt.asInstanceOf[StructType].fields
              //var returnStruct: StructField = null
              for ( value <- fieldName ){
                   if ( loggingEnabled.equalsIgnoreCase("Y") ){
                     println( "Name - " + value.name + "\t" + " Data Type - " + value.dataType + "\t" + " Data Type Name - " + value.dataType.typeName )
                   }
                   if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else if ( value.dataType.typeName.equalsIgnoreCase("array") ){
                      if ( explodeArray.equalsIgnoreCase("Y") ){
                         findArrayFields(parentName + "." + value.name , value.dataType)
                      }else{
                         jsonSchema = jsonSchema.add(StructField( parentName + "."+ value.name, value.dataType, value.nullable ))
                      }
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     jsonSchema = jsonSchema.add(StructField( parentName + "."+ value.name, value.dataType, value.nullable ))
                   }
              }   
              //returnStruct
          }
          
          def findArrayFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame,
              val fieldName: ArrayType = dt.asInstanceOf[ArrayType]
              
              fieldName.elementType match {
                case s: StructType => findFields(parentName, fieldName.elementType )
                case s: ArrayType => if ( explodeArray.equalsIgnoreCase("Y") ){
                                        findArrayFields(parentName, fieldName.elementType  )
                                     }else{
                                        jsonSchema = jsonSchema.add(StructField( parentName, fieldName.elementType, true ))
                                      }
                case other => jsonSchema = jsonSchema.add(StructField( parentName, fieldName.elementType, true ))
              }
          }
         

          jsonFileStream.schema.fields.map { x => { 
                                                if ( loggingEnabled.equalsIgnoreCase("Y") ){
                                                   println( "Data Type - " + x.dataType.typeName
                                                      ,"Name - " + x.name
                                                      ,"Default Size - " + x.dataType.defaultSize
                                                      //,"Metadata - " + x.metadata 
                                                      ,"Index - " + jsonFileStream.schema.fieldIndex(x.name)
                                                    )
                                                }
                                                ( if ( x.dataType.typeName.equalsIgnoreCase("struct") ) 
                                                     findFields(x.name, x.dataType)
                                                else
                                                     jsonSchema = jsonSchema.add(StructField( x.name
                                                                                ,x.dataType
                                                                                ,x.nullable) )
                                                )
                                             }
                                      }
          
          logger.debug(" - Print jsonSchema Tree String - ")  
          jsonSchema.printTreeString()
          //logger.debug(" - Print jsonSchema - " + jsonSchema.length + " = " + jsonSchema.size )
          //var colList: String = "$\"animated_debut\", $\"name\", $\"original_voice_actor\""   //""
          //var colLists: List[Column] = List[Column]();
          var colLists: MSeq[Column] = MSeq.empty
          var colListsAlias: MSeq[String] = MSeq.empty
          jsonSchema.map { x => { colLists ++= MSeq(col(x.name) ) 
                                  colListsAlias ++= MSeq(x.name.replace(".", ""))
                                }
                         }
          //val colLists = jsonSchema.map { x => col(x.name) }
          //val colListsAlias = jsonSchema.map { x => x.name.replace(".", "") }
          /*jsonSchema.foreach { x => { println( x.name + " - " + x.dataType) 
                                      colList ++= ( if ( colList == "" ) colList.replace("", "$\""+x.name+"\"") else ",$\""+x.name+"\"" )  //"$\""+ +"\""
                                      colLists ++= c(x.name)
                                    } }*/
          logger.debug( " - colListsAlias - " + colListsAlias )
          colLists.foreach { println }
          val jsonFileStreamExploded = jsonFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => x.alias(y) }: _* )
          //val jsonFileStreamExploded = jsonFileStream.select(colLists.map( c => col(c)):_*)//.alias(colListsAlias.apply( colLists.indexOf(_)   ))
          if ( loggingEnabled.equalsIgnoreCase("Y") ){
            jsonFileStreamExploded.show()
          }
          
          val processedCount = jsonFileStream.count().toInt
          val failedCount = 0 //jsonFileStream.count().toInt
          if (failedCount > 0){
             //unMatchedRecords.repartition(1).saveAsTextFile(cffPath + "/" + "rejected_" + fileName + "_" + getCurrentTime().getTime)
             errorMessage = "No of column counts in some rows are not same as expected"
          }       
          
          logger.debug(" - matchedRecords and unMatchedRecords - "
                    + " - processedCount - " + processedCount
                    + " - failedCount - " + failedCount )
          //matchedRecords.show()                    
          
           val getIdUDF = udf(getId _)
           val jsonFileStreamSave = jsonFileStreamExploded
                                   .select( jsonFileStreamExploded("name"), jsonFileStreamExploded("original_voice_actor"), jsonFileStreamExploded("animated_debut")
                                          ,jsonFileStreamExploded("index_id"),jsonFileStreamExploded("index_index"),jsonFileStreamExploded("index_type")
                                              )
                                   .withColumn("id", generateUUID ())
                                   //.withColumn("id", lit(UUID.randomUUID().toString()) )
          jsonFileStreamSave.show()
          saveDataFrame(jsonFileStreamExploded
                        .select( jsonFileStreamExploded("name"), jsonFileStreamExploded("original_voice_actor"), jsonFileStreamExploded("animated_debut")
                                ,jsonFileStreamExploded("index_id"),jsonFileStreamExploded("index_index"),jsonFileStreamExploded("index_type")
                                //,lit(UUIDs.timeBased().toString()).alias("id")
                                )
                        .withColumn("id", generateUUID ())
                     ,"wip_shipping_ing"
                     ,"json_orig" )
                       
            if (jsonFileStream.rdd.isEmpty() ) {
              logger.debug("Encountered Empty file   " + fileName + " at " + currentRunTime)
              validationStatus = "Failed"
              status = "1"
              errorMessage = "No records in the file"
              //throw new Exception("No records in the File - " + fileName + " at  " + currentRunTime)
            }else{
                if (failedCount > 0) {
                   validationStatus = "Partial_Complete"
                } else {
                   validationStatus = "Fully_Complete"
                }
           }
           
          validationStatus = "Inserted"  // Added for testing Pushkar
          val currentExtTime = null          
  			  val fileStatusRDD = sc.parallelize( Seq( Row( fileId, fileName, snapShotId, processedCount, failedCount, (processedCount.toInt + failedCount.toInt), validationStatus, errorMessage, currentExtTime) ) ) 
          val fileStatusDF = sqlContext.createDataFrame( fileStatusRDD, fileStatusSchema ).toDF()
          //saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_streaming_tracker")
          
          val fileControllerRDD = sc.parallelize( Seq( Row( jobId, fileId, jsonSchema.toString() ) ) )  //fileName
          val fileControllerDF = sqlContext.createDataFrame( fileControllerRDD, fileControllerSchema ).toDF()
          saveDataFrame(fileControllerDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_ingestion_controller")
          logger.debug(" file " + fileName + " processed  at " + getCurrentTime())
           
	   }
		 catch {
		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("JSON file processing error : " + e.getMessage )
            e.printStackTrace()
            status = "1"
            ///throw new Exception("Error while processing json File at : " + e.getMessage + ": " + e.printStackTrace())
			}  
			return status
   }
   
  def processingXMLFile( sqlContext: SQLContext, dfFileController: DataFrame
                        ,fileName: String, snapShotId: String, fileCreationTime: String
                        ): String = {   //sc: SparkContext,   ,cassandraConnector: CassandraConnector 
     logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                 processingXMLFile                  " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
		 val currentTime = getCurrentTime()
     var status = "0";
     var callStatus = "0";
     var validationStatus = "Inserted"
     var concatenatedErrorMessage = ""

     var cffPath = ""
     var fileSchema = "" 
     var explodeArray = ""
     //var explodeColumns: MListBuffer[String] = new MListBuffer[String]()
     //var explodeColumns: List[String] = new java.util.ArrayList[String]()
     var explodeColumns: scala.collection.Map[String, String] = scala.collection.Map[String, String]()
     var trimZeroes = ""
     var intFieldIndexes = ""
     var xmlSchema: StructType = new StructType()
     var xmlSchemaArray: StructType = new StructType()
     var xmlSchemaExplodeArray: StructType = new StructType()
     var xmlSchemaExplod: Seq[String] = Seq[String]()
     
     var itemArrayExploded = 1;
     var arrayElements:MSeq[String] = MSeq.empty
     
     import sqlContext.implicits._
     
     logger.debug("file name => " + fileName + "\n"
                + "snapShotId => " + snapShotId + "\n"
                + "fileCreationTime => " + fileCreationTime 
                 )
                 
     val dfItemDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_doc_id", "keyspace" -> "mdm") )
            .load();  
     
     val dfItemDetailsGtin = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_gtin", "keyspace" -> "mdm") )
            .load();
     
     try {
         
           try {
                 cffPath = dfFileController.select("file_path").first().getString(0)
                 fileSchema = dfFileController.select("file_schema").first().getString(0)
                 explodeArray = dfFileController.select("explode_array").first().getString(0)
                 explodeColumns = dfFileController.select("explode_column").first().getMap[String, String](0)
               } catch {
                   case e @ (_ :Exception | _ :Error | _ :java.io.IOException | _:java.util.NoSuchElementException ) =>
                     logger.error("Error while gettign file schema at : " + e.getMessage + " "  +  e.printStackTrace() )
               }
               
          logger.debug("file name => " + fileName + "\n"
                      + "fileSchema => " + fileSchema + "\n"
                      + "filePath => " + cffPath + "/" + fileName + "\n"
                      + "explodeArray => " + explodeArray + "\n"
                      //+ "explodeColumns => explodeColumns.
                 )
                                  
          val xmlFileStream = sqlContext
                               .read
                               .format("com.databricks.spark.xml")
                               .option("rowTag", "document")  //document  item
                               .option("attributePrefix", "_")
                               .option("valueTag", "_VALUE")
                               .load(cffPath + "/" + fileName)
                               //.filter( "item = '44710044602'" )
                               // jsonFile()

          xmlFileStream.printSchema()
           //xmlFileStream.registerTempTable("item_xml_file")
          //xmlFileStream.show(200)
          
          logger.debug(" - Create Struct Type from XML - ")  
                    
          def loop(path: String, dt: DataType, acc:Seq[String]): Seq[String] = {
              if ( loggingEnabled.equalsIgnoreCase("Y") ){
                logger.debug( " - loop - " + "\t" + " - path - " + path + "\t" + " - dt - " + dt)
              }
              dt match {
              case s: ArrayType =>
                   if ( explodeArray.equalsIgnoreCase("Y") || itemArrayExploded == 1 ){
                      if ( loggingEnabled.equalsIgnoreCase("Y") ){
                        logger.debug( "itemArrayExploded - " + itemArrayExploded + "\t"
                                  + "path - " + path 
                                   ) 
                      }
                      //xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))
                      arrayElements ++= MSeq(path)
                      itemArrayExploded = itemArrayExploded + 1
                      xmlSchemaExplodeArray = xmlSchemaExplodeArray.add( StructField( path, dt, true ))
                      loop(path, s.elementType, acc )
                   }else{
                      if ( loggingEnabled.equalsIgnoreCase("Y") ){
                         logger.debug( "Element exists Array? - " + path + " - " + xmlSchemaArray.fieldNames.contains(path) )
                      }
                      if ( !( xmlSchemaArray.fieldNames.contains(path) ) )  // Added on 02/2/2018
                         xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))
                      if ( path.startsWith("link") ) {
                          //arrayElements ++= MSeq( path )   //.replace(".", "_").replace("__", "_").toLowerCase() 
                          xmlSchemaExplodeArray = xmlSchemaExplodeArray.add( StructField( path, dt, true ))
                          //loop( path, s.elementType, acc )
                      }
                      /*else{
                          xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))  
                      }*/
                   }
              case s: StructType =>    
                  // Added on 03/12/2018 - link.parentItem tag came as STRUCT instead of ARRAY  
                  if ( path.startsWith("link") ) {
                        if ( !(xmlSchemaExplodeArray.fieldNames.contains(path)) ) 
                           xmlSchemaExplodeArray = xmlSchemaExplodeArray.add( StructField( path, dt, true ))
                        if ( !( xmlSchemaArray.fieldNames.contains(path) ) )  // Added on 03/15/2018
                           xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))
                        //loop( path, s.elementType, acc )
                   }
                   s.fields.flatMap{f => if ( loggingEnabled.equalsIgnoreCase("Y") ){
                                           logger debug(" - path - " + path + " - f.name - " + f.name )
                                         }
                                        // This is done to add Flex attrib columns as it is in xmlSchemaArray
                                         if ( explodeColumns.contains(  (path + "_" + f.name).replace(".", "_").toLowerCase() ) ){
                                             //logger.debug( "Element exists? - " + path + "." + f.name + " - " + xmlSchemaArray.fieldNames.contains(path + "." + f.name) )
                                              if ( !( xmlSchemaArray.fieldNames.contains(path + "." + f.name) ) )
                                                xmlSchemaArray = xmlSchemaArray.add( StructField( path + "." + f.name, dt, true ))
                                         }
                                         loop(path + "." + f.name, f.dataType, acc )
                                   }
              case other => 
                   acc:+ path
                   //logger.debug( "Element exists Other? - " + path + " - " + xmlSchemaArray.fieldNames.contains(path) )
                   if ( !( xmlSchemaArray.fieldNames.contains(path) ) ) // Added on 02/2/2018
                     xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))
                   if ( itemArrayExploded == 1 )    // Add elements only till item tag  
                       xmlSchemaExplodeArray = xmlSchemaExplodeArray.add( StructField( path, dt, true ))
              }
              acc.foreach {  x => println ("acc - "  + x)  }
              acc 
          }
                       
          xmlFileStream.schema.fields.map { x =>  loop( x.name, x.dataType, xmlSchemaExplod ) }  //It is also working
          //logger.debug("- xmlSchemaExplod - ")  
          //xmlSchemaExplod.foreach { println }         
          logger.debug(" - Print xmlSchemaArray Tree String - ")  
          xmlSchemaExplodeArray.printTreeString()
          xmlSchemaArray.printTreeString()                   
          
          var colLists: MSeq[Column] = MSeq.empty
          var colListsAlias: MSeq[String] = MSeq.empty
          var colExplodeLists: MSeq[Column] = MSeq.empty
          var colExplodeListsAlias: MSeq[String] = MSeq.empty
          xmlSchemaExplodeArray.map{ x => { colExplodeLists ++= MSeq(col(x.name) ) 
                                            colExplodeListsAlias ++= MSeq(x.name.replace(".", "_"))
                                          }
                                  }
          
          // Check if columnName exists in Explode List then replace it with explodeListAlias
          xmlSchemaArray.map { x => { colLists ++= MSeq( col( if ( colExplodeListsAlias.contains( x.name.replace(".", "_") ) )
                                                                 x.name.replace(".", "_")  
                                                             else x.name
                                                            ) ) 
                                      colListsAlias ++= MSeq( x.name.replace("_", "").replace(".", "_").toLowerCase()  )
                                }
                            }
                    
          //colLists.map { x => if ( colExplodeLists.zip(colExplodeListsAlias).contains(x) ) x.name.replace(".", "_") }
          logger.debug( " - colExplodeListsAlias - " + colExplodeListsAlias )
          logger.debug(" - colExplodeLists - " + colExplodeLists )
          logger.debug( " - colListsAlias - " + colListsAlias )
          logger.debug(" - colLists - " + colLists )          
          logger.debug(" - arrayElements - " + arrayElements )     
          
          // Added for testing by Pushkar
          /*val xmlFileStreamExploded1 =  xmlFileStream.select( colExplodeLists.zip(colExplodeListsAlias).map{ case(x,y) => 
                                                                                                      if ( arrayElements.contains( y ) ){
                                                                                                             //println("exploded")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)  //.cast("String")
                                                                                                         }: _* 
                                                          )
           if ( loggingEnabled.equalsIgnoreCase("Y") ){
              xmlFileStreamExploded1.show()
              xmlFileStreamExploded1.printSchema()
           }*/
           // Ended for testing by Pushkar    
          
          //colLists.foreach { println }   //println( " x Datatype - "  )
          //xmlFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => println(x.dataType) }: _* )
          val xmlFileStreamExploded = xmlFileStream.select( colExplodeLists.zip(colExplodeListsAlias).map{ case(x,y) => 
                                                                                                      if ( arrayElements.contains( y ) ){
                                                                                                             //println("exploded")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)  //.cast("String")
                                                                                                         }: _* 
                                                          )
                                                    .select( colLists.zip(colListsAlias).map{ case(x,y) => if ( y.equalsIgnoreCase("item_gtin") || y.equalsIgnoreCase("hierarchyinformation_publishedgtin")
                                                                                                             || y.equalsIgnoreCase("item_alternateitemidentification_id") )
                                                                                                              lpad(x, 14, "0" ).alias(y)   //.cast(StringType)
                                                                                                          else if ( y.equalsIgnoreCase("hierarchyinformation_informationprovidergln") || y.equalsIgnoreCase("item_brandownergln")
                                                                                                                  || y.equalsIgnoreCase("item_informationprovidergln"))
                                                                                                              lpad(x, 13, "0" ).alias(y)   //.cast(StringType)
                                                                                                            else
                                                                                                              x.alias(y) 
                                                                                             }: _* 
                                                          )
  
                                                    //.filter("item_gtin = '44710044602' ")
          /*val xmlFileStreamExploded = xmlFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => if ( arrayElements.contains(x) ){
                                                                                                             println("exploded")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)
                                                                                             }: _* 
                                                          )*/
          logger.debug(" - xmlFileStreamExploded - " )
          xmlFileStreamExploded.printSchema()
          if ( loggingEnabled.equalsIgnoreCase("Y") ){            
            xmlFileStreamExploded.show()
          }
          //.filter("item_gtin = '44700079294' Or item_gtin = '00044700079294'")
          
          var tableColList: MSeq[String] = MSeq.empty
          dfItemDetails.schema.fields.map { x => tableColList ++= MSeq(x.name) }
          logger.debug( " - tableColList - " + tableColList )
          conn.withSessionDo { x => colListsAlias.map { y => //logger.debug(" Check column existense for " + y )
                                                                           if ( !( tableColList.contains(y) ) ) {
                                                                              logger.debug(" Run alter on mdm.item_details_by_doc_id for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_doc_id ADD " + y + " text ")
                                                                           }
                                                                     }  
                                            }  //cassandraConnector
          
          var tableColListGtin: MSeq[String] = MSeq.empty
          dfItemDetailsGtin.schema.fields.map { x => tableColListGtin ++= MSeq(x.name) }
          logger.debug( " - tableColList GTIN - " + tableColListGtin )
          conn.withSessionDo { x => colListsAlias.map { y => //logger.debug(" Check column existense for " + y )
                                                                           if ( !( tableColListGtin.contains(y) ) ) {
                                                                              logger.debug(" Run alter on mdm.item_details_by_gtin for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_gtin ADD " + y + " text ")
                                                                           }
                                                                     }  
                                            }  //cassandraConnector
          
          logger.debug( " - column existence validation completed - " )
          //saveDataFrameM("file", xmlFileStreamExploded, "", "xmlFileStreamExploded", null, null, null) 
          //Commented by Pushkar for testing
          /*saveDataFrameM("table", xmlFileStreamExploded.withColumn("update_time", lit(currentTime) )
                                                       .withColumn("file_name", lit(fileName) )
                                , "", null, "mdm", "item_details_by_doc_id", "dev") 
          saveDataFrameM("table", xmlFileStreamExploded.withColumn("update_time", lit(currentTime) )
                                                       .withColumn("file_name", lit(fileName) )
                               , "", null, "mdm", "item_details_by_gtin", "dev")*/
          
          //if condition added by Pushkar for testing
          //if ( xmlFileStreamExploded.filter("item_gtin = '00044000032029' ").select("item_gtin").collect().apply(0).getString(0) == "00044000032029" ){
          //Commented by Pushkar for testing
          explodeColumns.foreach { x =>  logger.debug( "ExplodeColumn - " + x._1 + " : Corresponding Table - " + x._2 )
                                          if ( colListsAlias.contains(x._1) ){
                                             //if ( x._1.equals("item_flex_attrgroupmany") ){ //x._1.equals("item_flex_attrgroup") ||  // Added by Pushkar for testing
                                             val ( callStatus, errorMessage ) = explodeChildLiteral( sqlContext, xmlFileStreamExploded, x._1, x._2, "Y", fileName )  //sc, cassandraConnector
                                             logger.debug( "Explode Child Literal Status - " + callStatus + " - " + errorMessage)
                                             if ( status != "1" )
                                                status = callStatus
                                             if ( errorMessage != null )
                                                 concatenatedErrorMessage.concat( errorMessage)
                                             //}

                                             val ( callStatusP, errorMessageP ) = pivotalAttributes(sqlContext, x._1, x._2, fileName )  //, tableColList, tableColListGtin
                                             logger.debug( "Pivotal Attributes Status - " + callStatusP + " - " + errorMessageP)
                                             if ( status != "1" )
                                                status = callStatusP         
                                             if ( errorMessageP != null )
                                                concatenatedErrorMessage.concat( errorMessageP)
                                          }
                                 }
          
         val ( callStatus, errorMessage ) = explodeChildLiteral( sqlContext, xmlFileStreamExploded, "link_parentitem", "document_link_details", "N", fileName )  //sc, cassandraConnector
         logger.debug( "Explode Child Literal Link Parent Status - " + callStatus + " - " + errorMessage)
         if ( status != "1" )
            status = callStatus
        if ( errorMessage != null )
            concatenatedErrorMessage.concat( errorMessage)
            
         val ( callStatusP, errorMessageP ) = pivotalLink(sqlContext, null, "document_link_details", fileName )  //, tableColList, tableColListGtin
         logger.debug( "Pivotal Attributes Link Parent Status - " + callStatusP + " - " + errorMessageP)
         if ( status != "1" )
            status = callStatusP         
         if ( errorMessageP != null )
            concatenatedErrorMessage.concat( errorMessageP)
            
          val processedCount = xmlFileStreamExploded.count().toInt
          val failedCount = 0 //xmlFileStream.count().toInt
          if (failedCount > 0){
             //unMatchedRecords.repartition(1).saveAsTextFile(cffPath + "/" + "rejected_" + fileName + "_" + getCurrentTime().getTime)
             concatenatedErrorMessage.concat("No of column counts in some rows are not same as expected" + " || " )
          }       
          
          logger.debug(" - matchedRecords and unMatchedRecords - "
                    + " - processedCount - " + processedCount
                    + " - failedCount - " + failedCount )
          //matchedRecords.show()          
                    
          if (xmlFileStream.rdd.isEmpty() ) {
              logger.debug("Encountered Empty file   " + fileName + " at " + currentRunTime)
              validationStatus = "Failed"
              status = "1"
              concatenatedErrorMessage.concat("No records in the file" + " || " )
              //throw new Exception("No records in the File - " + fileName + " at  " + currentRunTime)
            }else{
                if (failedCount > 0 || status.equalsIgnoreCase("1") ) {
                   validationStatus = "Partial_Complete"
                } else {
                   validationStatus = "Fully_Complete"
                   concatenatedErrorMessage = null
                }
           }
          
          logger.debug( " - validationStatus - " + validationStatus 
                      + " - status - " + status 
                      + " - concatenatedErrorMessage - " + concatenatedErrorMessage 
                      )
          
          val currentExtTime = null
          //validationStatus = "Ingested"   //Hardcoded by Pushkar for testing
          val fileStatusRDD = sc.parallelize( Seq( Row( fileId, fileName, snapShotId, processedCount, failedCount, (processedCount.toInt + failedCount.toInt), validationStatus, concatenatedErrorMessage, currentExtTime) ) ) 
          val fileStatusDF = sqlContext.createDataFrame( fileStatusRDD, fileStatusSchema ).toDF()
          saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_streaming_tracker")
                  
  			  /*val fileControllerRDD = sc.parallelize( Seq( Row( jobId, fileId, xmlSchemaArray.toString() ) ) )   //fileName
          val fileControllerDF = sqlContext.createDataFrame( fileControllerRDD, fileControllerSchema ).toDF()
          saveDataFrame(fileControllerDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_ingestion_controller")
          */  // This is removed as new table has file_name as primary key 
          logger.debug(" file " + fileName + " processed  at " + getCurrentTime())
           
	   }
		 catch {
		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("XML file processing error : " + e.getMessage )
            //e.printStackTrace()
            status = "1"
            val fileStatusRDD = sc.parallelize( Seq( Row( fileId, fileName, snapShotId, 0, 0, 0, "Failed", e.getMessage, null) ) ) 
            val fileStatusDF = sqlContext.createDataFrame( fileStatusRDD, fileStatusSchema ).toDF()
            saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_streaming_tracker")
             
            //throw new Exception("Error while processing xml File at : " + e.getMessage + ": " + e.printStackTrace())
			}  
			return status
   }
   
   def explodeChildLiteral ( sqlContext: SQLContext
                            ,xmlFileStream: DataFrame, attrName: String 
                            ,tableName: String 
                            ,attributes_yn: String
                            ,fileName: String ): ( String, String ) = {  // sc: SparkContext, ,cassandraConnector: CassandraConnector 
       
        logger.info(" - explodeChild - " + "\n" 
                 + " - fileName - " + fileName + "\n"
                 + " - attrName - " + attrName + "\n"
                 + " - tableName - " + tableName + "\n"
                  );
    		 val currentTime = getCurrentTime()
    		 var status = "0"
    		 var message: String = null
         val xmlSchema: StructType = new StructType()
         var arrayElements:MSeq[String] = MSeq.empty                                 // Array Elements for which atleast one child is of Struct Type
         var arrayElementsNonStruct:MSeq[String] = MSeq.empty   // Added on 03/05/2018  Array Elements for which no child is of Struct Type
         var xmlSchemaExplod: Seq[String] = Seq[String]()
         var xmlFileAttr: DataFrame = null
         var primaryColumns: List[String] = null
         val defaultArray: Array[String] = new Array[String](0)
         
         var attrType: String =  ""
         
         if ( attributes_yn.equalsIgnoreCase( "Y" ) ) {
             primaryColumns = List[String]("documentid", "item_gtin", "hierarchyinformation_informationprovidergln", "uuid")
         }else{
             primaryColumns = List[String]("documentid", "link_parentitem", "uuid")
         }
     
         import sqlContext.implicits._ 
         try{
             val dfFlexAttr = sqlContext.read
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "mdm")
                .option("table", tableName ) 
                .load()
                .filter(s" file_name = '$fileName'")
             
             var tableColList: MSeq[String] = MSeq.empty
             dfFlexAttr.schema.fields.map { x => tableColList ++= MSeq(x.name) }
             logger.debug( " - tableColList - " + tableColList )
             
             if ( attributes_yn.equalsIgnoreCase( "Y" ) ) {
                 xmlFileAttr = xmlFileStream.select("documentId", "item_gtin", "hierarchyinformation_informationprovidergln", attrName)  //.filter("item_gtin = '00044000032029'")   // filter added by Pushkar for testing
             }else{
                 xmlFileAttr = xmlFileStream.select("documentId", attrName).distinct()  //.filter(" documentid = 'cin.LJ1901430000I07.0083.0074819091009.00044000044848.US.Modify' ")  //.distinct()
             }
                                //.filter("item_gtin = '20044700079298'")
             if ( loggingEnabled.equalsIgnoreCase("Y") ){
                 xmlFileAttr.printSchema()
                 xmlFileAttr.show()
                 xmlFileAttr.schema.fields.map { x => if ( x.name.equalsIgnoreCase(attrName) )
                                                          attrType = x.dataType.typeName
                                               }
             }
             xmlFileAttr.registerTempTable("item_xml_file")
    
             logger.debug(" - Create Struct Type from XML - ")  
                        
             val arrayChild = MLinkedHashMap[String, Array[String]]()
             val parentChildRelations = MLinkedHashMap[String, (String, Array[String]) ]()
             //val arrayChildS = MMap[String, Array[String]]()
             
             // It will be called to 
             // 1. insert all Childs and Parent data type in parentChildRelations for all Array and Struct elements 
             // 2. To insert all Childs in arrayChild for Array elements
             def getArrayElements( path: String, dt: DataType, addinArrayChild: String, parentPathDataType: String ) = {
                 if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                    logger.debug( "\n"
                             + " - getArrayElements - " + "\t" + " - parentPathDataType- "+ parentPathDataType + "\n" 
                             + " - path - " + path + "\n" + " - dt - " + dt + "\n"
                             + " - addinArrayChild - " + addinArrayChild )
                 }
                  dt match {
                  case s: StructType =>      
                       //s.fields.foreach { x => logger.debug("x.name " + path + "_" + x.name ) }
                       //s.fieldNames.foreach { x => logger.debug("x " + path + "_" + x ) }
                       val arrayFields = s.fieldNames.map { x => ( path.toLowerCase() + "_" + x.toLowerCase() ).replace(".", "_").replace("__", "_") }
                       arrayFields.foreach { x => logger.debug(" array elements x.name " + x ) }
                       if ( addinArrayChild.equalsIgnoreCase("Y") ){
                          arrayChild ++= MLinkedHashMap( path.toLowerCase() -> arrayFields  )
                       }
                       
                       if ( !parentChildRelations.contains(path.toLowerCase()) ){
                          //logger.debug("Not in ParentChild")
                          parentChildRelations ++= MLinkedHashMap( path.toLowerCase() -> (parentPathDataType, arrayFields)  )
                       }
                       //arrayChildS ++= MLinkedHashMap( path -> s.fieldNames  ) 
                       //s.fields.flatMap(f => 
                  // Added on 03/05/2018
                  case other =>  ""
                       /*if ( addinArrayChild.equalsIgnoreCase("Y") ){
                          arrayChild ++= MLinkedHashMap( path.toLowerCase() -> Array(path.toLowerCase()) )
                       } */                   
                 }
             }
             
             
             def createPath(path: String): String = {
                 if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                    logger.debug( "createPath - " + path)
                 }
                 var modifiedPath: String = ""
                 var lookforValue = ""
                 try{
                     /*logger.debug( "Index of . " + path.lastIndexOf(".") + " " 
                                + path.trim() + " " 
                                //+ path.split('.').foreach { x => logger.debug(x) }
                                ) */
                     //Below does not work as it considers it as regular expression split.
                     //path.split(".").foreach { x => logger.debug( "Splitted Values - " + x )  } and splitAt.split(""".""")
                     //splitAt.split("""\.""") or splitAt.split("\\.") will also work. 
                                        
                     path.substring(0, if ( path.lastIndexOf(".") == -1 ) 0 else path.lastIndexOf(".") )
                         .split('.').foreach { x => if ( !( x.equalsIgnoreCase("") ) ) {
                                                      lookforValue = lookforValue.concat( ( if ( lookforValue.equalsIgnoreCase("") ) "" else "." ) + x.toLowerCase()  )
                                                      if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                                                         logger.debug( " : x - " + x //+ "\n"
                                                                     + " :modifiedPath - " + modifiedPath 
                                                                     + " :Look for - " + lookforValue.trim()  //( modifiedPath + x.toLowerCase() ).replace(".", "_").replace("__", "_")
                                                                     + " :DataType - " + parentChildRelations.getOrElse(lookforValue, ("", defaultArray) )._1
                                                                     )
                                                       }
                                                 
                                                       if ( parentChildRelations.getOrElse( lookforValue.trim(), ("", defaultArray) )._1.equalsIgnoreCase("Struct") ) 
                                                           modifiedPath = modifiedPath.concat(x.toLowerCase() + ".")
                                                      else modifiedPath = modifiedPath.concat(x.toLowerCase() + "_")
                                                      
                                                      // Below if is added as if value exists in arrayElements then it will have an alias with all . replaced with _
                                                      if ( arrayElements.contains(modifiedPath.dropRight(1) ) ) 
                                                         modifiedPath = modifiedPath.replace(".", "_").replace("__", "_")
                                                      
                                                 }  // If ! is added because if path comes as item_flex_attrgroupmany then it will run loop on "" value and append "_" on the modified path
                                              //logger.debug( " modifiedPath - " + modifiedPath )
                                             }
                     modifiedPath = modifiedPath.concat( path.substring( if ( path.lastIndexOf(".") == -1 ) 0 else path.lastIndexOf(".") + 1, path.length() ).toLowerCase() )
                     //if ( modifiedPath.endsWith(".") || modifiedPath.endsWith("_") ) modifiedPath = modifiedPath.dropRight(1)
                 }
                 catch {
                		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                			    	logger.error("Error while creating New Path for ArrayElement : " + e.getMessage )
                            //e.printStackTrace()
                            //throw new Exception("Error while processing xml File at : " + e.getMessage + ": " + e.printStackTrace())
                 }  
                 modifiedPath
             }
             
             def loop(path: String, dt: DataType, acc:Seq[String]): Seq[String] = {
                  if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                      logger.debug( " - loop - " + "\t" + " - path - " + path + "\t" + " - dt - " + dt)
                  }
                  dt match {
                  case s: ArrayType =>
                          var newPath = ""
                          var parentDataType = ""
                          /*val parentLastIndex = if ( path.lastIndexOf(".") == -1 ) 0 else path.lastIndexOf(".")
                          if ( loggingEnabled.equalsIgnoreCase("Y") ){
                              logger.debug( /*"subs tring - " + path.substring(0, ( if ( path.lastIndexOf(".") == -1 ) 0 else path.lastIndexOf(".") ) ) 
                                 +*/  " - Looking for value - " + path.substring(0, parentLastIndex ).toLowerCase()   //.replace(".", "_").replace("__", "_")
                                      )
                          }
                          if ( parentChildRelations.contains(path.substring(0, parentLastIndex ).toLowerCase() ) ){
                              parentDataType = parentChildRelations.get( path.substring(0, parentLastIndex ).toLowerCase()).get._1
                          }
                          else{
                              parentDataType = ""
                          }
                          if ( loggingEnabled.equalsIgnoreCase("Y") ){
                            logger.debug( " - parentDataType - " + parentDataType ) 
                          }
                          if ( path.lastIndexOf(".") == -1 ) 
                              newPath = path.replace(".", "_").replace("__", "_").toLowerCase()
                          else
                              newPath = if ( parentDataType.equalsIgnoreCase("Array")     )   ///arrayElements.contains( path.substring(0, path.lastIndexOf(".") ).replace(".", "_").replace("__", "_").toLowerCase() ) )
                                           path.replace(".", "_").replace("__", "_").toLowerCase()
                                      else ( path.substring(0, parentLastIndex ).replace(".", "_").replace("__", "_") + "." +
                                            path.substring( parentLastIndex + 1, path.length() ).replace(".", "_").replace("__", "_") ).toLowerCase() 
                          */
                          newPath = createPath(path)
                          if ( loggingEnabled.equalsIgnoreCase("Y") ){
                            logger.debug( "newPath - " + newPath + " - s.elementType.typeName - " + s.elementType.typeName ) 
                          }
                          if ( s.elementType.typeName.equalsIgnoreCase("Struct") ) // Added on 03/05/2018 
                            arrayElements ++= MSeq( newPath )  //path.toLowerCase()  
                          else
                            arrayElementsNonStruct ++= MSeq( newPath ) 
                           //xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))  
                          //xmlSchemaExplodeArray = xmlSchemaExplodeArray.add( StructField( path, dt, true ))
                          getArrayElements( path, s.elementType, "Y", "Array")   //path
                          loop(path, s.elementType, acc )   
                          //val st = StructType
                  case s: StructType =>      
                       getArrayElements( path, s, "N", "Struct") 
                       s.fields.flatMap(f => loop(path + "." + f.name, f.dataType, acc ))
                  case other => 
                       acc:+ path
                       //xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))
                       //xmlSchemaExplodeArray = xmlSchemaExplodeArray.add( StructField( path, dt, true ))
                  }
                  acc.foreach {  x => println ("acc - "  + x)  }
                  acc 
              }
                                    
              xmlFileAttr.schema.fields.map { x => //logger.debug(" Schema x.name = " +  x.name )
                                                   loop( x.name, x.dataType, xmlSchemaExplod ) 
                                            } 
              //logger.debug("- xmlSchemaExplod - ")  
              //xmlSchemaExplod.foreach { println }         
              //logger.debug(" - Print xmlSchemaArray Tree String - ")  
              //xmlSchemaArray.printTreeString()     
              //xmlSchemaExplodeArray.printTreeString()
              //xmlSchemaExplodeArray.fields.map { x => x }
              
              var colLists: MSeq[Column] = MSeq.empty
              var colListsAlias: MSeq[String] = MSeq.empty
              
              /*logger.debug(" - arrayElements - " + arrayElements )
              logger.debug(" - arrayChild - " + arrayChild.size )
              arrayChild.foreach( f => logger.debug( "Element " + f._1 + " => "
                                                                + f._2.foreach { x => logger.debug( "Array element " + x) } 
                                                   ) )
              
               logger.debug(" - parentChildRelations - " + parentChildRelations.size )
               parentChildRelations.foreach(f => logger.debug( "Element " + f._1 + " => "
                                                                + f._2._2.foreach { x => logger.debug( "Parent and Child Data Type " + f._2._1 + " - " + x ) } 
                                                   ) )*/
               
               logger.debug(" - Print converted array objects - ")
               val convArrayChild = arrayChild.flatMap( y => MLinkedHashMap( y._1.replace(".", "_").replace("__", "_").toLowerCase() -> y._2 ) )
               //val convParentChildRelations1 = parentChildRelations.flatMap( y => MLinkedHashMap( y._1.replace(".", "_").replace("__", "_").toLowerCase() -> y._2 ) )
               val convParentChildRelations = parentChildRelations.map{ case(k, v) => k.replace(".", "_").replace("__", "_").toLowerCase() -> v }
               val convarrayElementsNonStruct = arrayElementsNonStruct.map { x => x.replace(".", "_").replace("__", "_").toLowerCase() }
              
              if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                  logger.debug( " Print arrayElements - " + arrayElements.size )
                  arrayElements.foreach { x => logger.debug("Array Element - " + x) }
                  
                  logger.debug(" Print convArrayChild - " + convArrayChild.size ) 
                  convArrayChild.foreach{f => logger.debug( "Parent - " + f._1 + " :Child - " + f._2.foreach { x => logger.debug( " Value - " + x) }   ) }
                  
                  logger.debug(" Print parentChildRelations - " + parentChildRelations.size ) 
                  parentChildRelations.foreach{f => logger.debug( "Parent - " + f._1 + " Parent Data Type - " + f._2._1 + " :Child - " + f._2._2.foreach { x => logger.debug( " Value - " + x) }   ) }
                  logger.debug(" Print convParentChildRelations - " + convParentChildRelations.size ) 
                  convParentChildRelations.foreach{f => logger.debug( "Parent - " + f._1 + " Parent Data Type - " + f._2._1 + " :Child - " + f._2._2.foreach { x => logger.debug( " Value - " + x) }   ) }
                  //logger.debug(" Print convParentChildRelations1 - " + convParentChildRelations1.size ) 
                  //convParentChildRelations1.foreach{f => logger.debug( "Parent - " + f._1 + " Parent Data Type - " + f._2._1 + " :Child - " + f._2._2.foreach { x => logger.debug( " Value - " + x) }   ) }
                    
                  logger.debug(" Print convarrayElementsNonStruct - " + convarrayElementsNonStruct.size ) 
                  convarrayElementsNonStruct.foreach{f => logger.debug( "element - " + f ) }
                  
                  logger.debug(" Ended array printing ")
              }
              
              val stringQuery = new StringBuilder
              val selectStatement = new StringBuilder
              val fromStatement = new StringBuilder
              val whereStatement = new StringBuilder
              
              logger.debug(" - Start String - " )
              stringQuery.append("SELECT ")
              
              fromStatement.append(" FROM item_xml_file ")
              
              //var columnListStArr: MSeq[String] = MSeq.empty
              val columnListStArr: MLinkedHashMap[Int, (String, Int, Array[String])] = MLinkedHashMap[Int, (String, Int, Array[String]) ]()  // Store Struct Columns Childs and number of childs
              var columnListStArrIndex: Int = 0
              var columnListStArrNoOfChild: Int = 0
              //logger.debug( "Check values - " +  arrayChild.get("item_flex_attrgroupmany").get.mkString(",") )
              arrayElements.foreach { x =>  if ( loggingEnabled.equalsIgnoreCase("Y") ){
                                                logger.debug( " arrayElement - " + x
                                                            + " convArrayChild.get - "
                                                            + convArrayChild.getOrElse( x.replace(".", "_").replace("__", "_"), defaultArray ).foreach { y => logger.debug("  - Array Childs- " + y) }
                                                        )
                                            }
                                           convArrayChild.getOrElse( x.replace(".", "_").replace("__", "_"), defaultArray ).foreach { y => if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                                                                                                                                               logger.debug( " Array Child - " + y) 
                                                                                                                                           }
                                                                                                                                           if ( convParentChildRelations.getOrElse(y.toLowerCase(), ("", defaultArray) )._1  == "Struct"  ) {
                                                                                                                                              columnListStArr ++= MLinkedHashMap( columnListStArrIndex -> ( y.toLowerCase(), convParentChildRelations.getOrElse(y.toLowerCase(), ("", defaultArray) )._2.size, convParentChildRelations.getOrElse(y.toLowerCase(), ("", defaultArray) )._2 ) )
                                                                                                                                              columnListStArrIndex += 1
                                                                                                                                              selectStatement.append( "," + y.toLowerCase() + ".* " )  //+ y.toLowerCase()
                                                                                                                                          }
                                                                                                                                          else if ( convParentChildRelations.getOrElse(y, ("", defaultArray) )._1  == "Array"  )
                                                                                                                                              ""
                                                                                                                                          else  
                                                                                                                                             selectStatement.append( "," + y.toLowerCase() )
                                                                                                                                             //( if ( y.contains("link_parentitem_gtin") || y.contains("link_parentitem_childitem_value") ) "lpad("+ y.toLowerCase() + ", 14, '0' ) " + y.toLowerCase() else y.toLowerCase() )
                                                                                                                                    }
                                          whereStatement.append( " LATERAL VIEW OUTER inline(" 
                                                             + x + ")"    //.replace(".", "_").replace("__", "_") 
                                                             + x.replace(".", "_").replace("__", "_") + " as " 
                                                             + convArrayChild.getOrElse( x.replace(".", "_").replace("__", "_"), defaultArray ).mkString(",")
                                                             ) 
                                                             
              
                                   }
              
              //Added on 03/05/2018
              logger.debug( " - attrType - " + attrType )
              //if ( arrayElements.size == 0 ) {   // if condition changed on 03/16/2018
              if ( attrType.equalsIgnoreCase("struct") ){
                 convParentChildRelations.foreach{ f => if ( loggingEnabled.equalsIgnoreCase("Y") ){
                                                           logger.debug( " f - " + f )
                                                         }
                                                        if ( f._2._1 == "Struct" ) {
                                                          columnListStArr ++= MLinkedHashMap( columnListStArrIndex -> ( f._1.toLowerCase(), f._2._2.size, f._2._2 ) )
                                                          columnListStArrIndex += 1
                                                        }
                                                }    
                 selectStatement.insert( 0, "," + attrName + ".* " ) 
              }
              //Ended on 03/05/2018 
              
              sqlContext.udf.register( "getUUID", getUUID _)
              if ( attributes_yn.equalsIgnoreCase("Y") ){
                 stringQuery.append( selectStatement.insert( 0, "documentid, item_gtin, hierarchyinformation_informationprovidergln, getUUID() as uuid" ) )  
                            // if ( arrayElements.size == 0 ) selectStatement.append( ",*" ) else
                            // if else Added on 03/05/2018 
                            .append( fromStatement )
                            .append(whereStatement)
              }else{
                stringQuery.append( selectStatement.insert( 0, "documentid, getUUID() as uuid" ) )
                           .append( fromStatement )
                           .append(whereStatement)
              }
              
              logger.debug( "stringQuery - " + stringQuery.toString() + "\n" 
                       + "selectStatement - " + selectStatement.toString() + "\n" 
                       + "whereStatement - " + whereStatement.toString() 
                          )
                          
             // Added by Pushkar for testing
             // Failed with Error org.apache.hadoop.hive.ql.exec.UDFArgumentException: The sub element must be struct, but was string
             /*val genratedQuery = """ SELECT documentid,
                                             item_gtin,
                                             hierarchyinformation_informationprovidergln,
                                             getuuid() AS uuid,
                                             item_flex_attrmany.*
                                            ,value
                                        FROM item_xml_file
                                     lateral view OUTER inline(item_flex_attrmany.value) item_flex_attrmany_value AS value
                                      """
             //--
             logger.debug( " - genratedQuery- " + genratedQuery ) 
             val stringResult = sqlContext.sql(genratedQuery)*/
              //Ended by Pushkar for testing
                          
             // Added by Pushkar for testing
             // Failed with null value as link details came with parent_item as STRUCT  - 1WS_CS_CIN.cin.LJ1NM5200000707.1000020473835
             /*val genratedQuery = s""" SELECT documentid,
                                             getuuid() AS uuid,
                                             link_parentitem.*,
                                             --link_parent_item._gtin,
                                             --link_parent_item._numberOfUniqueGtins,
                                             --link_parent_item._totalQuantityOfNextLowerLevelTradeItem,
                                             link_parentitem_childitem_value,
                                             link_parentitem_childitem_quantity
                                  FROM item_xml_file ixf lateral view
                                 OUTER inline(link_parentitem.childitem) link_parentitem_childitem AS link_parentitem_childitem_value, link_parentitem_childitem_quantity
                                """
             logger.debug( " - genratedQuery- " + genratedQuery ) 
             val stringResult1 = sqlContext.sql(genratedQuery)
             stringResult1.printSchema()             
             stringResult1.show()*/
              //Ended by Pushkar for testing
             
             // Added by Pushkar for testing
             // Failed with org.apache.spark.sql.AnalysisException: cannot resolve 'item_flex_attrgroupmany_row_attrgroupmany.row' 
             // given input columns: 
             // [item_flex_attrgroupmany, documentId, item_flex_attrgroupmany_row, hierarchyinformation_informationprovidergln, item_gtin, item_flex_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attr_name, item_flex_attrgroupmany_name]; line 1 pos 1192
          
                          /*
                           * 
                                             --item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany.*,
                                             --item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany.*,
                                             --item_flex_attrgroupmany_row_attrgroupmany_row_attr_value,
                                             --item_flex_attrgroupmany_row_attrgroupmany_row_attr_name,
                           *                 --item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_value,
                                             --item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_name,
                                             --OUTER inline(item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany.row.attr) item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_name lateral view
                                            --OUTER inline(item_flex_attrgroupmany_row.attrgroupmany.row) item_flex_attrgroupmany_row_attrgroupmany_row AS item_flex_attrgroupmany_row_attrgroupmany_row_attr, item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany, item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany lateral view
                                             --OUTER inline(item_flex_attrgroupmany_row_attrgroupmany_row_attr) item_flex_attrgroupmany_row_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attrgroupmany_row_attr_name lateral view
                                                                                    * 
                           */
             /*val genratedQuery = s""" SELECT documentid,
                                             item_gtin,
                                             hierarchyinformation_informationprovidergln,
                                             getuuid() AS uuid,
                                             item_flex_attrgroupmany_name,
                                             item_flex_attrgroupmany_row.*,
                                             item_flex_attrgroupmany_row_attr_value,
                                             item_flex_attrgroupmany_row_attr_name,
                                             item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany.*,
                                             item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany.*,
                                             item_flex_attrgroupmany_row_attrgroupmany_row_attr_value,
                                             item_flex_attrgroupmany_row_attrgroupmany_row_attr_name,
                                             item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_value,
                                             item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_name,
                                             item_flex_attrgroupmany_row_attrqual_value,
                                             item_flex_attrgroupmany_row_attrqual_name,
                                             item_flex_attrgroupmany_row_attrqual_qual,
                                             item_flex_attrgroupmany_row_attrqualmany_name,
                                             item_flex_attrgroupmany_row_attrqualmany_value.*
                                        FROM item_xml_file lateral view
                                       OUTER inline(item_flex_attrgroupmany) item_flex_attrgroupmany AS item_flex_attrgroupmany_name, item_flex_attrgroupmany_row lateral view
                                       OUTER inline(item_flex_attrgroupmany_row.attr) item_flex_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attr_name lateral view
                                       OUTER inline(item_flex_attrgroupmany_row.attrgroupmany.row) item_flex_attrgroupmany_row_attrgroupmany_row AS item_flex_attrgroupmany_row_attrgroupmany_row_attr, item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany, item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany lateral view
                                       OUTER inline(item_flex_attrgroupmany_row_attrgroupmany_row_attr) item_flex_attrgroupmany_row_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attrgroupmany_row_attr_name lateral view
                                       OUTER inline(item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany.row.attr) item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attrgroupmany_row_attrgroupmany_row_attr_name lateral view
                                       OUTER inline(item_flex_attrgroupmany_row.attrqual) item_flex_attrgroupmany_row_attrqual AS item_flex_attrgroupmany_row_attrqual_value, item_flex_attrgroupmany_row_attrqual_name, item_flex_attrgroupmany_row_attrqual_qual lateral view
                                       OUTER inline(item_flex_attrgroupmany_row.attrqualmany) item_flex_attrgroupmany_row_attrqualmany AS item_flex_attrgroupmany_row_attrqualmany_name, item_flex_attrgroupmany_row_attrqualmany_value
                                """
             logger.debug( " - genratedQuery- " + genratedQuery ) 
             val stringResult = sqlContext.sql(genratedQuery)
             stringResult.printSchema()             
             stringResult.show()*/
              //Ended by Pushkar for testing
                          
              val stringResult = sqlContext.sql(stringQuery.toString())
              stringResult.printSchema()
              //stringResult.show()
              //stringResult.registerTempTable("item_xml_final_file")
               
              if ( loggingEnabled.equalsIgnoreCase("Y") ) {
                stringResult.show() 
                logger.debug(" Print Struct elements columnListStArr - " + columnListStArr.size ) 
                columnListStArr.foreach { x => logger.debug("Column List StrAr Parent - " + x._1 
                                                            + " :Parent Name - " + x._2._1 
                                                            + " :Size - " + x._2._2 
                                                            + x._2._3.foreach { y => logger.debug( ":Child - " +  y) }  ) }
              }
              //val selectColumn = stringResult.schema.fieldNames 
              var columnList: MSeq[Column] = MSeq.empty
              var columnListAlias: MSeq[String] = MSeq.empty
              columnListStArrIndex = 0
              
              //val selectStatementNew = new StringBuilder
              var columnAliasAll: MSeq[String] = MSeq.empty  // = new StringBuilder
              val structColumns: MLinkedHashMap[String, Array[String]] = MLinkedHashMap[String, Array[String]]()   // Stores Struct columns
              val structColumnsArr: MLinkedHashMap[Int, (String, Int, Array[String])] = MLinkedHashMap[Int, (String, Int, Array[String]) ]()
              var structColumnsArrIndex = 0
              var columnName = ""
              //val defaultColumnList: (String, Int, Array[String]) = new ("", 0, new Array[String](1))
              stringResult.schema.fields.map { x => /*logger.debug( "Index - " + stringResult.schema.fieldIndex(x.name) + "\t"
                                                                + " :Column Name - " + x.name + "\t" 
                                                                + " :Data Type String - "+ x.dataType.typeName + "\t" 
                                                                + "Data Type - " + x.dataType + "\t" )*/
                                                    /*if ( x.name.startsWith("item") ){
                                                      logger.debug("add name in StArr")
                                                      columnListStArr ++= MSeq( x.name.replace(".", "_").replace("__", "_").toLowerCase() )
                                                    }*/
                                                    // Below IF condition is removed as it shouldn't have impact on Renamed Columns list i.e. columnAliasAll
                                                    // Instead it should e used only to avoid selecting struct and array in final list i.e. columnListAlias
                                                    //if ( !( x.dataType.typeName.equalsIgnoreCase("struct") || x.dataType.typeName.equalsIgnoreCase("array") ) ) {
                                                        //logger.debug(" Select Column")
                                                        //selectStatementNew.append(", ").append(x.name).append(" as " )
                                                        //columnList ++= MSeq( col( x.name ) )  commented on 02/28/2018
                                                        
                                                                       
                                                        //if ( !( primaryColumns.contains(x.name) ) && !( x.name.startsWith("item") ) ) {     //( x.name.startsWith("_") )  //!( x.name.startsWith("item") )  //x.name.startsWith("_") || x.name.startsWith("value") || x.name.startsWith("qual")
                                                        if ( !( primaryColumns.contains(x.name) ) 
                                                           && ( ( attributes_yn.equalsIgnoreCase( "Y" ) && !( x.name.startsWith("item") ) ) 
                                                             || ( attributes_yn.equalsIgnoreCase( "N" ) && !( x.name.startsWith("link") ) )
                                                             )
                                                          ) {  // Added on 03/20/2018 && condition is changed to include attributes_yn and link
                                                            columnName = if ( ( columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_").startsWith("_") )
                                                                              ( columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_").drop(1)
                                                                       else ( columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_")
                                                          
                                                           //logger.debug("columnListStArrIndex - " + columnListStArrIndex + " :Last Name - " + columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) ) )
                                                           if ( !( x.dataType.typeName.equalsIgnoreCase("array") ) || convarrayElementsNonStruct.contains(columnName) ) {  // Commented on 02/28/2018 to Keep Structs x.dataType.typeName.equalsIgnoreCase("struct") ||
                                                               // || arrayElementsNonStruct Added on 03/05/2018
                                                               columnListAlias ++= MSeq( columnName )  //.substring(1)
                                                           }
                                                           if ( x.dataType.typeName.equalsIgnoreCase("struct") ){                                                              
                                                              structColumns ++= MLinkedHashMap( columnName
                                                                                    -> convParentChildRelations.getOrElse( ( columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_")
                                                                                                                         , ( "", defaultArray ) )._2
                                                                                   )
                                                              structColumnsArr ++= MLinkedHashMap ( structColumnsArrIndex -> ( columnName
                                                                                                                     ,convParentChildRelations.getOrElse( ( columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_")
                                                                                                                                                         ,( "", defaultArray ) )._2.size
                                                                                                                     ,convParentChildRelations.getOrElse( ( columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_")
                                                                                                                                                         ,( "", defaultArray ) )._2
                                                                                                                   ) 
                                                                                       )
                                                              structColumnsArrIndex += 1
                                                           }
                                                           //selectStatementNew.append( (columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_") )
                                                           columnAliasAll ++= MSeq( columnName )
                                                        }
                                                        else {
                                                            columnName = if ( ( x.name.replace(".", "_").replace("__", "_").toLowerCase() ).startsWith("_") )
                                                                              ( x.name.replace(".", "_").replace("__", "_").toLowerCase() ).drop(1)
                                                                       else x.name.replace(".", "_").replace("__", "_").toLowerCase()
                                                          
                                                            if ( !( x.dataType.typeName.equalsIgnoreCase("array") ) || convarrayElementsNonStruct.contains(columnName) ) {   // Commented on 02/28/2018 to Keep Structs x.dataType.typeName.equalsIgnoreCase("struct") || 
                                                               // || arrayElementsNonStruct Added on 03/05/2018 
                                                               columnListAlias ++= MSeq( columnName )
                                                            }
                                                            if ( x.dataType.typeName.equalsIgnoreCase("struct") ){                                                              
                                                              structColumns ++= MLinkedHashMap( columnName
                                                                                    -> convParentChildRelations.getOrElse( x.name.replace(".", "_").replace("__", "_").toLowerCase()
                                                                                                                         , ( "", defaultArray ) )._2
                                                                                   )
                                                           }
                                                            //selectStatementNew.append( x.name.replace(".", "_").replace("__", "_").toLowerCase() )
                                                            columnAliasAll ++= MSeq( columnName )
                                                        }
                                                        
                                                     /*}else{
                                                         columnAliasAll ++= MSeq(x.name.replace(".", "_").replace("__", "_").toLowerCase())
                                                     }*/
                                                    //if ( !( primaryColumns.contains(x.name) ) && !( x.name.startsWith("item") ) ) {
                                                    if ( !( primaryColumns.contains(x.name) ) 
                                                           && ( ( attributes_yn.equalsIgnoreCase( "Y" ) && !( x.name.startsWith("item") ) ) 
                                                             || ( attributes_yn.equalsIgnoreCase( "N" ) && !( x.name.startsWith("link") ) )
                                                             )
                                                          ) {  // Added on 03/20/2018 && condition is changed to include attributes_yn and link
                                                       //logger.debug("columnListStArrIndex - " + columnListStArrIndex + " :Last Name - " + columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) ) )
                                                      //logger.debug("Increase no of childs") 
                                                      columnListStArrNoOfChild += 1
                                                       if ( columnListStArrNoOfChild == columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._2 ) {
                                                              //logger.debug("all child retrieves" )
                                                              columnListStArrIndex += 1
                                                              columnListStArrNoOfChild = 0
                                                       }
                                                    }
                                             } 
              
              /*logger.debug( "Final Column List - " + columnList.zip(columnListAlias).map{ case(x,y) => x.alias(y).toString() }.mkString(",") + "\n"
                       // + " - selectStatementNew - " + selectStatementNew.deleteCharAt(0) + "\n"
                        + " - Flatten - " + columnListAlias.mkString(",") + "\n"
                        + " - columnAliasAll - " + columnAliasAll.mkString(",")      ///.deleteCharAt(0).toString()
                          )*/
               // All columns are renamed based on parent tag
               val stringResultRenamed = stringResult.toDF( columnAliasAll: _* )  //map { x => if ( x.startsWith("_") ) x.drop(1) else x} 
               logger.debug("Renamed columns")
               if ( loggingEnabled.equalsIgnoreCase("Y") ){
                   stringResultRenamed.printSchema()  
                   stringResultRenamed.show()
               }
               
               if ( loggingEnabled.equalsIgnoreCase("Y") ){ 
                  logger.debug( " Print columnListStArr - " + columnListStArr.size )
                  columnListStArr.foreach(f  => logger.debug( "Index - " +  f._1 + " :Value - " + f._2._1 + " :Size " + f._2._2 + " :Child - " 
                                           + f._2._3.foreach { x => logger.debug( " Value - " + x ) }  ) 
                                      )
                  
                  logger.debug(" Print structColumns - " + structColumns.size ) 
                  structColumns.foreach(f => logger.debug( "Struct Column - "  + f._1 + " :Child - " + f._2.foreach { x => logger.debug( " Value - " + x ) } ) )
                  
                  logger.debug(" Print structColumnsArr - " + structColumnsArr.size ) 
                  structColumnsArr.foreach{f => logger.debug( "Index - " +  f._1 + " :Value - " + f._2._1 + " :Size " + f._2._2 + " :Child - "
                                              + f._2._3.foreach { x => logger.debug( " Value - " + x ) }  ) 
                                          }
                  
                  logger.debug(" Ended struct printing ")
              }

               // Added on 02/28/2018  
               /*val explodeStruct = stringResultRenamed.select("documentid", "item_gtin", "hierarchyinformation_informationprovidergln", "uuid"
                                        ,"item_flex_attrgroupmany_name", "item_flex_attrgroupmany_row_attrgroupmany_name"
                                        ,"item_flex_attrgroupmany_row_attr_value", "item_flex_attrgroupmany_row_attr_name"
                                        ,"item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name"
                                        ,"item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_value.*"
                                        )*/
               
                // Structs are exploded and Arrays are removed from first set of columns except if Array is selected in exploded Struct column
                val explodeStruct = stringResultRenamed.select( columnListAlias.map { x => if ( structColumns.contains( x.toLowerCase() ) ) s"$x.*" else x }.head
                                                               ,columnListAlias.map { x => if ( structColumns.contains( x.toLowerCase() ) ) s"$x.*" else x }.tail: _* 
                                                               )                        
                logger.debug("explodeStruct columns")
                if ( loggingEnabled.equalsIgnoreCase("Y") ){
                    explodeStruct.printSchema()
                    explodeStruct.show()
                }
                                
                var finalColumnListAlias: MSeq[String] = MSeq.empty
                var finalColumnListAll: MSeq[String] = MSeq.empty     // Added on 03/20/2018
                
                columnListStArrIndex = 0
                columnListStArrNoOfChild = 0
                columnName = ""
                explodeStruct.schema.fields.map { x => if ( !( primaryColumns.contains(x.name) ) 
                                                           && ( ( attributes_yn.equalsIgnoreCase( "Y" ) && !( x.name.startsWith("item") ) ) 
                                                             || ( attributes_yn.equalsIgnoreCase( "N" ) && !( x.name.startsWith("link") ) )
                                                             )
                                                          ) {  // Added on 03/20/2018 && condition is changed to include attributes_yn and link
                                                           //( x.name.startsWith("_") )  //!( x.name.startsWith("item") )  //x.name.startsWith("_") || x.name.startsWith("value") || x.name.startsWith("qual")
                                                           //logger.debug("if columnListStArrIndex - " + columnListStArrIndex + " :Last Name - " + ( structColumnsArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_") )
                  
                                                           //If has been removed as 
                                                           // Exploded Struct column can have an array and in that case ColumnListAlias will have that column missing
                                                           // which will cause an error - The number of columns doesn't match.
                                                           //if ( !( x.dataType.typeName.equalsIgnoreCase("array") ) || convarrayElementsNonStruct.contains(x.name)  ) {  // Commented on 02/28/2018 to Keep Structs x.dataType.typeName.equalsIgnoreCase("struct") || 
                                                                // || arrayElementsNonStruct Added on 03/05/2018
                                                                val columnName = if ( ( structColumnsArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_").startsWith("_") )
                                                                                      ( structColumnsArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_").drop(1)
                                                                               else (structColumnsArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._1.toString() + "_" + x.name.replace(".", "_").replace("__", "_").toLowerCase() ).replace("__", "_")
                                                                                            
                                                                finalColumnListAlias ++= MSeq( columnName )  //.substring(1)
                                                                                            
                                                                //logger.debug( " DataType in Non Primary List - " + x.dataType.typeName + " : " + columnName )  
                                                                // Added to avoid selecting array elements in the final list
                                                                if ( !( x.dataType.typeName.equalsIgnoreCase("array") ) ){
                                                                   //logger.debug( "Insert column name in final List - in Non Primary" )
                                                                   finalColumnListAll ++= MSeq( columnName )
                                                                }
                                                           //}
                                                        }
                                                        else {
                                                            //logger.debug(" else columnListStArrIndex - " + columnListStArrIndex + " :Last Name - " + x.name.replace(".", "_").replace("__", "_").toLowerCase() )
                                                           
                                                            // has been removed as 
                                                            // Exploded Struct column can have an array and in that case ColumnListAlias will have that column missing
                                                            // which will cause an error - The number of columns doesn't match.                                                          
                                                            //if ( !( x.dataType.typeName.equalsIgnoreCase("array") ) || convarrayElementsNonStruct.contains(x.name)  ) {   // Commented on 02/28/2018 to Keep Structs x.dataType.typeName.equalsIgnoreCase("struct") || 
                                                              // || arrayElementsNonStruct Added on 03/05/2018
                                                              
                                                              val columnName = if ( x.name.replace(".", "_").replace("__", "_").toLowerCase().startsWith("_") )
                                                                                  x.name.replace(".", "_").replace("__", "_").toLowerCase().drop(1)
                                                                              else x.name.replace(".", "_").replace("__", "_").toLowerCase()
                                                              finalColumnListAlias ++= MSeq( columnName )
                                                              
                                                              // Added to avoid selecting array elements in the final list
                                                              //logger.debug( " DataType in Primary List - " + x.dataType.typeName + " : " + columnName )
                                                              if ( !( x.dataType.typeName.equalsIgnoreCase("array") ) ){
                                                                 //logger.debug( "Insert column name in final List - in Primary" )
                                                                 finalColumnListAll ++= MSeq( columnName )
                                                              }
                                                            //}
                                                        }
                                                        
                                                        if ( !( primaryColumns.contains(x.name) ) 
                                                           && ( ( attributes_yn.equalsIgnoreCase( "Y" ) && !( x.name.startsWith("item") ) ) 
                                                             || ( attributes_yn.equalsIgnoreCase( "N" ) && !( x.name.startsWith("link") ) )
                                                             )
                                                          ) {  // Added on 03/20/2018 && condition is changed to include attributes_yn and link  
                                                           //logger.debug("columnListStArrIndex - " + columnListStArrIndex + " :Last Name - " + columnListStArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) ) )
                                                          //logger.debug("Increase no of childs") 
                                                          columnListStArrNoOfChild += 1
                                                           if ( columnListStArrNoOfChild == structColumnsArr.getOrElse( columnListStArrIndex, ("" ,0, defaultArray ) )._2 ) {
                                                                  //logger.debug("all child retrieves" )
                                                                  columnListStArrIndex += 1
                                                                  columnListStArrNoOfChild = 0
                                                           }
                                                        }
                                             } 
                
               // Explode Struct Renamed 
               val explodeStructRenamed = explodeStruct.toDF( finalColumnListAlias: _* )   //.map { x => if ( x.startsWith("_") ) x.drop(1) else x} 
               logger.debug("Renamed columns Explode Struct")
               if ( loggingEnabled.equalsIgnoreCase("Y") ){
                   explodeStructRenamed.printSchema()  
                   explodeStructRenamed.show()
                   
                   logger.debug( "Final Column List - " + finalColumnListAll.size )
                   finalColumnListAll.foreach { x => logger.debug( "Final Column List - " + x ) }
               }
                              
               // finalColumnListAlias replaced with finalColumnListAll on 03/20/2018
               val finalData = explodeStructRenamed.select( finalColumnListAll.map { x =>  { if ( x.contains("link_parentitem_gtin") || x.contains("link_parentitem_childitem_value") ) lpad(col(x), 14, "0").alias(x) else col(x) }  }:_* ) //.mkString(",")
               //if ( x.startsWith("_") ) { if ( x.contains("link_parentitem_gtin") || x.contains("link_parentitem_childitem_value") ) lpad(col(x), 14, "0").alias(x.drop(1)) else col(x.drop(1)) } else
               logger.debug("final columns")
               if ( loggingEnabled.equalsIgnoreCase("Y") ){
                   finalData.printSchema()
                   finalData.show()
               }
               
               //Failed with error 
               //org.apache.hadoop.hive.ql.exec.UDFArgumentException: Top level object must be an array but was struct<_name:string,value:struct<_value:double,_qual:string>>
               // Added by Pushkar for testing
               /*val generatedQuery = s"""SELECT documentid,
                                               item_gtin,
                                               hierarchyinformation_informationprovidergln,
                                               getuuid() AS uuid,
                                               item_flex_attrgroupmany_name,
                                               item_flex_attrgroupmany_row_attrgroupmany.*,
                                               item_flex_attrgroupmany_row_attrqual,
                                               item_flex_attrgroupmany_row_attrqualmany,
                                               item_flex_attrgroupmany_row_attr_value,
                                               item_flex_attrgroupmany_row_attr_name,
                                               item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany.*,
                                               item_flex_attrgroupmany_row_attrgroupmany_row_attr_value,
                                               item_flex_attrgroupmany_row_attrgroupmany_row_attr_name,
                                               item_flex_attrgroupmany_row_attrqual_value,
                                               item_flex_attrgroupmany_row_attrqual_name,
                                               item_flex_attrgroupmany_row_attrqual_qual,
                                               item_flex_attrgroupmany_row_attrqualmany_name,
                                               item_flex_attrgroupmany_row_attrqualmany_value.*
                                        	  ,item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name
                                        	  ,item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_value.*
                                          FROM item_xml_file lateral view
                                         OUTER inline(item_flex_attrgroupmany) item_flex_attrgroupmany AS item_flex_attrgroupmany_name, item_flex_attrgroupmany_row lateral view
                                         OUTER inline(item_flex_attrgroupmany_row) item_flex_attrgroupmany_row AS item_flex_attrgroupmany_row_attr, item_flex_attrgroupmany_row_attrgroupmany, item_flex_attrgroupmany_row_attrqual, item_flex_attrgroupmany_row_attrqualmany lateral view
                                         OUTER inline(item_flex_attrgroupmany_row_attr) item_flex_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attr_name lateral view
                                         OUTER inline(item_flex_attrgroupmany_row_attrgroupmany.row) item_flex_attrgroupmany_row_attrgroupmany_row AS item_flex_attrgroupmany_row_attrgroupmany_row_attr, item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany lateral view
                                         OUTER inline(item_flex_attrgroupmany_row_attrgroupmany_row_attr) item_flex_attrgroupmany_row_attrgroupmany_row_attr AS item_flex_attrgroupmany_row_attrgroupmany_row_attr_value, item_flex_attrgroupmany_row_attrgroupmany_row_attr_name lateral view
                                         OUTER inline(item_flex_attrgroupmany_row_attrqual) item_flex_attrgroupmany_row_attrqual AS item_flex_attrgroupmany_row_attrqual_value, item_flex_attrgroupmany_row_attrqual_name, item_flex_attrgroupmany_row_attrqual_qual lateral view
                                         OUTER inline(item_flex_attrgroupmany_row_attrqualmany) item_flex_attrgroupmany_row_attrqualmany AS item_flex_attrgroupmany_row_attrqualmany_name, item_flex_attrgroupmany_row_attrqualmany_value lateral view
                                         OUTER inline(item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany) item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_row AS item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name, item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_value
                                            """
               logger.debug(" - generatedQuery - " + generatedQuery ) 
               val generatedData = sqlContext.sql(generatedQuery)
               generatedData.show()
               generatedData.printSchema()*/
               // Ended by Pushkar for testing
               
               //val selectStatementFinal = selectStatementNew.deleteCharAt(0).toString()
               //val finalDataQuery = s""" SELECT $selectStatementFinal FROM item_xml_final_file"""
               //logger.debug( "- finalDataQuery - " + finalDataQuery) 
               //stringResult.select(col, cols)
               //val finalData = sqlContext.sql(finalDataQuery) 
                //stringResult.select( selectStatementNew.deleteCharAt(0).toString() )
                //stringResult.select( columnList.zip(columnListAlias).map{ case(x,y) => x.alias(y)}: _*  )
              //finalData.show()
              //finalData.printSchema()
              
               // Commented by Pushkar for testing
              val finalColumnList = finalData.schema.fieldNames
              conn.withSessionDo { x => finalColumnList.map { y => //logger.debug(" Check column existense for " + y )
                                                                   if ( !( tableColList.contains(y) ) ) {
                                                                      logger.debug(" Run alter on mdm." + tableName + " for column " + y )
                                                                      x.execute("ALTER TABLE mdm." + tableName + " ADD " + y + " text ")
                                                                   }
                                                           }   //cassandraConnector 
                                                }
              saveDataFrameM("table", finalData.withColumn("update_time", lit( getCurrentTime() ) )
                                               .withColumn("file_name", lit(fileName) )
                                               .withColumn("active", lit("Y") )
                                    , "", "null", "mdm", tableName, "dev")
              // Commenting ended by Pushkar for testing
          }
    		 catch {
    		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
    			    	logger.error("XML file Explode Child processing error for " + tableName + " : " + e.getMessage )
                e.printStackTrace()
                status = "1"
                message = "XML file Explode Child processing error for " + tableName + " : " + e.getMessage
                //throw new Exception("Error while processing xml File at : " + e.getMessage + ": " + e.printStackTrace())
    			} 
    		 return ( status, message )
   }
   
   def pivotalAttributes(sqlContext: SQLContext, attrName: String, tableName: String, fileName: String ): ( String, String )
        =  {  //, docTableList: MSeq[String], gtinTableList: MSeq[String]
       logger.info(" - pivotalAttributes - " + "\n" 
                 + " - fileName - " + fileName + "\n"
                 + " - attrName - " + attrName + "\n"
                 + " - tableName - " + tableName + "\n"
                  );
    		 val currentTime = getCurrentTime()
    		 var status = "0"
    		 var message: String = null
    		 
    		 val dfItemDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_doc_id", "keyspace" -> "mdm") )
            .load();  
     
        val dfItemDetailsGtin = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_gtin", "keyspace" -> "mdm") )
            .load();
     
    		 var newdocTableList: Array[String] = dfItemDetails.schema.fieldNames
    		 var newgtinTableList: Array[String] = dfItemDetails.schema.fieldNames
    		 
    		 import sqlContext.implicits._ 
         try{
             val dfFlexAttr = sqlContext.read
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "mdm")
                .option("table", tableName ) 
                .load()
                .filter(s" file_name = '$fileName' AND active = 'Y' ")
             
             val pivotColumns: MLinkedHashMap[String, Array[String]] = MLinkedHashMap[String, Array[String] ]()
             var concatcolumns: MArrayBuffer[String] = MArrayBuffer[String]()
             val fieldNames = dfFlexAttr.schema.fieldNames
             
             fieldNames.map { x => if ( x.endsWith("name") && !(x.equalsIgnoreCase("file_name") ) ){
                                      concatcolumns.clear()   
                                   }// Added on 03/20/2018 concatColumns List should be cleared onyl if next element ends with _name
                                   if ( ( x.endsWith("name") || x.endsWith("qual") ) && !(x.equalsIgnoreCase("file_name") ) ){
                                      if ( loggingEnabled.equalsIgnoreCase("Y") ){
                                        logger.debug("Name or qual - " + x) 
                                      }
                                      concatcolumns ++= Seq(x)
                                   }
                                 else if ( x.endsWith("value") ){
                                   if ( loggingEnabled.equalsIgnoreCase("Y") ){
                                     logger.debug("Value - " + x)   
                                   }
                                   pivotColumns ++= MLinkedHashMap( x -> concatcolumns.toArray )
                                   //concatcolumns.clear() this is commented on 03/20/2018 
                                   //It will case issue in case value column is of STRUCT type and has 2 childs - _Value and _Qual
                                 }
                           }
             
             //pivotColumns.foreach(f => logger.debug( "Key - " +  f._1 + " Values - " + f._2.foreach( x => logger.debug("Names - " + x) ) )) 
             //val col = fieldNames.map { x => if ( x.toLowerCase().contains("name") ) x }.mkString(",")
             //val col = fieldNames.filter { x => x.toLowerCase().contains("name") }.mkString(",")
             //logger.debug( "col - " + col)
             
             pivotColumns.foreach{ x =>
                  logger.debug( "Processing for - " + x._1 )
                  val pivotedValues = 
                             dfFlexAttr
                             .withColumn("pivoted_column", concat( x._2.map { y => col(y) }:_* ) )  //, lit("_") if ( y != "null" ) col(y) else null  didnt work
                             .groupBy("item_gtin", "documentid")
                             .pivot("pivoted_column")
                             //.agg("item_flex_attrgroupmany_row_attr_value")
                             //.pivot("item_flex_attrgroupmany_name","item_flex_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_name","item_flex_attrgroupmany_row_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name","item_flex_attrgroupmany_row_attrmany_name","item_flex_attrgroupmany_row_attrqual_name","item_flex_attrgroupmany_row_attrqualmany_name")
                             //.max("item_flex_attr_value")
                             .agg( min( x._1 ) )
                             .distinct()
                             //, avg("item_flex_attrgroupmany_row_attr_value"), max("item_flex_attrgroupmany_row_attr_value") )
                             //.filter("item_gtin = '00044000032029'")
                  //pivotedValues.show()
                                    
                  val pivotedFields = pivotedValues.schema.fieldNames.filter { x => x != "null" }.map { x => x.toLowerCase() }   //
                  val pivotedValuesWONull = pivotedValues.select( pivotedFields.filter { x => x != "null" }.head
                                                                 ,pivotedFields.filter { x => x != "null" }.tail:_*
                                                                 )
                  //val pivotedValuesWONull  = pivotedValues.toDF( pivotedFields:_* )
                  
                  logger.debug("Without null")    
                  if ( loggingEnabled.equalsIgnoreCase("Y") ){
                      pivotedValuesWONull.printSchema()
                      pivotedValuesWONull.show()
                  }
                  
                  conn.withSessionDo { x => pivotedFields.map { y => //logger.debug(" Check column existense for " + y )
                                                                           if ( !( newdocTableList.contains(y.toLowerCase()) ) && y.toLowerCase() != "null" ) {
                                                                              logger.debug(" Run alter on mdm.item_details_by_doc_id for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_doc_id ADD " + y + " text ")
                                                                              newdocTableList ++= MSeq(y)
                                                                           }
                                                                           
                                                                           if ( !( newgtinTableList.contains(y.toLowerCase()) ) && y.toLowerCase() != "null" ) {
                                                                              logger.debug(" Run alter on mdm.item_details_by_gtin for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_gtin ADD " + y + " text ")
                                                                              newgtinTableList ++= MSeq(y)
                                                                           }
                                                                     }  
                                            }
                  
                 saveDataFrameM("table", pivotedValuesWONull.withColumn("update_time", lit(getCurrentTime()) )
                                                            //.withColumn("file_name", lit(fileName) )
                                       , "", "null", "mdm", "item_details_by_doc_id", "dev") 
                 saveDataFrameM("table", pivotedValuesWONull.withColumn("update_time", lit(getCurrentTime()) )
                                                           // .withColumn("file_name", lit(fileName) )                      
                                       , "", "null", "mdm", "item_details_by_gtin", "dev")
             }
                                 
             /*val pivotedValues = 
               dfFlexAttr
               .withColumn("pivoted_column", concat($"item_flex_attrgroupmany_name", lit("_"), $"item_flex_attrgroupmany_row_attr_name") )
               .groupBy("item_gtin", "documentid")
               .pivot("pivoted_column")
               //.agg("item_flex_attrgroupmany_row_attr_value")
               //.pivot("item_flex_attrgroupmany_name","item_flex_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_name","item_flex_attrgroupmany_row_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name","item_flex_attrgroupmany_row_attrmany_name","item_flex_attrgroupmany_row_attrqual_name","item_flex_attrgroupmany_row_attrqualmany_name")
               //.max("item_flex_attr_value")
               .agg( min("item_flex_attrgroupmany_row_attr_value") )
               //, avg("item_flex_attrgroupmany_row_attr_value"), max("item_flex_attrgroupmany_row_attr_value") )
               .filter("item_gtin = '00044700079294'")
             
             /*val pivotedValues = dfFlexAttr.groupBy("item_gtin", "documentid")
             .pivot(  fieldNames.filter { x => x.toLowerCase().contains("name") }.mkString(",")  )
             //.max("item_flex_attr_value")
             .agg( max( fieldNames.filter { x => x.toLowerCase().contains("value") }.mkString(",") ) )
             .filter("item_gtin = '00044700079294'")*/
             
             pivotedValues.show()
             saveDataFrameM("file", pivotedValues, "", "pivotedValues", null, null, "dev")*/
             
    		 }
    		 catch {
    		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
    			    	logger.error("XML file pivotal child error for " + tableName + " : " + e.getMessage )
                e.printStackTrace()
                status = "1"
                message = "XML file pivotal child error for " + tableName + " : " + e.getMessage
                //throw new Exception("Error while processing xml File at : " + e.getMessage + ": " + e.printStackTrace())
    			} 
    		 return ( status, message )    		 
   }
  
   def pivotalLink(sqlContext: SQLContext, attrName: String, tableName: String, fileName: String ): ( String, String ) =  {  //, docTableList: MSeq[String], gtinTableList: MSeq[String]
       logger.info(" - pivotalLink - " + "\n" 
                 + " - fileName - " + fileName + "\n"
                 + " - attrName - " + attrName + "\n"
                 + " - tableName - " + tableName + "\n"
                  );
    		 val currentTime = getCurrentTime()
    		 var status = "0"
    		 var message: String = null
    		 
    		 val dfItemDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_doc_id", "keyspace" -> "mdm") )
            .load();  
     
        val dfItemDetailsGtin = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_gtin", "keyspace" -> "mdm") )
            .load();
     
    		 var newdocTableList: Array[String] = dfItemDetails.schema.fieldNames
    		 var newgtinTableList: Array[String] = dfItemDetails.schema.fieldNames
    		 
    		 //import sqlContext.implicits._ 
         try{
             val dfFlexAttr = sqlContext.read
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "mdm")
                .option("table", tableName ) 
                .load()
                .filter(s" file_name = '$fileName' AND active = 'Y' ")
             
             /*dfFlexAttr.registerTempTable("item_link_attribs")
             
             val maxCountQuery = """ SELECT MAX( COUNT(1) ) max_count
                                       FROM item_link_attribs
                                   GROUP BY link_parentitem_gtin,documentid
                                 """
             logger.debug( " - maxCountQuery- " + maxCountQuery )
             val maxCount = sqlContext.sql(maxCountQuery)
             val maxchilds = maxCount.select("max_count").collect().apply(0).getInt(0)
             logger.debug( " - maxchilds - " + maxchilds )
             
             val start = new RichInt(1)
             
             val pivotalColumns = for (quick <- start to maxchilds ) yield concat( lit("link_parentitem_childitem_value_"), lit(quick) ) 
             */
             
             //val pivotColumns: MLinkedHashMap[String, Array[String]] = MLinkedHashMap[String, Array[String] ]()
             //var concatcolumns: MArrayBuffer[String] = MArrayBuffer[String]()
             
            /*val distinctRows = dfFlexAttr
                          /*.select(col("link_parentitem_gtin").alias("item_gtin"), col("documentid")
                                ,col("link_parentitem_numberofuniquegtins"), col("link_parentitem_totalquantityofnextlowerleveltradeitem")
                                ,col("link_parentitem_childitem_quantity"), col("link_parentitem_childitem_value")
                                )
                          .filter("link_parentitem_gtin = '00044000044152'")
                          .map( row => ( row.getString(0), row.getString(1), row.getString(2), row.getString(3) ) -> (row.getString(4), row.getString(5)) )
                          .groupByKey()*/
                          //.count()
                          .groupBy( col("link_parentitem_gtin").alias("item_gtin"), col("documentid"), col("link_parentitem_numberofuniquegtins"), col("link_parentitem_totalquantityofnextlowerleveltradeitem") )
                          .max(  )
                          //.agg( max() )*/
             
              /*val pivotedValues =  dfFlexAttr
                             //.filter("link_parentitem_gtin = '00044000044152'")
                             .withColumn("pivotal_column", concat( lit("link_parentitem_childitem_qty_"), col("link_parentitem_childitem_value") ) )
                             .groupBy( col("link_parentitem_gtin").alias("item_gtin"), col("documentid"), col("link_parentitem_numberofuniquegtins"), col("link_parentitem_totalquantityofnextlowerleveltradeitem") )
                             .pivot("pivotal_column") //"link_parentitem_numberofuniquegtins", "link_parentitem_totalquantityofnextlowerleveltradeitem", "link_parentitem_childitem_value")
                             //.agg("item_flex_attrgroupmany_row_attr_value")
                             //.pivot("item_flex_attrgroupmany_name","item_flex_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_name","item_flex_attrgroupmany_row_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name","item_flex_attrgroupmany_row_attrmany_name","item_flex_attrgroupmany_row_attrqual_name","item_flex_attrgroupmany_row_attrqualmany_name")
                             //.max("item_flex_attr_value")
                             .agg( min("link_parentitem_childitem_quantity") )
                             //.max("link_parentitem_childitem_quantity")
                             .distinct()
                             //, avg("item_flex_attrgroupmany_row_attr_value"), max("item_flex_attrgroupmany_row_attr_value") )
                             */
             
             //val maxColumns = 5
             //for ( index <- 1 to 5 ) println(index)
             //for ( q <- 1.toInt to maxchilds )
             
             val pivotedValues =  dfFlexAttr
                             //.filter("link_parentitem_gtin = '00044000044152'")
                             //.withColumn("pivotal_column", pivotalColumns:_* )
                             .groupBy( col("link_parentitem_gtin").alias("item_gtin"), col("documentid"), col("link_parentitem_numberofuniquegtins"), col("link_parentitem_totalquantityofnextlowerleveltradeitem") )
                             //.pivot("pivotal_column") //"link_parentitem_numberofuniquegtins", "link_parentitem_totalquantityofnextlowerleveltradeitem", "link_parentitem_childitem_value")
                             //.agg("item_flex_attrgroupmany_row_attr_value")
                             //.pivot("item_flex_attrgroupmany_name","item_flex_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_name","item_flex_attrgroupmany_row_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name","item_flex_attrgroupmany_row_attrmany_name","item_flex_attrgroupmany_row_attrqual_name","item_flex_attrgroupmany_row_attrqualmany_name")
                             //.max("item_flex_attr_value")
                             .agg( max("link_parentitem_childitem_value").alias("link_parentitem_childitem_value"), max("link_parentitem_childitem_quantity").alias("link_parentitem_childitem_quantity") )
                             //.max("link_parentitem_childitem_quantity")
                             .distinct()
                             //, avg("item_
              
                logger.debug("With null")                                              
                if ( loggingEnabled.equalsIgnoreCase("Y") ){
                  pivotedValues.printSchema()
                  pivotedValues.show()
                }
              //pivotedValues.show()
              
              val pivotedFields = pivotedValues.schema.fieldNames.filter { x => x != "null" }.map { x => x.toLowerCase() }   //
              val pivotedValuesWONull = pivotedValues.select( pivotedFields.filter { x => x != "null" }.head
                                                                 ,pivotedFields.filter { x => x != "null" }.tail:_*
                                                                 )
                //val pivotedValuesWONull  = pivotedValues.toDF( pivotedFields:_* )
                
                logger.debug("Without null")                                              
                if ( loggingEnabled.equalsIgnoreCase("Y") ){
                  pivotedValuesWONull.printSchema()
                  pivotedValuesWONull.show()
                }
                             
                conn.withSessionDo { x => pivotedFields.map { y => //logger.debug(" Check column existense for " + y )
                                                                         if ( !( newdocTableList.contains(y.toLowerCase()) ) && y.toLowerCase() != "null" ) {
                                                                            logger.debug(" Run alter on mdm.item_details_by_doc_id for column " + y )
                                                                            x.execute("ALTER TABLE mdm.item_details_by_doc_id ADD " + y + " text ")
                                                                            newdocTableList ++= MSeq(y)
                                                                         }
                                                                         
                                                                         if ( !( newgtinTableList.contains(y.toLowerCase()) ) && y.toLowerCase() != "null" ) {
                                                                            logger.debug(" Run alter on mdm.item_details_by_gtin for column " + y )
                                                                            x.execute("ALTER TABLE mdm.item_details_by_gtin ADD " + y + " text ")
                                                                            newgtinTableList ++= MSeq(y)
                                                                         }
                                                                   }  
                                          }
                
               saveDataFrameM("table", pivotedValuesWONull.withColumn("update_time", lit(getCurrentTime()) )
                                                          .filter(" item_gtin is not null ")
                                                          //.withColumn("file_name", lit(fileName) )
                                     , "", "null", "mdm", "item_details_by_doc_id", "dev") 
               saveDataFrameM("table", pivotedValuesWONull.withColumn("update_time", lit(getCurrentTime()) )
                                                          .filter(" item_gtin is not null ")
                                                         //.withColumn("file_name", lit(fileName) )                      
                                     , "", "null", "mdm", "item_details_by_gtin", "dev")
              
              /*val fieldNames = dfFlexAttr.schema.fieldNames
             fieldNames.map { x => if ( ( x.endsWith("name") || x.endsWith("qual") ) && !(x.equalsIgnoreCase("file_name") ) ){
                                      //logger.debug("Name or qual - " + x) 
                                      concatcolumns ++= Seq(x)
                                   }
                                 else if ( x.endsWith("value") ){
                                   //logger.debug("Value - " + x)   
                                   pivotColumns ++= MLinkedHashMap( x -> concatcolumns.toArray )
                                   concatcolumns.clear()
                                 }
                           }
                                                 
              pivotColumns.foreach{ x =>
                  logger.debug( "Processing for - " + x._1 )
                  val pivotedValues = 
                             dfFlexAttr
                             .withColumn("pivoted_column", concat( x._2.map { y => col(y) }:_* ) )  //, lit("_") if ( y != "null" ) col(y) else null  didnt work
                             .groupBy("item_gtin", "documentid")
                             .pivot("pivoted_column")
                             //.agg("item_flex_attrgroupmany_row_attr_value")
                             //.pivot("item_flex_attrgroupmany_name","item_flex_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_name","item_flex_attrgroupmany_row_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name","item_flex_attrgroupmany_row_attrmany_name","item_flex_attrgroupmany_row_attrqual_name","item_flex_attrgroupmany_row_attrqualmany_name")
                             //.max("item_flex_attr_value")
                             .agg( min( x._1 ) )
                             .distinct()
                             //, avg("item_flex_attrgroupmany_row_attr_value"), max("item_flex_attrgroupmany_row_attr_value") )
                             //.filter("item_gtin = '00044000032029'")
                  pivotedValues.show()
                                    
                  val pivotedFields = pivotedValues.schema.fieldNames.filter { x => x != "null" }.map { x => x.toLowerCase() }   //
                  val pivotedValuesWONull = pivotedValues.select( pivotedFields.filter { x => x != "null" }.head
                                                                 ,pivotedFields.filter { x => x != "null" }.tail:_*
                                                                 )
                  //val pivotedValuesWONull  = pivotedValues.toDF( pivotedFields:_* )
                  
                  logger.debug("Without null")                                              
                  pivotedValuesWONull.show()
                               
                  conn.withSessionDo { x => pivotedFields.map { y => //logger.debug(" Check column existense for " + y )
                                                                           if ( !( newdocTableList.contains(y.toLowerCase()) ) && y.toLowerCase() != "null" ) {
                                                                              logger.debug(" Run alter on mdm.item_details_by_doc_id for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_doc_id ADD " + y + " text ")
                                                                              newdocTableList ++= MSeq(y)
                                                                           }
                                                                           
                                                                           if ( !( newgtinTableList.contains(y.toLowerCase()) ) && y.toLowerCase() != "null" ) {
                                                                              logger.debug(" Run alter on mdm.item_details_by_gtin for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_gtin ADD " + y + " text ")
                                                                              newgtinTableList ++= MSeq(y)
                                                                           }
                                                                     }  
                                            }
                  
                 saveDataFrameM("table", pivotedValuesWONull.withColumn("update_time", lit(getCurrentTime()) )
                                                            .withColumn("file_name", lit(fileName) )
                                       , "", "null", "mdm", "item_details_by_doc_id", "dev") 
                 saveDataFrameM("table", pivotedValuesWONull.withColumn("update_time", lit(getCurrentTime()) )
                                                            .withColumn("file_name", lit(fileName) )                      
                                       , "", "null", "mdm", "item_details_by_gtin", "dev")
                                    
               }*/
                                 
             /*val pivotedValues = 
               dfFlexAttr
               .withColumn("pivoted_column", concat($"item_flex_attrgroupmany_name", lit("_"), $"item_flex_attrgroupmany_row_attr_name") )
               .groupBy("item_gtin", "documentid")
               .pivot("pivoted_column")
               //.agg("item_flex_attrgroupmany_row_attr_value")
               //.pivot("item_flex_attrgroupmany_name","item_flex_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_name","item_flex_attrgroupmany_row_attrgroupmany_row_attr_name","item_flex_attrgroupmany_row_attrgroupmany_row_attrqualmany_name","item_flex_attrgroupmany_row_attrmany_name","item_flex_attrgroupmany_row_attrqual_name","item_flex_attrgroupmany_row_attrqualmany_name")
               //.max("item_flex_attr_value")
               .agg( min("item_flex_attrgroupmany_row_attr_value") )
               //, avg("item_flex_attrgroupmany_row_attr_value"), max("item_flex_attrgroupmany_row_attr_value") )
               .filter("item_gtin = '00044700079294'")
             
             /*val pivotedValues = dfFlexAttr.groupBy("item_gtin", "documentid")
             .pivot(  fieldNames.filter { x => x.toLowerCase().contains("name") }.mkString(",")  )
             //.max("item_flex_attr_value")
             .agg( max( fieldNames.filter { x => x.toLowerCase().contains("value") }.mkString(",") ) )
             .filter("item_gtin = '00044700079294'")*/
             
             pivotedValues.show()
             saveDataFrameM("file", pivotedValues, "", "pivotedValues", null, null, "dev")*/
             
    		 }
    		 catch {
    		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
    			    	logger.error("XML file pivotal link error for " + tableName + " : " + e.getMessage )
                e.printStackTrace()
                status = "1"
                message = "XML file pivotal link error for " + tableName + " : " + e.getMessage
                //throw new Exception("Error while processing xml File at : " + e.getMessage + ": " + e.printStackTrace())
    			} 
    		 return ( status, message )    		 
   }
   
   def explodeChild (sc: SparkContext, sqlContext: SQLContext
                    ,cassandraConnector: CassandraConnector 
                    ,xmlFileStream: DataFrame, attrName: String 
                    ,tableName: String ) = {
       
        logger.info(" - explodeChild - ");
    		 val currentTime = getCurrentTime()
         var status = "0";
         var validationStatus = "Inserted"
         var errorMessage: String = null
    
         var cffPath = ""
         var fileSchema = "" 
         var explodeArray = ""
         var trimZeroes = ""
         var intFieldIndexes = ""
         
         var xmlSchema: StructType = new StructType()
         var xmlSchemaArray: StructType = new StructType()
         var xmlSchemaExplodeArray1: StructType = new StructType()
         var xmlSchemaExplodeArray2: StructType = new StructType()
         var xmlSchemaExplodeArray3: StructType = new StructType()
         var xmlSchemaExplodeArray4: StructType = new StructType()
         var xmlSchemaExplodeArray5: StructType = new StructType()
         
         var arrayElements1:MSeq[String] = MSeq.empty
         var arrayElements2:MSeq[String] = MSeq.empty
         var arrayElements3:MSeq[String] = MSeq.empty
         var arrayElements4:MSeq[String] = MSeq.empty
         var arrayElements5:MSeq[String] = MSeq.empty
         
         var xmlSchemaExplod: Seq[String] = Seq[String]()   // just for acumulation
         
         var arrayExplode = 1;
         var arrayExplodeCurrentLevel = 0;
         var arrayExplodeLevel = 3;
         
         import sqlContext.implicits._ 
         
         val dfFlexAttr = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "mdm")
            .option("table", tableName ) 
            //.options( Map("table" -> "item_details_by_gtin", "keyspace" -> "mdm") )
            .load();
         
         var tableColList: MSeq[String] = MSeq.empty
         dfFlexAttr.schema.fields.map { x => tableColList ++= MSeq(x.name) }
         logger.debug( " - tableColList - " + tableColList )
         
         val xmlFileAttr = xmlFileStream.select("documentId", "item_gtin", attrName)
                           //.filter("item_gtin = '20044700079298'")
         xmlFileAttr.printSchema()
         xmlFileAttr.show()
         xmlFileAttr.registerTempTable("item_file_attr")
         //var attribRows: Row<> = Row.
         
         //val xmlFilwAttrRow = xmlFileAttr.map { x => Row( x.getString(0), x.getLong(1), XML.load(x.getAs(WrapedArray)(2) ) ) }
         //xmlFilwAttrRow.foreach { println }
         
         //xmld
         //doc.GetElementsByTagName("dt");      
         
         val xmlFileAttrLevel1 = xmlFileAttr.select($"documentid", $"item_gtin", explode($"item_flex_attrgroupmany").alias("item_flex_attrgroupmany"))
         xmlFileAttrLevel1.show()
         
         val xmlFileAttrLevel12 = xmlFileAttrLevel1   //.select($"documentid", $"item_gtin", explode($"item_flex_attrgroupmany").alias("item_flex_attrgroupmany"))
                                  .select($"documentid", $"item_gtin", $"item_flex_attrgroupmany._name".alias("item_flex_attrgroupmany_name")
                                         ,explode($"item_flex_attrgroupmany.row").alias("item_flex_attrgroupmany_row"))
                                  .select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                        ,$"item_flex_attrgroupmany_row.attr".alias("item_flex_attrgroupmany_row_attr")
                                        ,$"item_flex_attrgroupmany_row.attrGroupMany".alias("item_flex_attrgroupmany_row_attrGroupMany")
                                        ,$"item_flex_attrgroupmany_row.attrMany".alias("item_flex_attrgroupmany_row_attrMany")
                                        ,$"item_flex_attrgroupmany_row.attrQual".alias("item_flex_attrgroupmany_row_attrQual")
                                        ,$"item_flex_attrgroupmany_row.attrQualMany".alias("item_flex_attrgroupmany_row_attrQualMany")
                                        )
         xmlFileAttrLevel12.show()          
                          
         val xmlFileAttrLevel1231 = xmlFileAttrLevel12.select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                         ,explode($"item_flex_attrgroupmany_row_attr").alias("item_flex_attrgroupmany_row_attr")
                                                         )
                                                   .select( $"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                          ,$"item_flex_attrgroupmany_row_attr._value".alias("item_flex_attrgroupmany_row_attr_value")
                                                          ,$"item_flex_attrgroupmany_row_attr._name".alias("item_flex_attrgroupmany_row_attr_name")
                                                          ,lit("attr").alias("type")
                                                          )
         xmlFileAttrLevel1231.show()
         
         val xmlFileAttrLevel1232 = xmlFileAttrLevel12.select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                         ,explode($"item_flex_attrgroupmany_row_attrQual").alias("item_flex_attrgroupmany_row_attrQual")
                                                         )
                                                   .select( $"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                          ,$"item_flex_attrgroupmany_row_attrQual._value".alias("item_flex_attrgroupmany_row_attrQualr_value")
                                                          ,$"item_flex_attrgroupmany_row_attrQual._name".alias("item_flex_attrgroupmany_row_attrQual_name")
                                                          ,$"item_flex_attrgroupmany_row_attrQual._qual".alias("item_flex_attrgroupmany_row_attrQual_qual")
                                                          ,lit("attrQual").alias("type")
                                                          )
         xmlFileAttrLevel1232.show()
         
         val xmlFileAttrLevel1233 = xmlFileAttrLevel12.select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                         ,explode($"item_flex_attrgroupmany_row_attrQualMany").alias("item_flex_attrgroupmany_row_attrQualMany")
                                                         )
                                                   .select( $"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                          ,$"item_flex_attrgroupmany_row_attrQualMany._name".alias("item_flex_attrgroupmany_row_attrQualMany_name")
                                                          ,$"item_flex_attrgroupmany_row_attrQualMany.value._value".alias("item_flex_attrgroupmany_row_attrQualMany_value_value")
                                                          ,$"item_flex_attrgroupmany_row_attrQualMany.value._qual".alias("item_flex_attrgroupmany_row_attrQualMany_value_qual")
                                                          ,lit("attrQualMany").alias("type")
                                                          )
         xmlFileAttrLevel1233.show()
         
         val xmlFileAttrLevel1234 = xmlFileAttrLevel12.select( $"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                          ,$"item_flex_attrgroupmany_row_attrMany._name".alias("item_flex_attrgroupmany_row_attrMany_name")
                                                          ,$"item_flex_attrgroupmany_row_attrMany.value".alias("item_flex_attrgroupmany_row_attrMany_value")
                                                          ,lit("attrMany").alias("type")
                                                          )
         xmlFileAttrLevel1234.show()
         
         val xmlFileAttrLevel125 = xmlFileAttrLevel12.select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany._name".alias("item_flex_attrgroupmany_row_attrGroupMany_name")
                                                         ,explode($"item_flex_attrgroupmany_row_attrGroupMany.row").alias("item_flex_attrgroupmany_row_attrGroupMany_row")     
                                                         )
                                                   .select( $"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                          ,$"item_flex_attrgroupmany_row_attrGroupMany_name".alias("item_flex_attrgroupmany_row_attrGroupMany_name")
                                                          ,$"item_flex_attrgroupmany_row_attrGroupMany_row.attr".alias("item_flex_attrgroupmany_row_attrGroupMany_row_attr")
                                                          ,$"item_flex_attrgroupmany_row_attrGroupMany_row.attrQualMany._name".alias("item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_name")
                                                          ,$"item_flex_attrgroupmany_row_attrGroupMany_row.attrQualMany.value._qual".alias("item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_value_qual")
                                                          ,$"item_flex_attrgroupmany_row_attrGroupMany_row.attrQualMany.value._value".alias("item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_value_value")
                                                          ,lit("attrGroupMany").alias("type")
                                                          )
                                                   .select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_name"
                                                         ,explode($"item_flex_attrgroupmany_row_attrGroupMany_row_attr").alias("item_flex_attrgroupmany_row_attrGroupMany_row_attr")                                                         
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_name"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_value_qual"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_value_value"
                                                         ,$"type"
                                                          )
                                                    .select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_name"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attr._name".alias("item_flex_attrgroupmany_row_attrGroupMany_row_attr_name")
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attr._value".alias("item_flex_attrgroupmany_row_attrGroupMany_row_attr_value")
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_name"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_value_qual"
                                                         ,$"item_flex_attrgroupmany_row_attrGroupMany_row_attrQualMany_value_value"
                                                         ,$"type"
                                                          )
         xmlFileAttrLevel125.show()
         
         /*
          * ,flex_attrQualMany_name, flex_attrQualMany_value
                                                       --,flex_attrQualMany_value._value, flex_attrQualMany_value._qual
                                                       ,flex_attrGroupMany_name   
                                                       ,flex_attrGroupMany_row_attr, flex_attrGroupMany_row_attrGroupMany
                                                       --,flex_attrGroupMany_row_attrGroupMany._name, flex_attrGroupMany_row_attrGroupMany.row
                                                       ,flex_attrGroupMany_row_attrQual, flex_attrGroupMany_row_attrQualMany
                                                       --, flex_attrGroupMany_row
                                                       --, flex_attrGroupMany_row._name
                                                       --,flex_attrGroupMany_row.getItem(0) flex_attrGroupMany_row_name
                                                       --,flex_attrGroupMany_row.getItem(1) flex_attrGroupMany_row_row
                                                       -- flex_attrGroupMany_name_row
          */
         val xmlfileMultipleExplodeQuery = s""" SELECT documentId, gtin,  flex_attrGroupMany.*
                                                      ,flex_attrGroupMany_row.*                                  
                                                   FROM item_xml_file
                                                LATERAL VIEW OUTER inline(flex.attrGroupMany) flex_attrGroupMany 
                                                LATERAL VIEW OUTER inline(item.flex_attrGroupMany.row) flex_attrGroupMany_row 
                                                --LATERAL VIEW OUTER inline(item.flex_attrGroupMany_row_attrGroupMany) flex_attrGroupMany_row_attrGroupMany as flex_attrGroupMany_row_attrGroupMany_name, flex_attrGroupMany_row_attrGroupMany_row
                                            """
          logger.debug( " - xmlfileMultipleExplodeQuery- " + xmlfileMultipleExplodeQuery )
          val xmlfileMultipleExplode = sqlContext.sql(xmlfileMultipleExplodeQuery)
          xmlfileMultipleExplode.show()
          xmlfileMultipleExplode.printSchema()
          saveDataFrameM("file", xmlfileMultipleExplode, "", "xmlfileMultipleExplode", null, null, null) 
          
         /*val xmlfileMultipleExplodeQuery = s"""  SELECT documentId, item_gtin
                                                      ,item_flex_attrgroupmany_name
                                                      ,flex_attrGroupMany_row_attr_name
                                                      ,flex_attrGroupMany_row_attr_value
                                                      ,flex_attrGroupMany_row_attrQual_name
                                                      ,flex_attrGroupMany_row_attrQual_value
                                                      ,flex_attrGroupMany_row_attrQual_qual   
                                                      ,flex_attrGroupMany_row_attrQualmany_name
                                                      ,flex_attrGroupMany_row_attrQualmany_value.*   --._value
                                                      --,flex_attrGroupMany_row_attrQualmany_value._qual
                                                      ,flex_attrGroupMany_row_attrmany.*           --_name      
                                                      ,flex_attrGroupMany_row_attrGroupMany.*                                                   
                                                      ,flex_attrGroupMany_row_attrGroupMany_row_attr_value
                                                      ,flex_attrGroupMany_row_attrGroupMany_row_attr_name  
                                                      ,flex_attrGroupMany_row_attrGroupMany_row_attrqualmany.*
                                                   FROM item_xml_file
                                                LATERAL VIEW OUTER inline(item_flex_attrgroupmany) item_flex_attrgroupmany as item_flex_attrgroupmany_name, item_flex_attrgroupmany_row
                                                LATERAL VIEW OUTER inline(item_flex_attrgroupmany_row) flex_attrGroupMany_row as flex_attrGroupMany_row_attr, flex_attrGroupMany_row_attrGroupMany, flex_attrGroupMany_row_attrmany, flex_attrGroupMany_row_attrQual, flex_attrGroupMany_row_attrQualmany
                                                LATERAL VIEW OUTER inline(flex_attrGroupMany_row_attr) flex_attrGroupMany_row_attr as flex_attrGroupMany_row_attr_value, flex_attrGroupMany_row_attr_name
                                                LATERAL VIEW OUTER inline(flex_attrGroupMany_row_attrQual) flex_attrGroupMany_row_attrQual as flex_attrGroupMany_row_attrQual_value, flex_attrGroupMany_row_attrQual_name, flex_attrGroupMany_row_attrQual_qual
                                                LATERAL VIEW OUTER inline(flex_attrGroupMany_row_attrQualmany) flex_attrGroupMany_row_attrQualmany  as flex_attrGroupMany_row_attrQualmany_name, flex_attrGroupMany_row_attrQualmany_value
                                                LATERAL VIEW OUTER inline(flex_attrGroupMany_row_attrGroupMany.row) flex_attrGroupMany_row_attrGroupMany_row as flex_attrGroupMany_row_attrGroupMany_row_attr, flex_attrGroupMany_row_attrGroupMany_row_attrqualmany   --_name, flex_attrGroupMany_row_attrGroupMany_row_attrqualmany_value
                                                LATERAL VIEW OUTER inline(flex_attrGroupMany_row_attrGroupMany_row_attr) flex_attrGroupMany_row_attrGroupMany_row_attr as flex_attrGroupMany_row_attrGroupMany_row_attr_value, flex_attrGroupMany_row_attrGroupMany_row_attr_name
                                            """
          logger.debug( " - xmlfileMultipleExplodeQuery- " + xmlfileMultipleExplodeQuery )
          val xmlfileMultipleExplode = sqlContext.sql(xmlfileMultipleExplodeQuery)
          xmlfileMultipleExplode.show()
          xmlfileMultipleExplode.printSchema()*/
          //saveDataFrameM("file", xmlfileMultipleExplode, "", "xmlfileMultipleExplode", null, null, null)
          //xmlfileMultipleExplode.registerTempTable("xml_file_explode")
          
           /*val xmlfileMultipleExplodeValue = xmlfileMultipleExplode
                                .select($"documentid", $"item_gtin", $"item_flex_attrgroupmany_name"
                                       ,$"flex_attrGroupMany_row_attr_name"
                                       ,$"flex_attrGroupMany_row_attr_value"
                                       ,$"flex_attrGroupMany_row_attrQual_name"
                                       ,$"flex_attrGroupMany_row_attrQual_value"
                                       ,$"flex_attrGroupMany_row_attrQual_qual"
                                       ,$"flex_attrGroupMany_row_attrQualmany_name"
                                       ,$"flex_attrGroupMany_row_attrQualmany_value._qual"
                                       ,$"flex_attrGroupMany_row_attrQualmany_value._value"
                                       ,$"flex_attrGroupMany_row_attrmany._name"
                                       ,$"flex_attrGroupMany_row_attrmany.value"                                       
                                       ,$"flex_attrGroupMany_row_attrGroupMany._name"
                                       ,$"flex_attrGroupMany_row_attrGroupMany_row_attr_name"
                                       ,$"flex_attrGroupMany_row_attrGroupMany_row_attr_value"
                                       ,$"flex_attrGroupMany_row_attrGroupMany_row_attrqualmany._name"
                                       //,$"flex_attrGroupMany_row_attrGroupMany_row_attrqualmany.value"
                                       ,$"flex_attrGroupMany_row_attrGroupMany_row_attrqualmany.value._value"
                                       ,$"flex_attrGroupMany_row_attrGroupMany_row_attrqualmany.value._qual"
                                       )
          xmlfileMultipleExplodeValue.show()
          xmlfileMultipleExplodeValue.printSchema()
          saveDataFrameM("file", xmlfileMultipleExplodeValue, "", "xmlfileMultipleExplodeValue", null, null, null)
          */
          /*val xplodedQuery = s"""SELECT documentid, item_gtin, item_flex_attrgroupmany_name
                                       ,flex_attrGroupMany_row_attr_name
                                      ,flex_attrGroupMany_row_attr_value
                                      ,flex_attrGroupMany_row_attrQual_name
                                      ,flex_attrGroupMany_row_attrQual_value
                                      ,flex_attrGroupMany_row_attrQual_qual   
                                      ,flex_attrGroupMany_row_attrQualmany_name  
                                      ,flex_attrGroupMany_row_attrQualmany_value._value
                                      ,flex_attrGroupMany_row_attrQualmany_value._qual
                                   FROM xml_file_explode 
                              """
          logger.debug( " - xplodedQuery- " + xplodedQuery )
          val xploded = sqlContext.sql(xplodedQuery)
          xploded.show()
          xploded.printSchema()*/
          
          // Explode Child Flex Attributes 
          //explodeChild (sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrgroupmany", "item_attrgroupmany_details" )
          
         val explodeQuery = s""" SELECT * 
                                   FROM item_file_attr
                                LATERAL VIEW OUTER inline("item_flex_attrgroupmany") item_flex_attrgroupmany 
                             """
         
                                /* --LATERAL VIEW OUTER inline("item_flex_attrgroupmany.row") item_flex_attrgroupmany_row
                                 --LATERAL VIEW OUTER inline("item_flex_attrgroupmany_row.attr") item_flex_attrgroupmany_row_attr
                                 --LATERAL VIEW OUTER inline("item_flex_attrgroupmany_row.attrqual") attr
             */
         logger.debug( "  - explodeQuery - " + explodeQuery )
         val explodeData = sqlContext.sql(explodeQuery)
         explodeData.show
         explodeData.printSchema()
         
          /*cassandraConnector.withSessionDo { x => colListsAlias.map { y => //logger.debug(" Check column existense for " + y )
                                                                           if ( !( tableColList.contains(y) ) ) {
                                                                              logger.debug(" Run alter for column " + y )
                                                                              x.execute("ALTER TABLE mdm.item_details_by_doc_id ADD " + y + " text ")
                                                                           }
                                                                     }  
                                            }
          */
         /*var structLevels: MLinkedHashMap[String, Int] = MLinkedHashMap().empty
         
                   // Not Used
          /*if ( colLists.contains("item_flex_attrgroupmany") ) {
             explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrgroupmany", "item_attrgroupmany_details" )
          }
          explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrqualmany", "item_attrqualmany_details" )
          explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attr", "item_attr_details" )
          explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrgroup", "item_attrgroup_details" )
          explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrmany", "item_attrmany_details" )
          explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrqual", "item_attrqual_details" )
          explodeChildLiteral(sc, sqlContext, cassandraConnector, xmlFileStreamExploded, "item_flex_attrgroupmany", "item_attrgroupmany_details" )
          */
         def loop(path: String, dt: DataType, acc:Seq[String]): Seq[String] = {
              logger.debug( " - loop - " + "\t" + " - path - " + path + "\t" + " - dt - " + dt)
              dt match {
              case s: ArrayType =>
                   logger.debug( "path Level - " + structLevels.getOrElse(path, arrayExplodeLevel + 1) )
                   arrayExplodeCurrentLevel = arrayExplodeCurrentLevel + 1   
                   structLevels ++= MMap( path -> arrayExplodeCurrentLevel)
                   if ( arrayExplodeCurrentLevel == 0 || structLevels.getOrElse(path, arrayExplodeLevel + 1)  <= arrayExplodeLevel ){   //    //arrayExplode
                     logger.debug( "arrayExplode - " + arrayExplode ) 
                      if( arrayExplode == 1 ){
                        arrayElements1 ++= MSeq( path.toLowerCase() )
                        xmlSchemaExplodeArray1 = xmlSchemaExplodeArray1.add( StructField( path, dt, true ))
                      }
                      if( arrayExplode == 2 ){
                        arrayElements2 ++= MSeq( path.replace(".", "_").toLowerCase()  )
                        xmlSchemaExplodeArray2 = xmlSchemaExplodeArray2.add( StructField( path, dt, true ))
                      }
                      if( arrayExplode == 3 ){
                        arrayElements3 ++= MSeq( path.replace(".", "_").toLowerCase()  )
                        xmlSchemaExplodeArray3 = xmlSchemaExplodeArray3.add( StructField( path, dt, true ))
                      }
                      if( arrayExplode == 4 ){
                        arrayElements4 ++= MSeq( path.replace(".", "_").toLowerCase()  )
                        xmlSchemaExplodeArray4 = xmlSchemaExplodeArray4.add( StructField( path, dt, true ))
                      }
                      if( arrayExplode == 5 ){
                        arrayElements5 ++= MSeq( path.replace(".", "_").toLowerCase()  )
                        xmlSchemaExplodeArray5 = xmlSchemaExplodeArray5.add( StructField( path, dt, true ))
                      }
                      arrayExplode = arrayExplode + 1 
                     loop(path, s.elementType, acc )
                   }else{
                      xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))  
                   }
              case s: StructType =>                        
                   s.fields.flatMap(f => loop(path + "." + f.name, f.dataType, acc ))
              case other => 
                   acc:+ path
                   xmlSchemaArray = xmlSchemaArray.add( StructField( path, dt, true ))
                   if ( structLevels.getOrElse(path, arrayExplodeLevel + 1) <= arrayExplodeLevel ){   // Add elements only till arrayExplodeLevel
                      if( arrayExplode <= 1 ){
                         xmlSchemaExplodeArray1 = xmlSchemaExplodeArray1.add( StructField( path, dt, true ))
                      }
                     if( arrayExplode <= 2 ){
                        xmlSchemaExplodeArray2 = xmlSchemaExplodeArray2.add( StructField( path, dt, true ))
                     }
                     if( arrayExplode <= 2 ){
                        xmlSchemaExplodeArray3 = xmlSchemaExplodeArray3.add( StructField( path, dt, true ))
                     }
                     if( arrayExplode <= 2 ){
                        xmlSchemaExplodeArray4 = xmlSchemaExplodeArray4.add( StructField( path, dt, true ))
                     }
                     if( arrayExplode <= 2 ){
                        xmlSchemaExplodeArray5 = xmlSchemaExplodeArray5.add( StructField( path, dt, true ))
                     }
                   }
              }
              acc.foreach {  x => println ("acc - "  + x)  }
              acc 
          }
                       
          xmlFileAttr.schema.fields.map { x => //logger.debug( " index - " + xmlFileAttr.schema.fieldIndex(x.name)) 
                                               loop( x.name, x.dataType, xmlSchemaExplod ) }  
          //logger.debug("- xmlSchemaExplod - ")  
          //xmlSchemaExplod.foreach { println }         
          logger.debug(" - Print xmlSchemaArray Tree String - ")  
         
          structLevels.foreach(println)
          
          logger.debug(" - xmlSchemaExplodeArray1 - ") 
          xmlSchemaExplodeArray1.printTreeString()
          logger.debug(" - xmlSchemaExplodeArray2 - ") 
          xmlSchemaExplodeArray2.printTreeString()
          logger.debug(" - xmlSchemaExplodeArray3 - ") 
          xmlSchemaExplodeArray3.printTreeString()
          logger.debug(" - xmlSchemaArray - ") 
          xmlSchemaArray.printTreeString()   
          
          var colLists: MSeq[Column] = MSeq.empty
          var colListsAlias: MSeq[String] = MSeq.empty
          
          var colExplodeLists: MSeq[Column] = MSeq.empty
          var colExplodeListsAlias: MSeq[String] = MSeq.empty          
          var colExplodeLists2: MSeq[Column] = MSeq.empty
          var colExplodeListsAlias2: MSeq[String] = MSeq.empty
          var colExplodeLists3: MSeq[Column] = MSeq.empty
          var colExplodeListsAlias3: MSeq[String] = MSeq.empty
          
          xmlSchemaExplodeArray1.map{ x => { colExplodeLists ++= MSeq(col(x.name) ) 
                                             colExplodeListsAlias ++= MSeq(x.name.replace(".", "_").toLowerCase() )
                                          }
                                  }
          
          // Check if columnName exists in Explode List then replace it with explodeListAlias
          xmlSchemaExplodeArray2.map { x => { colExplodeLists2 ++= MSeq( col( if ( colExplodeListsAlias.contains( x.name.replace(".", "_").toLowerCase() ) )
                                                                                 x.name.replace(".", "_")  
                                                                             else x.name.toLowerCase()
                                                                            ) 
                                                                       ) 
                                              colExplodeListsAlias2 ++= MSeq( x.name.replace(".", "_").replace("__", "_").toLowerCase()  )
                                            }
                                      }
          
          xmlSchemaExplodeArray3.map { x => { colExplodeLists3 ++= MSeq( col( if ( colExplodeListsAlias2.contains( x.name.replace(".", "_").toLowerCase() ) )
                                                                                 x.name.replace(".", "_").replace("__", "_").toLowerCase()
                                                                             else replaceDot( x.name, 1 ).replace("__", "_").toLowerCase()
                                                                            ) 
                                                                       ) 
                                              colExplodeListsAlias3 ++= MSeq( x.name.replace(".", "_").replace("__", "_").toLowerCase()  )
                                            }
                                      }
          
          xmlSchemaArray.map { x => { colLists ++= MSeq( col( if ( colExplodeListsAlias2.contains( x.name.replace(".", "_").replace("__", "_").toLowerCase() ) ){
                                                                   //logger.debug( "contains - " + x.name.replace(".", "_").replace("__", "_") ) 
                                                                   //regex.replaceFirstIn( x.name, "_").replace("__", "_")     //x.name.replaceFirst(".", "_")
                                                                   x.name.replace(".", "_").replace("__", "_").toLowerCase()
                                                              }else replaceDot( x.name, 2 ).replace("__", "_").toLowerCase() //regex.replaceFirstIn( x.name, "_").replace("__", "_")  //.replace(".", "_").replace("__", "_")
                                                            ) ) 
                                      colListsAlias ++= MSeq( x.name.replace(".", "_").replace("__", "_").toLowerCase()  )
                                    }
                            }
                    
          //colLists.map { x => if ( colExplodeLists.zip(colExplodeListsAlias).contains(x) ) x.name.replace(".", "_") }
                   
          logger.debug( " - colExplodeListsAlias - " + colExplodeListsAlias )
          logger.debug(" - colExplodeLists - " + colExplodeLists )
          logger.debug( " - colExplodeListsAlias2 - " + colExplodeListsAlias2 )
          logger.debug(" - colExplodeLists2 - " + colExplodeLists2 )
          logger.debug( " - colExplodeListsAlias3 - " + colExplodeListsAlias2 )
          logger.debug(" - colExplodeLists3 - " + colExplodeLists2 )
          
          logger.debug( " - colListsAlias - " + colListsAlias )
          logger.debug(" - colLists - " + colLists )
          
          logger.debug(" - arrayElements1 - " + arrayElements1 )
          logger.debug(" - arrayElements2 - " + arrayElements2 )
          logger.debug(" - arrayElements3 - " + arrayElements3 )
          
          //colLists.foreach { println }   //println( " x Datatype - "  )
          
          //xmlFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => println(x.dataType) }: _* )
          val xmlFileStreamExploded = xmlFileAttr.select( colExplodeLists.zip(colExplodeListsAlias).map{ case(x,y) =>
                                                                                                      if ( arrayElements1.contains(y) ){   //y.tail == null ){    // 
                                                                                                             println("exploded")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)
                                                                                                         }: _* 
                                                          )
                                                   .select( colExplodeLists2.zip(colExplodeListsAlias2).map{ case(x,y) =>
                                                                                                      if ( arrayElements2.contains(y) ){   //y.tail == null ){    // 
                                                                                                             println("exploded 2")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)
                                                                                                         }: _* 
                                                          )
                                                   .select( colExplodeLists3.zip(colExplodeListsAlias3).map{ case(x,y) =>
                                                                                                      if ( arrayElements3.contains(y) ){   //y.tail == null ){    // 
                                                                                                             println("exploded 3")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)
                                                                                                         }: _* 
                                                          )
                                                   .select( colLists.zip(colListsAlias).map{ case(x,y) => x.alias(y)
                                                                                             }: _* 
                                                          )
                                                          * 
                                                          */
          /*val xmlFileStreamExploded = xmlFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => if ( arrayElements.contains(x) ){
                                                                                                             println("exploded")
                                                                                                             explode(x).alias(y)
                                                                                                          }
                                                                                                          else x.alias(y)
                                                                                             }: _* 
                                                          )*/
          /*logger.debug(" - xmlFileStreamExploded - " )
          xmlFileStreamExploded.show()
          xmlFileStreamExploded.printSchema()
          saveDataFrameM("file", xmlFileStreamExploded, "", "xmlFileStreamExplodedFlex", null, null, null)
          */
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
      //var warehousesForThisJobId = warehousesControlTable.filter(warehousesControlTable("job_id") === jobId)  
      //commented on 05/17/2017 as filter is added at dataframe level
      var warehousesFilteredList = warehousesControlTable.select("warehouse_id").flatMap { r => List(r.getString(0)) }  
      //warehousesForThisJobId is replaced with warehousesControlTable on 05/17/2017
      return warehousesFilteredList.collect() map { "\'%s\'".format(_) } mkString ","
    }

    def saveDataFrame(dataFrame: DataFrame, keyspacename: String, tablename: String): Boolean = {
      dataFrame.write.format("org.apache.spark.sql.cassandra")
        .mode("append").option("table", tablename)
        .option("keyspace", keyspacename).save()
      return true;
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
        case "boolean"   => return BooleanType
        case "counter"   => return IntegerType
        case "bigint"    => return IntegerType
        case "text"      => return StringType
        case "ascii"     => return StringType
        case "varchar"   => return StringType
        case "varint"    => return IntegerType
        case default     => return StringType
      }
    }
}

      /* Commented on 05/02/2017 as it is not required in Merge file
      // Added on 05/01/2017 
      val pendingFilesExtTimes = fileIngestionStatus
      .select( fileIngestionStatus("creation_time"), fileIngestionStatus("file_name"), fileIngestionStatus("file_id"), fileIngestionStatus("validation_status_norm") )
      .filter(s"file_id = 1 AND validation_status_norm = 'Ingested' ")
      .orderBy( fileIngestionStatus("creation_time") )
      .limit(1)
      .select( "file_name" , "creation_time" )
      .withColumn("from_unix_timestamp", expr(" from_unixtime( unix_timestamp( SUBSTR(file_name, INSTR(file_name,'.') + 1, 12 ), 'yyyyMMddHHmm' ) )") )
      .collect()
     
     if ( pendingFilesExtTimes.isEmpty ) {
            logger.debug( " No file  Processed since last run " ) 
            //return
      }
      
      currentShippingExtTime = pendingFilesExtTimes.map { x => x.getString(2) }
                       .mkString
                       
      logger.debug( " - currentShippingExtTime - " + currentShippingExtTime ) 
     // Ended on 05/01/2017 
     */

          //jsonFileStream.select(expr(s"$colList")).show()          
          //jsonFileStream.select(colList).show()
          //val jsonFileStreamExploded = jsonFileStream.select( colLists: _* ) //.alias( colListsAlias.apply() ) 


          /*val xmlFileStreamFlexValue = xmlFileStream.select($"gtin", $"informationProviderGLN", $"globalClassificationCategory", $"flex")
          logger.debug( "Flex Schema")
          xmlFileStreamFlexValue.printSchema()
          xmlFileStreamFlexValue.registerTempTable("item_xml_file")
          xmlFileStreamFlexValue.show()*/
              
          /*val xmlFileStreamFlexAttr = xmlFileStream.select($"gtin", $"informationProviderGLN", explode($"flex.attr").alias("flex_attr") )  //.show()          
          //xmlFileStreamFlexAttr.show()
          xmlFileStreamFlexAttr.printSchema()
          //xmlFileStreamFlex.map ( x => Row(x.getString(0), x.getString(1), x.getStruct(3).fieldIndex(name) )}
          xmlFileStreamFlexAttr.select($"gtin", $"informationProviderGLN", $"flex", $"flex_attr._VALUE", $"flex_attr._name")
          .show()*/
          
          /*val xmlFileStreamFlexGroupMany = xmlFileStream.select($"gtin", $"informationProviderGLN", $"flex", explode($"flex.attrGroupMany.row").alias("flex_attrGroupMany_row")  )  //.show()   
                                          .select($"gtin", $"informationProviderGLN", $"flex", explode($"flex_attrGroupMany_row.attr" ).alias("flex_attrGroupMany_row_attr") )
                                          .select($"gtin", $"informationProviderGLN", $"flex", $"flex_attrGroupMany_row_attr._VALUE", $"flex_attrGroupMany_row_attr._name" )
          xmlFileStreamFlexGroupMany.show()
          xmlFileStreamFlexGroupMany.printSchema()
          //xmlFileStreamFlex.map ( x => Row(x.getString(0), x.getString(1), x.getStruct(3).fieldIndex(name) )}
          //xmlFileStreamFlexGroupMany.select($"gtin", $"informationProviderGLN", $"flex", explode( $"flex_attrGroupMany.row"), $"flex_attrGroupMany._name")
          //.show()*/
          
          // Multiple Exploded will result in below error
          //org.apache.spark.sql.AnalysisException: Only one generator allowed per select but Generate and and Explode found.;
          /*val xmlFileStreamFlexGrpAndQualManu = xmlFileStream.select($"gtin", $"informationProviderGLN", explode($"flex.attrQualMany").alias("flex_attrQualMany"), explode($"flex.attrGroupMany").alias("flex_attrGroupMany") )  //.show()
          logger.debug( "Muiltiple Xplodes")
          xmlFileStreamFlexGrpAndQualManu.show()
          xmlFileStreamFlexGrpAndQualManu.printSchema()*/
          
          /*val xmlFileStreamFlexGroupMany = xmlFileStream.select($"gtin", $"informationProviderGLN", $"globalClassificationCategory.code"
                                                              , $"globalClassificationCategory.name", $"globalClassificationCategory.definition"
                                                              , $"flex", explode($"flex.attrGroupMany").alias("flex_attrGroupMany")  )  //.show()   
                                                        //.select($"gtin", $"informationProviderGLN", $"flex", explode($"flex_attrGroupMany_row.attr" ).alias("flex_attrGroupMany_row_attr") )
                                                        .select($"gtin", $"informationProviderGLN", $"code"
                                                              , $"name", $"definition"
                                                              , $"flex", $"flex_attrGroupMany.row", $"flex_attrGroupMany._name" )
          logger.debug( " - xmlFileStreamFlexGroupMany- " + xmlFileStreamFlexGroupMany )
          xmlFileStreamFlexGroupMany.show()
          xmlFileStreamFlexGroupMany.printSchema()
          */
          
          //                                            SELECT gtin, informationProviderGLN, flex_attrQualMany._name, flex_attrGroupMany._name
          //gtin, informationProviderGLN, flex_attrQualMany_name, flex_attrGroupMany_name._name, flex_attrGroupMany_name_row
          
          //Giving 2 column in Alias for explode flex_attrGroupMany gives below error 
          //org.apache.spark.sql.AnalysisException: The number of aliases supplied in the AS clause does not match the number of columns output by the UDTF expected 1 aliases but got flex_attrgroupmany_name,flex_attrgroupmany_row ;
          //posexplode will give position, value 
          /*val xmlfileMultipleExplodeQuery = s""" SELECT gtin, informationProviderGLN, globalClassificationCategory
                                                       ,globalClassificationCategory.code, globalClassificationCategory.name
                                                       ,globalClassificationCategory.definition
                                                       ,flex_attrQualMany_name, flex_attrQualMany_value
                                                       --,flex_attrQualMany_value._value, flex_attrQualMany_value._qual
                                                       ,flex_attrGroupMany_name   
                                                       ,flex_attrGroupMany_row_attr, flex_attrGroupMany_row_attrGroupMany
                                                       --,flex_attrGroupMany_row_attrGroupMany._name, flex_attrGroupMany_row_attrGroupMany.row
                                                       ,flex_attrGroupMany_row_attrQual, flex_attrGroupMany_row_attrQualMany
                                                       --, flex_attrGroupMany_row
                                                       --, flex_attrGroupMany_row._name
                                                       --,flex_attrGroupMany_row.getItem(0) flex_attrGroupMany_row_name
                                                       --,flex_attrGroupMany_row.getItem(1) flex_attrGroupMany_row_row
                                                       -- flex_attrGroupMany_name_row
                                                   FROM item_xml_file
                                                LATERAL VIEW OUTER inline(flex.attrQualMany) flex_attrQualMany as flex_attrQualMany_name, flex_attrQualMany_value
                                                LATERAL VIEW OUTER inline(flex.attrGroupMany) flex_attrGroupMany as flex_attrGroupMany_name, flex_attrGroupMany_row
                                                LATERAL VIEW OUTER inline(flex_attrGroupMany_row) flex_attrGroupMany_row as flex_attrGroupMany_row_attr, flex_attrGroupMany_row_attrGroupMany, flex_attrGroupMany_row_attrQual, flex_attrGroupMany_row_attrQualMany
                                                --LATERAL VIEW OUTER inline(flex_attrGroupMany_row_attrGroupMany) flex_attrGroupMany_row_attrGroupMany as flex_attrGroupMany_row_attrGroupMany_name, flex_attrGroupMany_row_attrGroupMany_row
                                            """
          logger.debug( " - xmlfileMultipleExplodeQuery- " + xmlfileMultipleExplodeQuery )
          val xmlfileMultipleExplode = sqlContext.sql(xmlfileMultipleExplodeQuery)
          xmlfileMultipleExplode.show()
          xmlfileMultipleExplode.printSchema()
          saveDataFrameM("file", xmlfileMultipleExplode, "", "xmlfileMultipleExplode", null, null, null) 
          */
          
          /*val xmlFileStreamExploded = xmlFileStream.map{ x  => Row( for (  i <- 0 to x.schema.fields.length - 1 )
                                                                  yield{ println( "x.schema.fields.apply(i).dataType.typeName - " + x.schema.fields.apply(i).dataType.typeName
                                                                               + " - x.schema.fields.apply(i).name - " + x.schema.fields.apply(i).name
                                                                                + " - x.schema.fieldIndex( x.schema.fields.apply(i).name ) - " + x.schema.fieldIndex( x.schema.fields.apply(i).name )
                                                                               )
                                                                              if ( x.schema.fields.apply(i).dataType.typeName.equalsIgnoreCase("array") )
                                                                                 //explode( $" + x.getString( x.schema.fieldIndex(y.name) ) + ")
                                                                                  explode( $" + x.schema.fields.apply(i).name + ")
                                                                            else if ( x.schema.fields.apply(i).dataType.typeName.equalsIgnoreCase("struct") )
                                                                                for ( fields <- x.schema.fields.apply(i).dataType.asInstanceOf[StructType].fields )
                                                                               yield { //println( " - x.schema.fields.apply(i).name+"."+fields.name - " + x.schema.fields.apply(i).name + "." + fields.name )
                                                                                       //x.getAs[String]( x.schema.fields.apply(i).name, "." , fields.name )
                                                                                       $" + concat( x.schema.fields.apply(i).name, '.' , fields.name ) ) + " 
                                                                                     }
                                                                            else //x.getAs[ x.schema.fields.apply(i).dataType ]( x.schema.fields.apply(i).name )
                                                                                 $" + x.schema.fields.apply(i).name + "
                                                                       } 
                                                                        
                                                                  ) 
                                                       }
           
          xmlFileStreamExploded.foreach { x => println(x) }
         */
          //saveDataFrameM("file", xmlFileStreamFlexAttr, "", "xmlFileStreamFlex", null, null, null) 
          //saveDataFrameM("file", xmlFileStreamFlexGroupMany, "", "xmlFileStreamFlexGroupMany", null, null, null) 
          
          //xmlFileStreamJSON.foreach { x => ??? }

          /*val nameJson = xmlFileStream.map { x => x.getAs[String]("name") }
          nameJson.foreach { println }
          xmlFileStream.registerTempTable("json_raw_data")*/


          //It is also working
          // Recursive function to get schema from stuct type field
          /*def findFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame, 
              val fieldName = dt.asInstanceOf[StructType].fields
              //var returnStruct: StructField = null
              for ( value <- fieldName ){
                   //println( "Name - " + value.name + "\t" + " Data Type - " + value.dataType + "\t" + " Data Type Name - " + value.dataType.typeName )
                   if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else if ( value.dataType.typeName.equalsIgnoreCase("array") ){
                      if ( explodeArray.equalsIgnoreCase("Y") ){
                         findArrayFields(parentName + "." + value.name , value.dataType)
                      }else{
                         xmlSchema = xmlSchema.add(StructField( parentName + "."+ value.name, value.dataType, value.nullable ))
                      }
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     xmlSchema = xmlSchema.add(StructField( parentName + "."+ value.name, value.dataType, value.nullable ))
                   }
              }   
              //returnStruct
          }
          
          def findArrayFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame,
              val fieldName: ArrayType = dt.asInstanceOf[ArrayType]
              
              /*println( "parent Name - "  + parentName + "\t"
                          + "Array Name - " + fieldName  + "\t"  //fieldName.productElement( value )
                          + "Array DataType - " + fieldName.elementType + "\t"
                      )*/
              /*for ( value <- dt.asInstanceOf[ArrayType].productIterator ){
                   println( "parent Name - "  + parentName + "\t"
                          + "Array Name - " + fieldName  + "\t"  //fieldName.productElement( value )
                          + "Array DataType - " + fieldName.elementType + "\t"
                          + "Array value - " + value )
                   /*if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     xmlSchema = xmlSchema.add(StructField( parentName +"."+value.name, value.dataType, value.nullable ))
                   }*/
              }*/
              fieldName.elementType match {
                case s: StructType => findFields(parentName, fieldName.elementType )
                case s: ArrayType => if ( explodeArray.equalsIgnoreCase("Y") ){
                                        findArrayFields(parentName, fieldName.elementType  )
                                     }else{
                                        xmlSchema = xmlSchema.add(StructField( parentName, fieldName.elementType, true ))
                                      }
                case other => xmlSchema = xmlSchema.add(StructField( parentName, fieldName.elementType, true ))
              }
          }
         
          xmlFileStream.schema.fields.map { x => { 
                                              /*println( "Field Name - " + x.name
                                                      ,"Field Data Type - " + x.dataType
                                                      ,"Field Data Type Name - " + x.dataType.typeName
                                                      ,"Field Index - " + xmlFileStream.schema.fieldIndex(x.name)
                                                      //,"Field Element Type - " + x.
                                                      //,"Field Default Size - " + x.dataType.defaultSize
                                                      //,"Metadata - " + x.metadata 
                                                    )*/
                                                ( if ( x.dataType.typeName.equalsIgnoreCase("struct") ) 
                                                     findFields(x.name, x.dataType)
                                                  else if ( x.dataType.typeName.equalsIgnoreCase("array") ) 
                                                     findArrayFields(x.name, x.dataType)
                                                     //null
                                                  else
                                                     xmlSchema = xmlSchema.add(StructField( x.name
                                                                                ,x.dataType
                                                                                ,x.nullable) )
                                                )
                                             }
                                      }
          
          logger.debug(" - Print xmlSchema Tree String - ")  
          xmlSchema.printTreeString()*/