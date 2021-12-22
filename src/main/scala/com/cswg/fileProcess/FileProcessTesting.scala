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

import scala.collection.mutable.{Seq => MSeq}
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{ListBuffer => MListBuffer}
import scala.collection.mutable.{ArrayBuffer => MArrayBuffer}
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

class FileProcessTesting {
  
}

object FileProcessTesting {
  var logger = LoggerFactory.getLogger(this.getClass);    //"Examples"
  logger.debug(" - FileProcessTesting Started - ")
     
  var thisJobWarehouses = ""
  val currentRunTime = getCurrentTime()
  var jobId = 1
  var fileId = 1
  var sc: SparkContext = null 
  var conn: CassandraConnector = null
  var loggingEnabled = "N"
  val schemaName = "mdm"  //"mdm"  "wip_shipping_ing" 
  var processFileName: String = ""
  // 0 - environment
  // 1 - Job Id 
  // 2 - File Id
  // 3 - ConfFilePath
  // 4 - Logging Enabled  
  // 5 - File Name
  def main(args: Array[String]) {
    // logger file name 
    MDC.put("fileName", "M2999_FileProcessingTesting_" + args(2).toString() )  //Jobid added on 09/08/2017
    
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
              + " - processFileName - " +  processFileName
             )
     
    val (hostName, userName, password) = getCassandraConnectionProperties(environment)
    logger.info( " Host Details got" )
        
    val conf = new SparkConf()
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .set("spark.cassandra.input.fetch.size_in_rows", "10000")
      .set("spark.cassandra.input.split.size_in_mb", "100000")
      .set("textinputformat.record.delimiter", "\n")
      //.set("spark.eventLog.enabled", "false")  commented on 09/08/2017
      .setAppName("M1999_FileProcessing" + args(2).toString())
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]")     
    //.set("spark.cassandra.output.ignoreNulls", "true")
    
    conn = CassandraConnector(conf)     
    sc = new SparkContext(conf)
    val sqlContext: SQLContext = new HiveContext(sc)
    import sqlContext.implicits._
    
    sc.broadcast(jobId)
    sc.broadcast(fileId)
    
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
            
            val dfFileStreamingTracker = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "file_streaming_tracker", "keyspace" -> "wip_configurations") )
            .load()
            .filter(s" file_id = $fileId "); 
            
            val dfFileTracker = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "file_tracker", "keyspace" -> "wip_configurations") )
            .load()
            //.filter(s" file_id = $fileId "); 
            
            val dfItemDetailsDocId = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_doc_id", "keyspace" -> schemaName) )
            .load()
            
            val dfItemDetailsGtin = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_details_by_gtin", "keyspace" -> schemaName) )
            .load()
            
            val dfDocumentLinkDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "document_link_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attr_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrGroupDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrgroup_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemGroupManyDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrgroupmany_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrManyDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrmany_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrQualDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrqual_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrQualManyDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrqualmany_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrQualOptDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrqualopt_details", "keyspace" -> schemaName) )
            .load()
            
            val dfItemAttrQualOptManyDetails = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "item_attrqualoptmany_details", "keyspace" -> schemaName) )
            .load()
            
            
            dfDataTypeMapping = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "cassandra_datatype_mapping", "keyspace" -> "wip_configurations") )
            .load() 
            
            dfWarehouseOperationMaster.registerTempTable("warehouse_operation_time")
            dfFileController.registerTempTable("file_ingestion_controller")
            dfFileStreamingTracker.registerTempTable("file_streaming_tracker")
            dfFileTracker.registerTempTable("file_tracker")
            dfItemDetailsDocId.registerTempTable("item_details_by_doc_id")
            dfItemDetailsGtin.registerTempTable("item_details_by_gtin")
            dfDocumentLinkDetails.registerTempTable("document_link_details")
            dfItemAttrDetails.registerTempTable("item_attr_details")
            dfItemAttrGroupDetails.registerTempTable("item_attrgroup_details")
            dfItemGroupManyDetails.registerTempTable("item_attrgroupmany_details")
            dfItemAttrManyDetails.registerTempTable("item_attrmany_details")
            dfItemAttrQualDetails.registerTempTable("item_attrqual_details")
            dfItemAttrQualManyDetails.registerTempTable("item_attrqualmany_details")
            dfItemAttrQualOptDetails.registerTempTable("item_attrqualopt_details")
            dfItemAttrQualOptManyDetails.registerTempTable("item_attrqualoptmany_details")
            
            dfDataTypeMapping.registerTempTable("cassandra_datatype_mapping")
            logger.debug( "Register Temp Tables" )
               
            if (dfFileController.rdd.isEmpty) {
                logger.debug(" No file schema is defined for Job id and File id combination " + currentRunTime)
                return
            }
            
            updateStatusinFilesTracker(sqlContext)
            //copyFilesInTracker(sqlContext) 
            //deleteAttributes(sqlContext, "N", "Y")
           	//getFileDetails(sqlContext)  //sc,  , conn

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
  
  def updateStatusinFilesTracker(sqlContext: SQLContext) = {
      /*( val fileTrackerQuery = s""" SELECT file_id, file_name, 'Ingested' validation_status_norm
                                     FROM file_streaming_tracker
                                    WHERE file_id = 4
                                      AND ( ( '$processFileName' = '' ) OR ( file_name = '$processFileName' ) )
                                      AND validation_status_norm is null
                               """*/
       val fileTrackerQuery = s""" SELECT file_id, file_name, 'Ingested' validation_status
                                     FROM file_streaming_tracker
                                    WHERE file_id = 4
                                      AND ( ( '$processFileName' = '' ) OR ( file_name = '$processFileName' ) )
                                      AND validation_status is null
                               """
      logger.debug(" fileTrackerQuery - " + fileTrackerQuery )
      val fileTracker = sqlContext.sql(fileTrackerQuery)
      fileTracker.show()
      
      saveDataFrameM("table", fileTracker, "", null, "wip_configurations", "file_streaming_tracker", "dev") 
       
      logger.debug( " --- Finished --- " )
  }
  
  def copyFilesInTracker(sqlContext: SQLContext) = {
       //AND file_name IN ('1WS_CS_CIN.cin.LJ1NM5209000307.1000020473835','1WS_CS_CIN.cin.LJ1R00159000707')
       val fileTrackerQuery = s""" SELECT 4 file_id, file_name, creation_time, snapshot_id, 'Ingested' validation_status_norm
                                     FROM file_tracker
                                    WHERE file_id = 3
                                      AND ( ( '$processFileName' = '' ) OR ( file_name = '$processFileName' ) )
                               """
      logger.debug(" fileTrackerQuery - " + fileTrackerQuery )
      val fileTracker = sqlContext.sql(fileTrackerQuery)
      fileTracker.show()
      
      saveDataFrameM("table", fileTracker, "", null, "wip_configurations", "file_streaming_tracker", "dev") 
       
      logger.debug( " --- Finished --- " )
  }
  
  def deleteAttributes( sqlContext: SQLContext, attributes: String, link: String ) = {
      if ( attributes.equalsIgnoreCase("Y") ){
          val itemAttrQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attr_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrQuery - " + itemAttrQuery )
          val itemAttr = sqlContext.sql(itemAttrQuery)
          
          saveDataFrameM("table", itemAttr, "", null, schemaName, "item_attr_details", "dev")                           
        
          val itemAttrGroupQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrgroup_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrGroupQuery - " + itemAttrGroupQuery )
          val itemAttrGroup = sqlContext.sql(itemAttrGroupQuery)
          
          saveDataFrameM("table", itemAttrGroup, "", null, schemaName, "item_attrgroup_details", "dev") 
          
          val itemAttrGroupManyQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrgroupmany_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrGroupManyQuery - " + itemAttrGroupManyQuery )
          val itemAttrGroupMany = sqlContext.sql(itemAttrGroupManyQuery)
          
          saveDataFrameM("table", itemAttrGroupMany, "", null, schemaName, "item_attrgroupmany_details", "dev") 
          
          val itemAttrManyQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrmany_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrManyQuery - " + itemAttrManyQuery )
          val itemAttrMany = sqlContext.sql(itemAttrManyQuery)
          
          saveDataFrameM("table", itemAttrMany, "", null, schemaName, "item_attrmany_details", "dev") 
          
          val itemAttrQualQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrqual_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrQualQuery - " + itemAttrQualQuery )
          val itemAttrQual = sqlContext.sql(itemAttrQualQuery)
          
          saveDataFrameM("table", itemAttrQual, "", null, schemaName, "item_attrqual_details", "dev")
          
          val itemAttrQualManyQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrqualmany_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrQualManyQuery - " + itemAttrQualManyQuery )
          val itemAttrQualMany = sqlContext.sql(itemAttrQualManyQuery)
          
          saveDataFrameM("table", itemAttrQualMany, "", null, schemaName, "item_attrqualmany_details", "dev")
          
          val itemAttrQualOptQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrqualopt_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrQualOptQuery - " + itemAttrQualOptQuery )
          val itemAttrQualOpt = sqlContext.sql(itemAttrQualOptQuery)
          
          saveDataFrameM("table", itemAttrQualOpt, "", null, schemaName, "item_attrqualopt_details", "dev")
          
          val itemAttrQualOptManyQuery = s""" SELECT documentid, item_gtin, hierarchyinformation_informationprovidergln, uuid, 'N' active
                                     FROM item_attrqualoptmany_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                               """
          logger.debug(" itemAttrQualOptManyQuery - " + itemAttrQualOptManyQuery )
          val itemAttrQualOptMany = sqlContext.sql(itemAttrQualOptManyQuery)
          
          saveDataFrameM("table", itemAttrQualOptMany, "", null, schemaName, "item_attrqualoptmany_details", "dev")
          
      }
      if ( link.equalsIgnoreCase("Y") ){
          val documentLinkQuery = s""" SELECT documentid, uuid, 'N' active
                                     FROM document_link_details
                                    WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' ) 
                                      AND link_parentitem_gtin is null 
                               """
          logger.debug(" documentLinkQuery - " + documentLinkQuery )
          val documentLink = sqlContext.sql(documentLinkQuery)
          documentLink.show()
          
          saveDataFrameM("table", documentLink, "", null, schemaName, "document_link_details", "dev")
      }
      
      logger.debug( " --- Finished --- " )
  }
  
  def getFileDetails(sqlContext: SQLContext) = {
    
      //val fileName = ""
      
      val documentDetailsQuery = s""" SELECT * 
                                        FROM item_details_by_doc_id
                                       WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                                  """
      logger.debug(" documentDetailsQuery - " + documentDetailsQuery )
      val documentDetails = sqlContext.sql(documentDetailsQuery)
      
      saveDataFrameM("file", documentDetails, "document_id, item_gtin", "DocumentDetails", "", "", "")
      
      val documentDetailsGtinQuery = s""" SELECT * 
                                        FROM item_details_by_gtin
                                       WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                                  """
      logger.debug(" documentDetailsGtinQuery - " + documentDetailsGtinQuery )
      val documentDetailsGtin = sqlContext.sql(documentDetailsGtinQuery)
      
      saveDataFrameM("file", documentDetailsGtin, "document_id, item_gtin", "DocumentDetailsGtin", "", "", "")
      
      logger.debug( " --- Finished --- " )
  }
 
  def readJson(sqlContext: SQLContext) = {
    
      //val fileName = ""
      
      val documentDetailsQuery = s""" SELECT * 
                                        FROM item_details_by_doc_id
                                       WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                                  """
      logger.debug(" documentDetailsQuery - " + documentDetailsQuery )
      val documentDetails = sqlContext.sql(documentDetailsQuery)
      
      saveDataFrameM("file", documentDetails, "document_id, item_gtin", "DocumentDetails", "", "", "")
      
      val documentDetailsGtinQuery = s""" SELECT * 
                                        FROM item_details_by_gtin
                                       WHERE ( '$processFileName' = '' ) OR ( file_name = '$processFileName' )  
                                  """
      logger.debug(" documentDetailsGtinQuery - " + documentDetailsGtinQuery )
      val documentDetailsGtin = sqlContext.sql(documentDetailsGtinQuery)
      
      saveDataFrameM("file", documentDetailsGtin, "document_id, item_gtin", "DocumentDetailsGtin", "", "", "")
      
      logger.debug( " --- Finished --- " )
  }
  
}