package com.cswg.fileProcess

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.{LoggerFactory, MDC}

import java.io.FileNotFoundException
import scala.util.control.Breaks._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ StructType,StructField,IntegerType,StringType,DoubleType,LongType,FloatType,ByteType,TimestampType,BooleanType,DataType,ArrayType}
import org.apache.spark.sql.functions._
import scala.collection.mutable.{Seq => MSeq}
import com.datastax.driver.core.utils.UUIDs

import org.apache.spark.sql.functions.monotonicallyIncreasingId

import scala.util.parsing.json._
import scala.util.parsing.json.JSON
import scala.io.Source

import java.io.File
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.{Calendar, UUID, Date, Properties}

//import com.databricks.spark.xml._

class FileProcessing {
  
}

object FileProcessing {
  val logger = LoggerFactory.getLogger(this.getClass());
  logger.debug(" - FileProcessing Started - ")
     
  var thisJobWarehouses = ""
  val currentRunTime = getCurrentTime()
  var jobId = 1
  var fileId = 1
  var dfDataTypeMapping: DataFrame = null
  val fileControllerSchema = StructType(Array(
                                              StructField("job_id", IntegerType, false),
                                              StructField("file_name", StringType, false),
                                              StructField("file_id", IntegerType, false),                                              
                                              StructField("file_schema", StringType, true)
                                              )
                                        )
                                        
  val fileStatusSchema = StructType(Array(
                                              StructField("file_id", IntegerType, true),
                                              StructField("file_name", StringType, true),
                                              StructField("snapshot_id", StringType, true),
                                              StructField("processed_rows", IntegerType, true),
                                              StructField("rejected_rows", IntegerType, true),
                                              StructField("total_rows", IntegerType, true),
                                              StructField("validation_status", StringType, true),
                                              StructField("error", StringType, true),
                                              StructField("ext_timestamp", TimestampType, true)
                                              )
                                        )
                                        
  // 0 - environment
  // 1 - Job Id 
  // 2 - File Id
  @throws(classOf[Exception])
  def main(args: Array[String]) {
    // logger file name 
    MDC.put("fileName", "M3031_FileProcessing_" + args(0).toString() )  //Jobid added on 09/08/2017
    //println( ClassLoader.getSystemResource("logback.xml") )
    
    val environment = if ( args(0) == null ) "dev" else args(0).toString 
    jobId = if (args.length > 0) args(1).toInt else 0
    fileId = if (args.length > 0) args(2).toInt else 0
    
    //println(this.getClass())
    
    logger.debug( " - environment - " + environment + "\n"
              + " - Job Id - " + jobId + "\n"
              + " - File Id - " + fileId + "\n"
             )
     
    val (hostName, userName, password) = getCassandraConnectionProperties(environment)
    logger.debug( " Host Details got" )
        
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      //.set("spark.cassandra.input.fetch.size_in_rows", "10000")
      //.set("spark.cassandra.input.split.size_in_mb", "100000")
      //.set("textinputformat.record.delimiter", "\n")
      //.set("spark.eventLog.enabled", "false")  commented on 09/08/2017
      .setAppName("M3031_FileProcessing_" + args(0).toString() )
      .setSparkHome("/Users/goyalpushkar/spark_163/")
      //s.set("spark.ui.port", "7080")
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]")     
    //.set("spark.cassandra.output.ignoreNulls", "true")

    val sc = new SparkContext(conf)
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
            .options( Map("table" -> "file_controller", "keyspace" -> "wip_configurations") )
            .load()
            .filter(s" file_id = $fileId and job_id = $jobId" );  
            
            val dfFileTracker = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "file_tracker", "keyspace" -> "wip_configurations") )
            .load()
            .filter(s" file_id = $fileId "); 
            
            dfDataTypeMapping = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options( Map("table" -> "cassandra_datatype_mapping", "keyspace" -> "wip_configurations") )
            .load() 
            
            dfWarehouseOperationMaster.registerTempTable("warehouse_operation_time")
            dfFileController.registerTempTable("file_controller")
            dfFileTracker.registerTempTable("file_tracker")
            dfDataTypeMapping.registerTempTable("cassandra_datatype_mapping")
            logger.debug( "Register Temp Tables" )
               
            /*if (dfFileController.rdd.isEmpty) {
                logger.debug(" No file schema is defined for Job id and File id combination " + currentRunTime)
                return
            }else{
                logger.debug("File Controller found")
            }*/
            
            callStatus = 	fileProcessing(sc, sqlContext, dfFileController)
            logger.debug( "Status returned by Shipping - " + callStatus  )

             
            //saveDataFrame(fileStatus DF.withColumn("update_time", lit(ShippingUtil.getCurrentTime())), "wip_configurations", "file_streaming_tracker")
            logger.debug("CSSparkJobStatus:Completed" )
            sqlContext.clearCache()

    } catch {
        case e @ ( _ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("fileProcessing Error : " + e.getMessage )
            throw new Exception("Error in main at : " + e.getMessage + ": " + e.printStackTrace() )   
    }
    sc.stop()   
  }
    
  def getCassandraType( javaType: String ): DataType = {
       //sqlContext: SQLContext, 
       logger.debug( " javaType - " + javaType )
       /*val getTypeMappingQuery = s"""SELECT cassandra_type 
                                       FROM cassandra_datatype_mapping 
                                      WHERE java_type = '$javaType' 
                                  """
       val getTypeMapping = sqlContext.sql(getTypeMappingQuery)*/
       val dataTypeValue = dfDataTypeMapping.select("cassandra_type", "java_type")
                           .filter(s" java_type = '$javaType' ")
                           .select("cassandra_type")//.asInstanceOf[DataType]
                           .collect()
      
       return dataTypeValue.apply(0).getAs[DataType](0)
       
   }
  
  def getScalaType( javaType: String ): DataType = {
       //sqlContext: SQLContext, 
       logger.debug( " javaType - " + javaType )
       /*val getTypeMappingQuery = s"""SELECT cassandra_type 
                                       FROM cassandra_datatype_mapping 
                                      WHERE java_type = '$javaType' 
                                  """
       val getTypeMapping = sqlContext.sql(getTypeMappingQuery)*/
       val dataTypeValue = dfDataTypeMapping.select("scala_type", "java_type")
                           .filter(s" java_type = '$javaType' ")
                           .select("scala_type")  //.asInstanceOf[DataType]
                           .collect()
       //dataTypeValue.select("cassandra_type")
       
       return dataTypeValue.apply(0).getAs[DataType](0)
       
   }
  
    def fileProcessing( sc: SparkContext, sqlContext: SQLContext, dfFileController: DataFrame): String = {
        logger.debug(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                 fileProcessing                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )
      
      var status = "0" 
  
      try{                 
           val currentFileQuery = s""" WITH base_rows AS 
                                          ( SELECT ft.file_id, ft.file_name
                                                  ,ft.snapshot_id, ft.creation_time
                                                  ,fc.file_type
                                                  ,MAX( ft.creation_time ) OVER ( PARTITION BY ft.file_id ) max_creation_time
                                              FROM file_tracker ft
                                                  ,file_controller fc
                                             WHERE ft.file_id = fc.file_id 
                                               AND ft.validation_status = 'Inserted'
                                         ) 
                                         SELECT file_id, file_name, snapshot_id
                                               ,creation_time, file_type
                                           FROM base_rows 
                                          WHERE max_creation_time = creation_time
                                        """
            val currentFile = sqlContext.sql(currentFileQuery).persist()
            currentFile.show()
            logger.debug("Current File extracted" )
            
            if (currentFile.rdd.isEmpty) {
                logger.debug(" No files to process are in waiting status  at " + currentRunTime)
                return status
            }
            
            val fileName = currentFile.first().get(1).toString()
            val snapShotId = currentFile.first().get(2).toString()
            val fileCreationTime = currentFile.first().get(3).toString()
            val fileType = currentFile.first().get(4).toString()
            
             if ( fileType.equalsIgnoreCase("csv") || fileType.equalsIgnoreCase("txt") ) {
                processCSVFile( sc, sqlContext, dfFileController, fileName, snapShotId, fileCreationTime ) 
             }else if ( fileType.equalsIgnoreCase("json") ) {
                processingJSONFile( sc, sqlContext, dfFileController, fileName, snapShotId, fileCreationTime )
             }else if ( fileType.equalsIgnoreCase("xml") ) {
                processingXMLFile( sc, sqlContext, dfFileController, fileName, snapShotId, fileCreationTime )
             }
      
         }
     catch {
          case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("fileProcessing Error : " + e.getMessage )
            status = "1"
            throw new Exception("Error in fileProcessing at : " + e.getMessage + ": " + e.printStackTrace())
         }
      return status
    }
    
   def processCSVFile( sc: SparkContext, sqlContext: SQLContext, dfFileController: DataFrame, fileName: String, snapShotId: String, fileCreationTime: String ): String = {
       logger.debug ( " - processCSVFile - " ) 
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
            fileDataWithTimeStamp.show()
           
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
             saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_tracker")
             logger.debug(" file " + fileName + " processed  at " + getCurrentTime())
                                 
       }
       catch {
         case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("Tex file processing error : " + e.getMessage )
            status = "1"
            throw new Exception("Error while processing data File at : " + e.getMessage + ": " + e.printStackTrace())
       }
       
       return status
   }
   
   
  def processingJSONFile( sc: SparkContext, sqlContext: SQLContext, dfFileController: DataFrame, fileName: String, snapShotId: String, fileCreationTime: String): String = {
		 logger.debug(" - processingJSONFile - ");
     var status = "0";
     var validationStatus = "Inserted"
     var errorMessage: String = null

     var cffPath = ""
     var fileSchema = "" 
     var trimZeroes = ""
     var intFieldIndexes = ""
     var jsonSchema: StructType = new StructType()
     /*jsonSchema =  jsonSchema.add(StructField( "name"
                                 ,StringType
                                 ,true)
                                     )

     logger.debug(" - Print jsonSchema - " + jsonSchema.length + " = " + jsonSchema.size )
     jsonSchema.foreach { x => logger.debug( x.name + " - " + x.dataType) }
     jsonSchema.printTreeString()*/
     
     import sqlContext.implicits._
     
     try {
         
           try {
                 cffPath = dfFileController.select("file_path").first().getString(0)
                 fileSchema = dfFileController.select("file_schema").first().getString(0)
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
          logger.debug("jsonFileStream - " + jsonFileStream);
          jsonFileStream.printSchema()
          jsonFileStream.show()
          /*val nameJson = jsonFileStream.map { x => x.getAs[String]("name") }
          nameJson.foreach { println }
          jsonFileStream.registerTempTable("json_raw_data")*/
          
          logger.debug(" - Create Struct Type from Json - ")  
          // Recursive function to get schema from stuct type field
          def findFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame, 
              val fieldName = dt.asInstanceOf[StructType].fields
              //var returnStruct: StructField = null
              for ( value <- fieldName ){
                   logger.debug( "Name - " + value.name + " Data Type - " + value.dataType )
                   if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     jsonSchema = jsonSchema.add(StructField( parentName +"."+value.name, value.dataType, value.nullable ))
                   }
              }   
              //returnStruct
          }

          jsonFileStream.schema.fields.map { x => { 
                                              /*logger.debug( "Data Type - " + x.dataType.typeName
                                                      ,"Name - " + x.name
                                                      ,"Default Size - " + x.dataType.defaultSize
                                                      //,"Metadata - " + x.metadata 
                                                      ,"Index - " + jsonFileStream.schema.fieldIndex(x.name)
                                                    )*/
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
          //jsonFileStream.select(expr(s"$colList")).show()          
          //jsonFileStream.select(colList).show()
          //val jsonFileStreamExploded = jsonFileStream.select( colLists: _* ) //.alias( colListsAlias.apply() ) 
          val jsonFileStreamExploded = jsonFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => x.alias(y) }: _* )
          //val jsonFileStreamExploded = jsonFileStream.select(colLists.map( c => col(c)):_*)//.alias(colListsAlias.apply( colLists.indexOf(_)   ))
          jsonFileStreamExploded.show()
          
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
           
          
          val currentExtTime = null          
  			  val fileStatusRDD = sc.parallelize( Seq( Row( fileId, fileName, snapShotId, processedCount, failedCount, (processedCount.toInt + failedCount.toInt), validationStatus, errorMessage, currentExtTime) ) ) 
          val fileStatusDF = sqlContext.createDataFrame( fileStatusRDD, fileStatusSchema ).toDF()
          saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_tracker")
          
          val fileControllerRDD = sc.parallelize( Seq( Row( jobId, fileName, fileId, jsonSchema ) ) ) 
          val fileControllerDF = sqlContext.createDataFrame( fileControllerRDD, fileControllerSchema ).toDF()
          saveDataFrame(fileControllerDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_controller")
          logger.debug(" file " + fileName + " processed  at " + getCurrentTime())
           
	   }
		 catch {
		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("JSON file processing error : " + e.getMessage )
            e.printStackTrace()
            status = "1"
            throw new Exception("Error while processing json File at : " + e.getMessage + ": " + e.printStackTrace())
            
			}  
			return status
   }
   
  def processingXMLFile( sc: SparkContext, sqlContext: SQLContext, dfFileController: DataFrame, fileName: String, snapShotId: String, fileCreationTime: String): String = {
		 logger.debug(" - processingXMLFile - ");
		 val currentTime = getCurrentTime()
     var status = "0";
     var validationStatus = "Inserted"
     var errorMessage: String = null

     var cffPath = ""
     var fileSchema = "" 
     var trimZeroes = ""
     var intFieldIndexes = ""
     var xmlSchema: StructType = new StructType()
     
     import sqlContext.implicits._
     
     try {
         
           try {
                 cffPath = dfFileController.select("file_path").first().getString(0)
                 fileSchema = dfFileController.select("file_schema").first().getString(0)
               } catch {
                   case e @ (_ :Exception | _ :Error | _ :java.io.IOException | _:java.util.NoSuchElementException ) =>
                     logger.error("Error while gettign file schema at : " + e.getMessage + " "  +  e.printStackTrace() )
           }
               
          //val noOfColumns = fileSchema.split(",").size
          //val firstField = fileSchema.split(",").apply(0).split(":").apply(0) 
               
          logger.debug("file name => " + fileName + "\n"
                      + "fileSchema => " + fileSchema + "\n"
                      + "filePath => " + cffPath + "/" + fileName + "\n"
                      //+ "firstField => " + firstField  + "\n"                      
                      //+ "noOfColumns => " + noOfColumns + "\n"
                      //+ "FileDelimiter => " + "\\"+ fileDelimiter
                 )
                        
          /*val fileStruct = StructType(fileSchema.split(",").map(fieldName => StructField(fieldName.split(":")(0)
                                                                                          ,getCassandraTypeMapping(fieldName.split(":")(1))
                                                                                          ,true)
                                                                    )
                                           )*/
                                           
          //val jsonFileStream = sqlContext.read.json(cffPath + "/" + fileName)
			    //val jsonFileStream = parser.parse((new FileReader(cffPath + "/" + fileName));
          //val jsonFileStream = Json.parse(new FileReader(cffPath + "/" + fileName).toString())
          //logger.debug("jsonFileStream - " + jsonFileStream);
			    //val neo4JJsonObject = neo4JObj;
			    //logger.debug("neo4JJsonObject - " + neo4JJsonObject);
           
         //val jsonFileStream = sqlContext.read.json(cffPath + "/" + fileName)
          val xmlFileStream = sqlContext
                               .read
                               .format("com.databricks.spark.xml")
                               .option("rowTag", "item")  //document
                               .load(cffPath + "/" + fileName)
                               // jsonFile()
			    //val jsonFileStream = parser.parse((new FileReader(cffPath + "/" + fileName));
          //val xmlFileStream = Json.parse(new FileReader(cffPath + "/" + fileName).toString())
          logger.debug("xmlFileStream - " + xmlFileStream);
          //val xmlFileStreamJSON = xmlFileStream.toJSON
          xmlFileStream.printSchema()
          xmlFileStream.show()
          
          xmlFileStream.select($"gtin", $"debugrmationProviderGLN", explode($"flex.attr") ).show()
          
          //xmlFileStreamJSON.foreach { x => ??? }

          /*val nameJson = xmlFileStream.map { x => x.getAs[String]("name") }
          nameJson.foreach { println }
          xmlFileStream.registerTempTable("json_raw_data")*/
          
          logger.debug(" - Create Struct Type from XML - ")  
          // Recursive function to get schema from stuct type field
          def findFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame, 
              val fieldName = dt.asInstanceOf[StructType].fields
              //var returnStruct: StructField = null
              for ( value <- fieldName ){
                   logger.debug( "Name - " + value.name + " Data Type - " + value.dataType.typeName )
                   if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else if ( value.dataType.typeName.equalsIgnoreCase("array") ){
                      findArrayFields(parentName, value.dataType)
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     xmlSchema = xmlSchema.add(StructField( parentName +"."+value.name, value.dataType, value.nullable ))
                   }
              }   
              //returnStruct
          }
          
          def findArrayFields(parentName: String, dt: DataType): Unit = {  //df: DataFrame,
              val fieldName = dt.asInstanceOf[ArrayType]
              for ( value <- dt.asInstanceOf[ArrayType].productIterator ){
                   logger.debug( "Array Name - " + fieldName   //fieldName.productElement( value )
                          + " Array value - " + value )
                   /*if ( value.dataType.typeName.equalsIgnoreCase("struct") ){
                      findFields(parentName +"."+value.name, value.dataType)
                   }else{
                      //returnStruct = StructField( value.name, value.dataType, value.nullable )
                     xmlSchema = xmlSchema.add(StructField( parentName +"."+value.name, value.dataType, value.nullable ))
                   }*/
              }   
              //returnStruct
          }

          xmlFileStream.schema.fields.map { x => { 
                                                 logger.debug( "Field Data Type - " + x.dataType.typeName
                                                             + " :Field Name - " + x.name
                                                               //,"Field Default Size - " + x.dataType.defaultSize
                                                              //,"Metadata - " + x.metadata 
                                                             + " :Field Index - " + xmlFileStream.schema.fieldIndex(x.name)
                                                            )
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
          xmlSchema.printTreeString()
          
          /*var colLists: MSeq[Column] = MSeq.empty
          var colListsAlias: MSeq[String] = MSeq.empty
          xmlSchema.map { x => { colLists ++= MSeq(col(x.name) ) 
                                  colListsAlias ++= MSeq(x.name.replace(".", "_"))
                                }
                         }
          logger.debug( " - colListsAlias - " + colListsAlias )
          logger.debug(" - colLists - " + colLists)
          //colLists.foreach { logger.debug }
          val xmlFileStreamExploded = xmlFileStream.select( colLists.zip(colListsAlias).map{ case(x,y) => x.alias(y) }: _* )
          logger.debug(" - xmlFileStreamExploded - " )
          xmlFileStreamExploded.show()
          
          val processedCount = xmlFileStream.count().toInt
          val failedCount = 0 //xmlFileStream.count().toInt
          if (failedCount > 0){
             //unMatchedRecords.repartition(1).saveAsTextFile(cffPath + "/" + "rejected_" + fileName + "_" + getCurrentTime().getTime)
             errorMessage = "No of column counts in some rows are not same as expected"
          }       
          
          logger.debug(" - matchedRecords and unMatchedRecords - "
                    + " - processedCount - " + processedCount
                    + " - failedCount - " + failedCount )
          //matchedRecords.show()          
                    
          if (xmlFileStream.rdd.isEmpty() ) {
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
          
          val currentExtTime = null
          validationStatus = "Inserted"   //hardcoded for testing
          val fileStatusRDD = sc.parallelize( Seq( Row( fileId, fileName, snapShotId, processedCount, failedCount, (processedCount.toInt + failedCount.toInt), validationStatus, errorMessage, currentExtTime) ) ) 
          val fileStatusDF = sqlContext.createDataFrame( fileStatusRDD, fileStatusSchema ).toDF()
          saveDataFrame(fileStatusDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_tracker")
                  
  			  val fileControllerRDD = sc.parallelize( Seq( Row( jobId, fileName, fileId, xmlSchema ) ) ) 
          val fileControllerDF = sqlContext.createDataFrame( fileControllerRDD, fileControllerSchema ).toDF()
          //saveDataFrame(fileControllerDF.withColumn("update_time", lit(getCurrentTime())), "wip_configurations", "file_controller")
          
          */
          logger.debug(" file " + fileName + " processed  at " + getCurrentTime())
           
	   }
		 catch {
		   case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("XML file processing error : " + e.getMessage )
            e.printStackTrace()
            status = "1"
            throw new Exception("Error while processing xml File at : " + e.getMessage + ": " + e.printStackTrace())
            
			}  
			return status
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
      .select( fileIngestionStatus("creation_time"), fileIngestionStatus("file_name"), fileIngestionStatus("file_id"), fileIngestionStatus("validation_status") )
      .filter(s"file_id = 1 AND validation_status = 'Ingested' ")
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