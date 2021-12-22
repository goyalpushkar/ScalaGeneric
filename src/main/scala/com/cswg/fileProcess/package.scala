package com.cswg

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.slf4j.{LoggerFactory, MDC }

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}
import java.io.File
import java.sql.Timestamp

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ListBuffer

import com.typesafe.config.ConfigFactory
import scala.collection.mutable.{Map => MMap}

package object fileProcess {
  var logger = LoggerFactory.getLogger(this.getClass);   //"Examples"
  
  var filePath = "/opt/cassandra/applicationstack/applications/wip/log/" 
  var confFilePath = "/opt/cassandra/applicationstack/applications/wip/resources/" 
  var dfDataTypeMapping: DataFrame = null
  
  def getCurrentTime() : java.sql.Timestamp = {
     val now = Calendar.getInstance()
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)

  }
  
  def getCurrentTime(runTimeStr:String) : java.sql.Timestamp = {
    // val now = Calendar.getInstance()
    //return new java.sql.Timestamp(now.getTimeInMillis)
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return new java.sql.Timestamp(sdf.parse(runTimeStr).getTime())
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
   
  def getPreviousDt(numberOfDays: Int) : String = {
    val now = Calendar.getInstance()
    now.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd")     
    return sdf.format(now.getTime)
  }
  
    def getPreviousOpsDate(opsDate: Timestamp, numberOfDays: Int) : java.sql.Timestamp = {
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")   
    val secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    return new java.sql.Timestamp(sdf.parse(sdf.format(opsDate.getTime + secondsAdd)).getTime)
  } 
  
  def removeHours(opsDate: Timestamp, numberOfDays: Int) : java.sql.Timestamp = {
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")   
    val secondsAdd = numberOfDays * ( 60 * 60 * 1000 ) 
    return new java.sql.Timestamp(sdf.parse(sdf.format(opsDate.getTime + secondsAdd)).getTime)
  } 
  
  def getNextTime(dateAdd: Timestamp, numberOfDays: Int) : java.sql.Timestamp = {
    //val now = Calendar.getInstance()
    //dateAdd.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd")   
    val secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    return new java.sql.Timestamp(sdf.parse(sdf.format(dateAdd.getTime + secondsAdd)).getTime)
  }
  
  def getNextDate(dateAdd: Timestamp, numberOfDays: Int) : String = {
    //val now = Calendar.getInstance()
    //dateAdd.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd") 
    val secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    return sdf.format( dateAdd.getTime + secondsAdd ) 
  }  
  
  def getId(id_name: String ): String = {
      return UUID.fromString(id_name).toString()
   }
  
  def generateUUID = udf( () => UUID.randomUUID().toString() )
          
  def removeMinSecondsFromDate(dateStr:String) : java.sql.Timestamp = {
  var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
  val parsedDate = sdf.parse(dateStr);
    val now = Calendar.getInstance()
    now.setTime(parsedDate)
    now.set(Calendar.MILLISECOND, 0)
    now.set(Calendar.SECOND, 0)
    now.set(Calendar.MINUTE, 0)
    return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)
  }
  
  def convertDateFormat(timeStr:String):java.sql.Timestamp = { 
     var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
    var  convertedFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   
    var  date:java.util.Date = simpleDateFormat.parse(timeStr);
    val ans = convertedFormat.format(date) 
 
    return new java.sql.Timestamp(convertedFormat.parse(ans).getTime)
      
      //return minInterval.intValue()
    }
  
  def convertToLong(timeStr:String):Long = { 
    
    var  convertedFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   
    var  date:java.util.Date = convertedFormat.parse(timeStr);
    val cdate = convertedFormat.format(date) 
 
    return new java.sql.Timestamp(convertedFormat.parse(cdate).getTime).getTime
      
      //return minInterval.intValue()
    }
  
  def getStringtoDate(date: String): java.sql.Timestamp = {
     val sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
     return new java.sql.Timestamp(sdf.parse(date).getTime())
  }
   
  def getNextDatePv(dateAdd: Timestamp, numberOfDays: Int) : java.sql.Timestamp = {
    //val now = Calendar.getInstance()
    //dateAdd.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd")   
    val secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    return new java.sql.Timestamp(sdf.parse(sdf.format(dateAdd.getTime + secondsAdd)).getTime)
  }
  
  def addDaystoCurrentDatePv(numberOfDays: Int) : String = {
    val now = Calendar.getInstance()
    now.add(Calendar.DATE, numberOfDays)
    var sdf = new SimpleDateFormat("yyyy-MM-dd")     
    return sdf.format(now.getTime)
  }
  
   def getPreviousDate(dateStr : String): java.sql.Timestamp = {
   var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   val parsedDate = sdf.parse(dateStr);
   
   var c = Calendar.getInstance()  
   c.setTime(parsedDate)
   c.add(Calendar.DATE, -1)
   c.set(Calendar.HOUR_OF_DAY, 0);
   c.set(Calendar.MINUTE, 0);
   c.set(Calendar.SECOND, 0);
   
   return new java.sql.Timestamp(sdf.parse(sdf.format(c.getTime())).getTime)
 }       
  
  def   getCurrentHourFromDate(date: String): Integer = {
    val cal = Calendar.getInstance()
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val parsedDate = sdf.parse(date);
    cal.setTime(parsedDate);

    return cal.get(Calendar.HOUR_OF_DAY)
  }   
   
  def getEnvironmentName(environment: String): (String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "environment.conf")) ///opt/cassandra/applicationstack/applications/wip/resources
      val conf = ConfigFactory.load(parsedFile)
      val oracleEnvName  = conf.getString("environment." + environment + ".oracleEnv")
      val cassandraEnvName  = conf.getString("environment." + environment + ".cassandraEnv")
      logger.debug("oracleEnvName - " + oracleEnvName + "\n" + "cassandraEnvName - " + cassandraEnvName)
      
      (oracleEnvName, cassandraEnvName)
    }
   
   def getCassandraConnectionProperties(environment: String): (String, String, String) = {
       val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "cassandra_properties.conf"))  ///opt/cassandra/applicationstack/applications/wip/resources/
       val conf = ConfigFactory.load(parsedFile)
       val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
       
       val connectionHost = conf.getString("cassandra." + cassandraEnv + ".host")
       val userName = conf.getString("cassandra." + cassandraEnv + ".username")
       val password = conf.getString("cassandra." + cassandraEnv + ".password")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
       (connectionHost, userName, password)
   }
   
  def sparkContext(environment: String, appName: String): ( SparkConf, SparkContext ) = {
     logger.info( " - environment - " + environment )
     val (hostName, userName, password) = getCassandraConnectionProperties(environment)
         
     val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      .set("spark.cassandra.output.ignoreNulls","true")
      //.set("spark.eventLog.enabled", "true" )
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .setMaster("local[*]") // This is required for IDE run
     
     val sc = new SparkContext(conf)
    
     ( conf, sc )
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
    
  def getUUID(): String = {
      return UUID.randomUUID().toString()       //.timestamp()       
  }
  
   def add_minutes(dateTime: Timestamp, minutes: Int ): Timestamp = {
       return new Timestamp(dateTime.getTime() + minutes * 60 * 1000 )  //* 60 
   }
   

   def add_hrs(dateTime: Timestamp, hrs: Int ): Timestamp = {
       return new Timestamp(dateTime.getTime() + hrs * 3600 * 1000 )  //* 60 
   }   
     
  def replaceDot( valueString: String, noOfOccurences: Int ): String = {
        //val valueString = "item_flex_attrgroupmany.row.attr"
        //val noOfOccurences = 1
        //logger.debug( " - Before Change - " + valueString)
        var index = 0        
        val afterChangeValue = valueString.flatMap{ x =>  
                                                      //logger.debug( " x - " + x )
                                                      if ( x == ".".charAt(0) ){
                                                         index = index + 1
                                                         //logger.debug("value matched - " + index)
                                                         if ( index <= noOfOccurences )
                                                             "_"
                                                         else x.toString()
                                                      }else{
                                                         //logger.debug("value not matched")
                                                         x.toString()
                                                      }
                                                }
        
        //logger.debug( " - After Change - " + afterChangeValue )
        
        /*val regexValue = ".".r
        logger.debug( " - Before Change2 - " + valueString + " - " + regexValue)
        val afterChangeValue2 = regexValue.replaceFirstIn(valueString, "_")
        logger.debug( " - After Change2 - " + afterChangeValue2)*/
        
        afterChangeValue
    }
  
    def getProfileValuePv( sqlContext: SQLContext,profileCode: String, profileLevelCode: String, profileLevelValue: Array[Row], appName: String ): Map[String,String] = {
      val dfLookupConfig =  sqlContext.read
                         .format("org.apache.spark.sql.cassandra")
                         .options( Map( "table" -> "lookup_profiles_config"
                                      , "keyspace" -> "wip_configurations") )
                         .load()

      logger.debug( " Inside getProfileValuePv "+  "\n"
                      + " profileCode - " + profileCode + "\n"
                      + " profileLevelCode - " + profileLevelCode + "\n"
                      + " appName - " + appName + "\n"
                      )    
      var profileValue = ""
      var configmap1:Map[String,String] = Map()
      var profileValueArray: Option[Array[Row]] = None
      var wh_id : String = null
      try{ 
          for (i <- profileLevelValue) {
            wh_id = i.getString(0)
            profileValueArray = Some( dfLookupConfig.filter(s" lookup_code = '$profileCode' AND level_code = '$profileLevelCode' AND CAST( level_value as Int) = '$wh_id' ")
            .select("profile_value").collect() )
                        
            if ( profileValueArray.get.isEmpty ) {
                profileValueArray = Some( dfLookupConfig.filter(s" lookup_code = '$profileCode' AND level_code = '-1' AND level_value = 'Site' ")
                .select("profile_value").collect() )
            }
                       
            profileValue =  profileValueArray.get.apply(0).getString(0)                          
            
            configmap1 += (i.getString(0) -> profileValue)
          }  
          
      }catch{
            case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing getProfileValuePv at : " +  e.getMessage + " "  +  e.printStackTrace() )
      }      
      
      configmap1
  } 
    
  def generateFile(dataframe: DataFrame, orderByClause: String, path: String ) = {
      logger.debug( "generateFile" );
      var orderBy1 = "";
      var orderBy2 = "";
      var orderBy3 = "";
      var orderBy4 = "";
      var orderBy5 = "";
      var i = 1;
      val orderBySeq = orderByClause.split(",").toSeq.map { x => x.trim()  }  // "\"" + + "\""
                        //.mkString(", ")
                      //.split(",")
      
      for ( OrderByString <- orderBySeq ) {
          if ( i == 1 ) orderBy1 = OrderByString
          if ( i == 2 ) orderBy2 = OrderByString
          if ( i == 3 ) orderBy3 = OrderByString
          if ( i == 4 ) orderBy4 = OrderByString
          if ( i == 5 ) orderBy5 = OrderByString
          i += 1;
      }
      
      //val orderByString = orderBySeq.mkString(",")
      
      //val orderByString = if ( path == "LiftWorkAssignments" ) { "warehouse_id", "ops_date", "aisle" } else { "warehouse_id", "ops_date", "employee_number" }  
      logger.debug( "Order By - " + orderBySeq + "\n" 
                  + "orderBy1 - " + orderBy1 + "\n" 
                  + "orderBy2 - " + orderBy2 + "\n"
                  + "orderBy3 - " + orderBy3 + "\n" 
                  + "orderBy4 - " + orderBy4 + "\n" 
                  + "orderBy5 - " + orderBy5 + "\n" 
                  )
      
      if ( orderBy5.equals("") ) {
         if ( orderBy4.equals("") ){
            if ( orderBy3.equals("") ) {
               if ( orderBy2.equals("") ) {
                  if ( orderBy1.equals("") ) {
                     dataframe
                         //.orderBy( "warehouse_id", "ops_date", "employee_number" ) //orderBySeq ) //expr( "ORDER BY " + orderByClause ) )
                         //.orderBy(orderBy1)
                         .coalesce(1)          
                         .write
                         .format("com.databricks.spark.csv")
                         .option("header", "true")
                         //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                         .mode("overwrite")
                         .save(filePath + path +  "/" )
                  }else{
                    dataframe
                       //.orderBy( "warehouse_id", "ops_date", "employee_number" ) //orderBySeq ) //expr( "ORDER BY " + orderByClause ) )
                       //.orderBy(orderBy1, orderBy2, orderBy3, orderBy4, orderBy5)
                       .coalesce(1)          
                       .write
                       .format("com.databricks.spark.csv")
                       .option("header", "true")
                       //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                       .mode("overwrite")
                       .save(filePath + path +  "/" )
                  }
               }else{
                 dataframe
                   //.orderBy( "warehouse_id", "ops_date", "employee_number" ) //orderBySeq ) //expr( "ORDER BY " + orderByClause ) )
                   //.orderBy(orderBy1, orderBy2)
                   .coalesce(1)          
                   .write
                   .format("com.databricks.spark.csv")
                   .option("header", "true")
                   //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                   .mode("overwrite")
                   .save(filePath + path +  "/" )
               }
            }else{
              dataframe
               //.orderBy( "warehouse_id", "ops_date", "employee_number" ) //orderBySeq ) //expr( "ORDER BY " + orderByClause ) )
               //.orderBy(orderBy1, orderBy2, orderBy3)
               .coalesce(1)          
               .write
               .format("com.databricks.spark.csv")
               .option("header", "true")
               //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
               .mode("overwrite")
               .save(filePath + path +  "/" )
            }
         }else {
           dataframe
           //.orderBy( "warehouse_id", "ops_date", "employee_number" ) //orderBySeq ) //expr( "ORDER BY " + orderByClause ) )
           //.orderBy(orderBy1, orderBy2, orderBy3, orderBy4)
           .coalesce(1)          
           .write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
           .mode("overwrite")
           .save(filePath + path +  "/" )
         }
        
      }else{
          dataframe
           //.orderBy( "warehouse_id", "ops_date", "employee_number" ) //orderBySeq ) //expr( "ORDER BY " + orderByClause ) )
           //.orderBy(orderBy1, orderBy2, orderBy3, orderBy4, orderBy5)
           .coalesce(1)          
           .write
           .format("com.databricks.spark.csv")
           .option("header", "true")
           //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
           .mode("overwrite")
           .save(filePath + path +  "/" )
      }
  }
  
  def readFile(sqlContext: SQLContext, dataFrame: DataFrame, fileLocation: String): DataFrame = {
      val dataFrameOut = sqlContext
        .read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load(filePath)
      
      dataFrameOut   
  }
  
  def writeDataFrame(dataFrame: DataFrame, targetKeyspace: String, targetTable: String, targetInstance: String) = {
      logger.debug( " - writeDataFrame - " + "\n"
                  + " - targetKeyspace - " + targetKeyspace + "\n"
                  + " - targetTable - " + targetTable + "\n" 
                 );
        
       dataFrame
       .write
       .format("org.apache.spark.sql.cassandra")
       .options( Map("keyspace" -> targetKeyspace
                     ,"table" -> targetTable ))
       .mode("append")
       .save()
  }
  
  def saveDataFrameM(target: String, dataframe: DataFrame, orderByClause: String, path: String, targetKeyspace: String, targetTable: String, targetInstance: String) = {
      logger.debug( "target - " + target + "\n" 
                  + "orderByClause - " + orderByClause + "\n"
                  + "path - " + path + "\n"
                  + "targetKeyspace - " + targetKeyspace + "\n"
                  + "targetTable - " + targetTable + "\n"
                  + "targetInstance - " + targetInstance + "\n"
                  //+ "loggingEnabled - " + loggingEnabled + "\n"
                  //+ "dataframe Count - " + dataframe.count() + "\n"    
                  )
                  
      /*val spark = SparkSession
                   .builder()
                   .appName("SparkSessionZipsExample")
                   .config("spark.sql.warehouse.dir", warehouseLocation)
                   .enableHiveSupport()
                   .getOrCreate()*/ // Supported in Spark 2.0
   
      if ( target.equalsIgnoreCase("file") ) {
        generateFile(dataframe, orderByClause, path)
        //null
      }else {
        writeDataFrame(dataframe, targetKeyspace, targetTable, targetInstance)
      }
    
  }    
}