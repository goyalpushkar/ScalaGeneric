package com.cswg

import org.slf4j.{LoggerFactory, MDC }

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}
import java.io.File
import java.sql.{ Timestamp, ResultSet, PreparedStatement }
import java.lang.{Long, Boolean}

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ListBuffer

import com.typesafe.config.ConfigFactory
import scala.collection.mutable.{Map => MMap}
import oracle.sql.{ARRAY, ArrayDescriptor, STRUCT, StructDescriptor}
import oracle.jdbc.{OracleCallableStatement, OracleTypes, OraclePreparedStatement}

import org.apache.commons.dbcp._;

package object fileProcess {
  var logger = LoggerFactory.getLogger(this.getClass);   //"Examples"
  
  var confFilePath = "/opt/wildfly/properties/"  // This is configuration file
  var oracleConnectionPool = new BasicDataSource();
  var oracleConn: java.sql.Connection = null; 
  
  val dateTimeFormat = "MM/DD/YYYY HH:mm:ss"
  val dateFormat = "MM/DD/YYYY HH:mm:ss"
  
  val parameterValueSplitter = ";";
  val TimestampType = "Timestamp";
	val DateType = "Date";
	val StringType = "String";
	val IntegerType = "Integer";
	val LongType = "Long";
	val BooleanType = "Boolean";
	
	val success = "Success"
	val error = "Error"
	
  //var filePath = "/opt/wildfly/rcm/applogs/"     //This is used to generate log file or csv file
  //var dfDataTypeMapping: DataFrame = null
  
  def getCurrentTime(format: String) : java.sql.Timestamp = {
     val now = Calendar.getInstance()
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")     
     return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)
  }
  
  def getCurrentTimeSt(format: String) : String = {
     val now = Calendar.getInstance()
     val sdf = new SimpleDateFormat(format)     
     return sdf.format(now.getTimeInMillis)
  }
      
  def addDaysHours(opsDate: Timestamp, numberOfDays: Int, format: String, Unit: String) : java.sql.Timestamp = {
    var sdf = new SimpleDateFormat(format)  
    var secondsAdd = 0;
    if ( Unit.equalsIgnoreCase("DAYS") )
      secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    else if ( Unit.equalsIgnoreCase("HOURS") ) 
      secondsAdd = numberOfDays * ( 60 * 60 * 1000 ) 
    else if ( Unit.equalsIgnoreCase("MINUTES") ) 
      secondsAdd = numberOfDays * ( 60 * 1000 ) 
    return new java.sql.Timestamp(sdf.parse(sdf.format(opsDate.getTime + secondsAdd)).getTime)
  } 
     
  def getStringtoDate(date: String, format: String): java.sql.Date = {
      try{ 
            //Added to handle default excel formats
           var cellFormat: String = ""
           if ( format.equalsIgnoreCase("m/d/yy h:mm") )  //22
              cellFormat = "MM/dd/yyyy hh:mm"  
          else if ( format.equalsIgnoreCase("m/d/yy")  ) //14
              cellFormat = "MM/dd/yyyy"     
    			 else
    					cellFormat = format
					
            val sdf = new SimpleDateFormat(cellFormat);
            return new java.sql.Date(sdf.parse(date).getTime())
      }catch{
          case e @ ( _: java.sql.SQLException | _: Exception | _: Error) => {
             logger.error("Error occurred while getting Date from String for value - " + date + " :format - " + format + " - " + e.getLocalizedMessage() )
             val sdf = new SimpleDateFormat("MM/dd/yyyy");
             return new java.sql.Date(sdf.parse(date).getTime())
          }
      }
  }
    
  def getMonthName(monthNumber: Int): String = {  //, date: String, format: String
      import java.util.Locale
      import java.time.Month
      
      return Month.of(monthNumber).toString()
      /*new DateFormatSymbols().getMonths()
      
      var cellFormat: String = ""
       if ( format.equalsIgnoreCase("m/d/yy h:mm") )  //22
          cellFormat = "MM/dd/yy hh:mm"  
      else if ( format.equalsIgnoreCase("m/d/yy")  ) //14
          cellFormat = "MM/dd/yy"     
			 else
					cellFormat = format
      val sdf = new SimpleDateFormat(cellFormat);
      val c = Calendar.getInstance();
      val returnedDate = sdf.parse(date).getTime
      return c.getDisplayName(Calendar.MONTH, returnedDate.toInt, Locale.ENGLISH)*/  
  }
  
  def convertCamelCase(passedValue: String): String = {
      import org.apache.commons.lang3.StringUtils;
      import org.apache.commons.lang3.text.WordUtils;
     
      return WordUtils.capitalizeFully(passedValue)
      //.remove(WordUtils.capitalizeFully(passedValue, '_'), "_")
  
  }
  
    def getStringtoDateTime(date: String, format: String): java.sql.Timestamp = {
     try{
       logger.debug( "date - " + date + " :format - " + format ) 
       //Added to handle default excel formats
       var cellFormat: String = ""
       if ( format.equalsIgnoreCase("m/d/yy h:mm") )  //22
          cellFormat = "MM/dd/yy hh:mm"  
      else if ( format.equalsIgnoreCase("m/d/yy")  ) //14
          cellFormat = "MM/dd/yy"     
			 else
					cellFormat = format
					
       val sdf = new SimpleDateFormat(cellFormat);
       
       return new java.sql.Timestamp(sdf.parse(date).getTime() )
     }catch {
       case e @ ( _: java.sql.SQLException | _: Exception | _: Error) => {
             logger.error("Error occurred while getting Date from String for value - " + date + " :format - " + format + " - " + e.getLocalizedMessage() )
             val sdf = new SimpleDateFormat("MM/dd/yyyy");
             return new java.sql.Timestamp(sdf.parse(date).getTime())
          }
               
     }
     
  }
  
  def getExcelDatetoDate(date: String, format: String): java.util.Date = {
       val sdf = new SimpleDateFormat(format);            
       return sdf.parse(date)
  }
  
  def getDoubletoDate(date: Double, format: String): String = {
       val sdf = new SimpleDateFormat(format).format(date);            
       return sdf
  }
  
  def getExcelDatetoString(date: String, format: String): String = {
       val sdf = new SimpleDateFormat(format);            
       return new java.sql.Timestamp(sdf.parse(date).getTime).toString()
  }
  
  def getStringtoInt(str: String): Option[Int] = {
      import scala.util.control.Exception._
      
      catching( classOf[NumberFormatException] ) opt str.toInt
  }
   
  def getStringtoFloat(str: String): Option[Float] = {
      import scala.util.control.Exception._
      
      catching( classOf[NumberFormatException] ) opt str.toFloat
  }
  
  def getStringtoDouble(str: String): Option[Double] = {
      import scala.util.control.Exception._
      
      catching( classOf[NumberFormatException] ) opt str.toDouble
  }
  
    sealed trait ReturnType
  
  def getFormattedValue(origValue: Any, dataType: String, format: String, subType: String = "", subTypeName: String = ""): Object  = {  //Any  Object
     		           
     if ( dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("varchar2") || dataType.equalsIgnoreCase("string") )
        origValue.asInstanceOf[Object]
     else if ( dataType.equalsIgnoreCase("int") || dataType.equalsIgnoreCase("number") )
        getStringtoInt(origValue.toString() ).getOrElse(null).asInstanceOf[Object]
     else if ( dataType.equalsIgnoreCase("float") )
        getStringtoFloat(origValue.toString()).getOrElse(null).asInstanceOf[Object]
     else if ( dataType.equalsIgnoreCase("double") )
        getStringtoDouble(origValue.toString()).getOrElse(null).asInstanceOf[Object]
     else if ( dataType.equalsIgnoreCase("date") )
        getStringtoDate(origValue.toString(), format).asInstanceOf[Object]   //getStringtoDate .asInstanceOf[Object] 
     else if ( dataType.equalsIgnoreCase("timestamp") )
        getStringtoDate(origValue.toString(), format).asInstanceOf[Object]  //getStringtoDateTime .asInstanceOf[Object]
     else if ( dataType.equalsIgnoreCase("array") ){
        if ( subType.equalsIgnoreCase("object") ){
            val vendorProfileStructDesc = StructDescriptor.createDescriptor(subTypeName, oracleConn)
            var arrayObject: Array[Object] = new Array[Object](0)
            ( if ( origValue != null ) 
                 if ( !origValue.equals("") )
                    arrayObject = origValue.asInstanceOf[Array[Object]]
              /*else
                arrayObject = new Array[Object](0) */
            )
            val vendorProfileChainStruct: STRUCT = new STRUCT(vendorProfileStructDesc
                									                           ,oracleConn
                									                           ,arrayObject
												                                     );
            return vendorProfileChainStruct
        }else if ( subType.equalsIgnoreCase("table") ){
            val vendorProfileArrayDesc = ArrayDescriptor.createDescriptor(subTypeName, oracleConn)
            var arrayObject: Array[Object] = new Array[Object](0)
            ( if ( origValue != null ) 
                 if ( !origValue.equals("") )
                    arrayObject = origValue.asInstanceOf[Array[Object]]
              /*else
                arrayObject = new Array[Object](0) */
            )
            val vendorProfileChainStruct: ARRAY = new ARRAY(vendorProfileArrayDesc
              									                            ,oracleConn
              									                            ,arrayObject //.toArray
											                                      );
            return vendorProfileChainStruct
        }else{
           ( if ( origValue != null && origValue != null ) 
                 origValue.asInstanceOf[ARRAY]
              else
                 new Array[Object](0) 
            )
        }
     }else
       origValue.asInstanceOf[Object] //.asInstanceOf[Object]
  }
    
  def inParam(paramName: String, value: Any, dataType: String, preparedStmt: OracleCallableStatement ) = 
    
    
    dataType.toUpperCase() match { //OracleTypes.n
    case ( "VARCHAR" |  "STRING" | "TEXT" ) => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.VARCHAR ) else preparedStmt.setString(paramName, value.toString() )
    //case "NUMBER" => preparedStmt.setNUMBER(paramName, value.asInstanceOf[oracle.sql.NUMBER])
    case ( "INT" | "NUMBER" ) => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.NUMBER ) else preparedStmt.setInt(paramName, value.asInstanceOf[Int])
    case "FLOAT" => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.FLOAT ) else  preparedStmt.setFloat(paramName, value.asInstanceOf[Float])
    case "DOUBLE" => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.DOUBLE ) else preparedStmt.setDouble(paramName, value.asInstanceOf[Double])
    case "DATE" => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.DATE ) else preparedStmt.setDate(paramName, value.asInstanceOf[java.sql.Date])
    case "TIMESTAMP" => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.TIMESTAMP ) else  preparedStmt.setTimestamp(paramName, value.asInstanceOf[java.sql.Timestamp])
    case "ARRAY" => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.ARRAY ) else preparedStmt.setArray(paramName, value.asInstanceOf[ARRAY])
    case _ => if ( value == null ) preparedStmt.setNull(paramName, OracleTypes.NUMBER ) else preparedStmt.setArray(paramName, value.asInstanceOf[ARRAY] )
  }
  
  def outParam(paramName: String, dataType: String, preparedStmt: OracleCallableStatement, arrayName: String ) = 
    dataType.toUpperCase() match {
    case ( "VARCHAR" |  "STRING" | "TEXT" ) => preparedStmt.registerOutParameter(paramName, OracleTypes.VARCHAR)
    //case "NUMBER" => preparedStmt.registerOutParameter(paramName, OracleTypes.NUMBER)
    case ( "INT" | "NUMBER" ) => preparedStmt.registerOutParameter(paramName, OracleTypes.INTEGER)
    case "FLOAT" => preparedStmt.registerOutParameter(paramName, OracleTypes.FLOAT)
    case "DOUBLE" => preparedStmt.registerOutParameter(paramName, OracleTypes.DOUBLE)
    case "DATE" => preparedStmt.registerOutParameter(paramName, OracleTypes.DATE)
    case "TIMESTAMP" => preparedStmt.registerOutParameter(paramName, OracleTypes.TIMESTAMP)
    case "ARRAY" => preparedStmt.registerOutParameter(paramName, OracleTypes.ARRAY, arrayName)
    //case arrayName => preparedStmt.registerOutParameter(paramName, OracleTypes.ARRAY, arrayName)
    case _ => preparedStmt.registerOutParameter(paramName, OracleTypes.ARRAY, dataType)
  }
  
  def inParamPosition(paramIndex: Int, value: Any, dataType: String, preparedStmt: OraclePreparedStatement ) =  //OracleCallableStatement 
    
    
    dataType.toUpperCase() match { //OracleTypes.n
    case ( "VARCHAR" |  "STRING" | "TEXT" ) => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.VARCHAR ) else preparedStmt.setString(paramIndex, value.toString() )
    //case "NUMBER" => preparedStmt.setNUMBER(paramIndex, value.asInstanceOf[oracle.sql.NUMBER])
    case ( "INT" | "NUMBER" ) => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.NUMBER ) else preparedStmt.setInt(paramIndex, value.asInstanceOf[Int])
    case "FLOAT" => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.FLOAT ) else  preparedStmt.setFloat(paramIndex, value.asInstanceOf[Float])
    case "DOUBLE" => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.DOUBLE ) else preparedStmt.setDouble(paramIndex, value.asInstanceOf[Double])
    case "DATE" => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.DATE ) else preparedStmt.setDate(paramIndex, value.asInstanceOf[java.sql.Date])
    case "TIMESTAMP" => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.TIMESTAMP ) else  preparedStmt.setTimestamp(paramIndex, value.asInstanceOf[java.sql.Timestamp])
    case "ARRAY" => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.ARRAY ) else preparedStmt.setArray(paramIndex, value.asInstanceOf[ARRAY])
    case _ => if ( value == null ) preparedStmt.setNull(paramIndex, OracleTypes.NUMBER ) else preparedStmt.setArray(paramIndex, value.asInstanceOf[ARRAY] )
  }
  
  def getValue(paramName: String, dataType: String, preparedStmt: OracleCallableStatement, arrayName: String ) = 
    dataType.toUpperCase() match {
    case ( "VARCHAR" |  "STRING" | "TEXT" ) => preparedStmt.getString(paramName)
    //case "NUMBER" => preparedStmt.registerOutParameter(paramName, OracleTypes.NUMBER)
    case ( "INT" | "NUMBER" ) => preparedStmt.getInt(paramName)
    case "FLOAT" => preparedStmt.getFloat(paramName)
    case "DOUBLE" => preparedStmt.getDouble(paramName)
    case "DATE" => preparedStmt.getDate(paramName)
    case "TIMESTAMP" => preparedStmt.getTimestamp(paramName)
    case "ARRAY" => preparedStmt.getArray(paramName)
    //case arrayName => preparedStmt.registerOutParameter(paramName, OracleTypes.ARRAY, arrayName)
    case _ => preparedStmt.getString(paramName)
  }
      
    
  def getEnvironmentName(environment: String): (String, String) = {
      logger.debug("----------getEnvironmentName----------");
      val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "environment.conf")) 
      val conf = ConfigFactory.load(parsedFile)
      val oracleEnvName  = conf.getString("environment." + environment + ".oracleEnv")
      val cassandraEnvName  = conf.getString("environment." + environment + ".cassandraEnv")
      logger.debug("oracleEnvName - " + oracleEnvName + "\n" + "cassandraEnvName - " + cassandraEnvName)
      
      (oracleEnvName, cassandraEnvName)
    }
   
   def getCassandraConnectionProperties(environment: String): (String, String, String) = {
       logger.debug("----------getCassandraConnectionProperties----------");
       val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "cassandra_properties.conf")) 
       val conf = ConfigFactory.load(parsedFile)
       val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
       
       val connectionHost = conf.getString("cassandra." + cassandraEnv + ".host")
       val userName = conf.getString("cassandra." + cassandraEnv + ".username")
       val password = conf.getString("cassandra." + cassandraEnv + ".password")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
       (connectionHost, userName, password)
   }
   
   def getOracleConnectionProperties(environment: String): (String, String, String, String) = {
      logger.debug("----------getOracleConnectionProperties----------");
       val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "oracle_properties.conf"))
       val conf = ConfigFactory.load(parsedFile)
       val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
       
       val connectionHost = conf.getString("oracle." + cassandraEnv + ".host")
       val userName = conf.getString("oracle." + cassandraEnv + ".username")
       val password = conf.getString("oracle." + cassandraEnv + ".password")
       val noOfConnections = conf.getString("oracle." + cassandraEnv + ".numberOfConnections")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password + "\n" + "noOfConnections - " + noOfConnections)
      
       (connectionHost, userName, password, noOfConnections)
   }
   
   def createConnectionPool(environment: String) {
    	logger.debug("----------createConnectionPool----------");
    	
    	val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
      val (connection, username, password, noOfConn) = getOracleConnectionProperties(oracleEnv);
      try {
               // make the connection
    	       logger.info( " - OracleConnection - " + connection + "\n"
    	    		       + " - OracleUserName - " + username + "\n"
    	    		       + " - OraclePassword - " + password + "\n"
    	    		       + " - NumberOfConnections - " + noOfConn + "\n"    
    	    		   ); 
               oracleConnectionPool.setDriverClassName("oracle.jdbc.driver.OracleDriver");
               oracleConnectionPool.setUrl(connection);
               oracleConnectionPool.setUsername(username);
               oracleConnectionPool.setPassword(password);
               oracleConnectionPool.setInitialSize( Integer.parseInt(noOfConn) );
               oracleConnectionPool.setDefaultAutoCommit(false);
			         //OracleConnectionPool.setLoginTimeout(60);
               oracleConnectionPool.setMaxWait(120);
               //oracleConnectionPool.setAccessToUnderlyingConnectionAllowed(true)
               //accessToUnderlyingConnectionAllowed
        } 
         catch {
         case e @ ( _: java.sql.SQLException | _: Exception | _: Error) => 
            logger.error("Error while executing createConnectionPool at : " +  e.getLocalizedMessage() );
         } 
    } 
    
  /*case class returnResult( status: String, message: String, result: ResultSet, errorData: String )
   
  def runQuery( query: String, parameters: MMap[String, String] ) : returnResult = { 
		logger.debug( "------------Inside runQuery------------------");
		var returnResultSet: java.sql.ResultSet  = null;
		var errorMsg: String = null;

		logger.debug( " - Query - " + query);
		try {
			if ( oracleConn == null ){
			  oracleConn = oracleConnectionPool.getConnection(); 
			}
			//oracleConnectionPool.getConnection().setNetworkTimeout(null, 60000);
			val warnings = oracleConn.getWarnings();
			while( warnings != null ){
			    logger.debug("*** SQL Warning *** " + "\n"
			              + "   State:   " + warnings.getSQLState() + "\t" 
			              + " - Message: " + warnings.getMessage() + "\t"
			              + " - Error:   " + warnings.getErrorCode()
						   );
			    warnings.getNextWarning()
			}
			
			var executionQuery: PreparedStatement = oracleConn.prepareStatement(query);
			/*for (  int index=1; index <= parameters.size(); index++ ){
				logger.debug("parameter value - " + parameters.get(index-1));
				executionQuery.setTimestamp(index, utilObject.convertStringTimestamp( parameters.get(index-1) ) );
				//("last_run_time", parameters.get(0));
			}*/
			parameters.foreach{f => 
			   val parameterType  = f._2.split(parameterValueSplitter)(0);
				 val parameterValue = f._2.split(parameterValueSplitter)(1);
				 val parameterKey = f._1
				logger.info( "parameterKey - " + parameterKey +  "\t"  + " - parameterType - " + parameterType + "\t" + " - parameterValue - " + parameterValue);
				
				if ( parameterType.equalsIgnoreCase(TimestampType) ){
					executionQuery.setTimestamp( Integer.parseInt(parameterKey), getStringtoDateTime( parameterValue, dateTimeFormat) );				
				} 
				if ( parameterType.equalsIgnoreCase(DateType) ){
					executionQuery.setDate( Integer.parseInt(parameterKey), getStringtoDate( parameterValue, dateTimeFormat ) );				
				}
				if ( parameterType.equalsIgnoreCase(StringType) ){
					executionQuery.setString( Integer.parseInt(parameterKey), parameterValue );				
				} 
				if ( parameterType.equalsIgnoreCase(IntegerType) ){
					executionQuery.setInt( Integer.parseInt(parameterKey), Integer.parseInt( parameterValue ) );				
				} 
				if ( parameterType.equalsIgnoreCase(LongType) ){
					executionQuery.setLong( Integer.parseInt(parameterKey), Long.parseLong( parameterValue ) );				
				}
				if ( parameterType.equalsIgnoreCase(BooleanType) ){
					executionQuery.setBoolean( Integer.parseInt(parameterKey), Boolean.valueOf(parameterValue) );				
				}
				
			}
			executionQuery.setQueryTimeout(60);
			logger.info("Final executionQuery - " + executionQuery);
			returnResultSet = executionQuery.executeQuery();
			
			//oracleConn.close();   If we add this then program will through error - java.sql.SQLException: Closed Resultset
			// as conection is closed before fetching the resultSet.
		} 
		catch {
         case e @ ( _: java.sql.SQLException | _: Exception | _: Error) => 
            logger.error("Run Query Error : " +  e.getLocalizedMessage() );
            errorMsg =  errorMsg.concat( e.getLocalizedMessage() );
         } 
		
		return returnResult.apply( if ( errorMsg == null ) success else error , errorMsg, returnResultSet, query)
	}*/
  
 /* def removeMinSecondsFromDate(dateStr:String, format: String) : java.sql.Timestamp = {
        var sdf = new SimpleDateFormat(format);  
        val parsedDate = sdf.parse(dateStr);
        val now = Calendar.getInstance()
        now.setTime(parsedDate)
        now.set(Calendar.MILLISECOND, 0)
        now.set(Calendar.SECOND, 0)
        now.set(Calendar.MINUTE, 0)
        return new java.sql.Timestamp(sdf.parse(sdf.format(now.getTimeInMillis)).getTime)
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
    }*/

}