package com.cswg.wip.neo4j

import org.apache.spark.{SparkContext, SparkConf}

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID, Properties}
import java.io.{File, FileInputStream, Reader }
import java.sql.Timestamp

import com.typesafe.config.ConfigFactory

import org.slf4j.{Logger, LoggerFactory, MDC }

import java.sql.{DriverManager, Connection }
import java.text.ParseException
//import java.sql._

//import org.json.JSONObject;
import org.json.simple.{JSONArray, JSONObject}
import org.json.simple.parser.{JSONParser, ParseException }
import org.apache.commons.dbcp._

//import org.anormcypher._
//import play.api.libs.ws._

import org.neo4j.spark._
//import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.driver.v1.{GraphDatabase, AuthTokens, Session, StatementResult}
import org.neo4j.driver.v1.exceptions.Neo4jException;

import scala.collection.mutable.{ArrayBuffer => MArrayBuffer}
import scala.collection.mutable.{Map => MMap}
import org.apache.spark.sql.types.TimestampType

package object etl {
  val logger = LoggerFactory.getLogger(this.getClass())  //(this.getClass())
  var confFilePath = "/opt/cassandra/applicationstack/applications/wip/resources/"
  var environment: String = null
  var oracleEnvironment: String = "xxaud_csdwp"
  var neo4JEnvironment: String = null;
  var cassandraEnvironment: String = null;
  var oracleConnection: Connection = null;
  var neo4JSession:Session = null;
  var loggingEnabled: String = null;
  val parameterValueSplitter = ";";
  val TimestampType = "Timestamp";
	val DateType = "Date";
	val StringType = "String";
	val IntegerType = "Integer";
	val LongType = "Long";
	val BooleanType = "Boolean";
	var identificationName: String = null; 
  
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
     
  def getStringtoDate(date: String): java.sql.Timestamp = {
     val sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
     return new java.sql.Timestamp(sdf.parse(date).getTime())
  }
  
  def getPreviousDate(numberOfDays: Int) : String = {
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
  
  def getNextDate(dateAdd: Timestamp, numberOfDays: Int) : String = {
    var sdf = new SimpleDateFormat("yyyy-MM-dd") 
    val secondsAdd = numberOfDays * ( 24 * 60 * 60 * 1000 ) 
    return sdf.format( dateAdd.getTime + secondsAdd ) 
  }  
  
   def add_minutes(dateTime: Timestamp, minutes: Int ): Timestamp = {
       return new Timestamp(dateTime.getTime() + minutes * 60 * 1000 )  //* 60 
   }
   
   def add_hrs(dateTime: Timestamp, hrs: Int ): Timestamp = {
       return new Timestamp(dateTime.getTime() + hrs * 3600 * 1000 )  //* 60 
   }   
  
   def convertStringTimestamp(passedDate: String): java.sql.Timestamp = {
	     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	     try {
	    	 return new java.sql.Timestamp(sdf.parse(passedDate).getTime());
		   } catch {
  		  case e @ (_:java.text.ParseException) => 
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    			logger.error("Error occurred while converting string to Timestamp");
    			return null;
  		}
	}	

	def convertStringDate(passedDate: String):java.sql.Date =  {
	     val sdf = new SimpleDateFormat("yyyy-MM-dd"); 
	     try {
	    	 return new java.sql.Date(sdf.parse(passedDate).getTime());
		} catch {
  		  case e @ (_:java.text.ParseException) => 
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Error occurred while converting string to Date");
			return null;
		}
	}	
	
	
   //Connections Start  
  def getEnvironmentName(environment: String): (String, String, String) = {
      val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "environment.conf"))
      //val parsedFile = ConfigFactory.parseFile(new File("/Users/goyalpushkar/opt/cassandra/applicationstack/applications/wip/resources/environment.conf"))
      val conf = ConfigFactory.load(parsedFile)
      val oracleEnvName  = conf.getString("environment." + environment + ".oracleEnv")
      val cassandraEnvName  = conf.getString("environment." + environment + ".cassandraEnv")
      val neo4JEnvName  = conf.getString("environment." + environment + ".neo4JEnv")
      logger.debug("oracleEnvName - " + oracleEnvName + "\n" + "cassandraEnvName - " + cassandraEnvName + "\n" + "neo4JEnvName - " + neo4JEnvName)
      
      oracleEnvironment = oracleEnvName 
      neo4JEnvironment = neo4JEnvName
      cassandraEnvironment = cassandraEnvName
      (oracleEnvName, cassandraEnvName, neo4JEnvName)
  }
  
   def getOracleConnectionProperties(): (String, String, String) = {
       val parsedFile = new File( confFilePath + "source_connection_2.properties")
       //val parsedFile = new File("/Users/goyalpushkar/opt/cassandra/applicationstack/applications/wip/resources/source_connection_2.properties")
       //val (oracleEnv, cassandraEnv) = getEnvironmentName(environment)
       
       val OracleFileInput = new FileInputStream(parsedFile);
       val properties = new Properties();
			 properties.load(OracleFileInput);
			 val connectionString = properties.getProperty(oracleEnvironment);
				
			 logger.info("connectionString - " + connectionString);
				
			 val OracleConnection = connectionString.split(",").apply(0)
			 val OracleUserName = connectionString.split(",").apply(1)
			 val OraclePassword = connectionString.split(",").apply(2)
			 OracleFileInput.close();
       
       logger.debug("OracleConnection - " + OracleConnection + "\n" + "OracleUserName - " + OracleUserName + "\n" + "OraclePassword - " + OraclePassword )
      
       (OracleConnection, OracleUserName, OraclePassword)
   }
   
   def getCassandraConnectionProperties(): (String, String, String) = { //environment: String
       val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "cassandra_properties.conf"))
       //val parsedFile = ConfigFactory.parseFile(new File("/Users/goyalpushkar/opt/cassandra/applicationstack/applications/wip/resources/cassandra_properties.conf"))
       val conf = ConfigFactory.load(parsedFile)
       //val (oracleEnv, cassandraEnvironment, neo4JEnvName) = getEnvironmentName(environment)
       
       val connectionHost = conf.getString("cassandra." + cassandraEnvironment + ".host")
       val userName = conf.getString("cassandra." + cassandraEnvironment + ".username")
       val password = conf.getString("cassandra." + cassandraEnvironment + ".password")
       
       logger.debug("connectionHost - " + connectionHost + "\n" + "userName - " + userName + "\n" + "password - " + password )
      
       (connectionHost, userName, password)
   }
   
   def getNeo4JConnectionProperties(): (String, String, String) = {  //(environment: String)
       val parsedFile = ConfigFactory.parseFile(new File( confFilePath + "Neo4JConnections.conf"))
       //val parsedFile = ConfigFactory.parseFile(new File("/Users/goyalpushkar/opt/cassandra/applicationstack/applications/wip/resources/Neo4JConnections.conf"))
       //val (oracleEnv, cassandraEnv, neo4JEnvironment) = getEnvironmentName(environment)
       
       val conf = ConfigFactory.load(parsedFile)
       val Neo4JdbPath = conf.getString("neo4j." + neo4JEnvironment + ".db_path")
       val Neo4JUserName = conf.getString("neo4j." + neo4JEnvironment + ".username")
       val Neo4JPassword = conf.getString("neo4j." + neo4JEnvironment + ".password")
       
       logger.debug("Neo4JdbPath - " + Neo4JdbPath + "\n" + "Neo4JUserName - " + Neo4JUserName + "\n" + "Neo4JPassword - " + Neo4JPassword )
       
       (Neo4JdbPath, Neo4JUserName, Neo4JPassword)
   }
   
  def sparkContextCassandra(appName: String): ( SparkConf, SparkContext ) = {  //
     //logger.info( " - environment - " + environment )
     val (hostName, userName, password) = getCassandraConnectionProperties()
         
     val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      //.set("spark.cassandra.output.ignoreNulls","true")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.eventLog.enabled", "true" )
      .set("spark.cassandra.connection.host",hostName)
      .set("spark.cassandra.auth.username", userName)
      .set("spark.cassandra.auth.password", password)
      .set("spark.ui.killEnabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      //.set("spark.ui.port", "7080")   // Required for Mac
      .setMaster("local[*]") // This is required for IDE run
     
     val sc = new SparkContext(conf)
    
     ( conf, sc )
  }  

  def sparkContextNeo4J( appName: String): ( SparkConf, SparkContext ) = {  //environment: String,
     //logger.info( " - environment - " + environment )
     val (hostName, userName, password) = getNeo4JConnectionProperties()
         
     val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
      //.set("spark.cassandra.output.ignoreNulls","true")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.eventLog.enabled", "true" )
      .set("spark.neo4J.bolt.url",hostName)
      .set("spark.noe4J.bolt.user", userName)
      .set("spark.noe4J.bolt.password", password)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.ui.killEnabled", "true")
      //.set("spark.ui.port", "7080")   // Required for Mac      
      .setMaster("local[*]") // This is required for IDE run
     
     val sc = new SparkContext(conf)
    
     ( conf, sc )
  }    
   
   def createOracleConnection(): Connection = {
       val (connection, username, password) = getOracleConnectionProperties()
       val connectionPool = new BasicDataSource()
       try {
              // make the connection
              Class.forName("oracle.jdbc.driver.OracleDriver")
              DriverManager.setLoginTimeout(60);
              oracleConnection = DriverManager.getConnection(connection, username, password)

          } catch {
               case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing createOracleConnection at : " +  e.getMessage + " "  +  e.printStackTrace() )
    
          } 
       oracleConnection
   }   
   
   def createNeo4JConnection(): Session = {   //environment: String
       logger.info( " - environment - " + environment )
       val (hostName, userName, password) = getNeo4JConnectionProperties()
       try {
              // make the connection
              val neo4JDriver = GraphDatabase.driver(hostName, AuthTokens.basic(userName, password))
              neo4JSession = neo4JDriver.session()

          } catch {
               case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing createOracleConnection at : " +  e.getMessage + " "  +  e.printStackTrace() )
    
          } 
       neo4JSession
       //implicit val wsclient = ning.NingWSClient()
       //implicit val connection2 = Neo4jREST.setServer(hostName, 7687, "bolt://127.0.0.1:7687", userName, password)
       //("localhost", 7474, "username", "password")
   }  
   
   object OracleDataSource {
       val OracleConnectionPool = new BasicDataSource()
       val (connection, username, password) = getOracleConnectionProperties()
       try {
              // make the connection
              OracleConnectionPool.setDriverClassName("oracle.jdbc.driver.OracleDriver")
              OracleConnectionPool.setUrl(connection)
              OracleConnectionPool.setUsername(username)
              OracleConnectionPool.setPassword(password)
              OracleConnectionPool.setInitialSize(5)

          } catch {
               case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing OracleDataSource at : " +  e.getMessage + " "  +  e.printStackTrace() )
    
          } 
   } 
   
   def closeOracleConnection() = {
       OracleDataSource.OracleConnectionPool.close()     
   }
      
   object Neo4JDataSource {
       val (hostName, userName, password) = getNeo4JConnectionProperties()
       val Neo4JConnectionPool = new BasicDataSource()
       try {
              // make the connection
              //val neo4JDriver = GraphDatabase.driver(hostName, AuthTokens.basic(userName, password))
              //neo4JSession = neo4JDriver.session()
              //Neo4JConnectionPool.setDriverClassName("oracle.jdbc.driver.OracleDriver")
              Neo4JConnectionPool.setUrl(hostName)
              Neo4JConnectionPool.setUsername(userName)
              Neo4JConnectionPool.setPassword(password)
              Neo4JConnectionPool.setInitialSize(5)

          } catch {
               case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
                logger.error("Error while executing Neo4JDataSource at : " +  e.getMessage + " "  +  e.printStackTrace() )
    
          } 
   }  
   // End Connections
   
   case class returnResult(msg: String, data: String, result: java.sql.ResultSet)
   /*case class resultSetColumns( columnName: String, columnType: Int, columnData: String )
   
   case class streamedData( config_id: Int, identification_name: String, source_system: String, process_type: String
                           ,process_query: String, node_relations: Reader, constraint_type: String, constraint_action: String
                           ,constraint_properties: Reader, last_run_timestamp: Timestamp, error_msg: String
                           ,resultData: MArrayBuffer[resultSetColumns], recordNumber: Int
                          )*/  
                          
   case class relationShips( sourceNodeName: String, sourceNodeAttributes: MMap[String, String]
                            ,destNodeName: String, destNodeAttributes: MMap[String, String]
                            ,relationName: String, relationAttributes: MMap[String, String]
                            ,destinationRelationType: String
                            ,sourceNodeKey: MMap[String, String]
                            ,destNodeKey: MMap[String, String]
                            ,relationKey: MMap[String, String]
                           )  
     
   
   // Query Processing Start
   def runQuery(Query: String, parameters: MMap[String, String]): returnResult = {
  		logger.info( "------------Inside runQuery------------------");
  		var returnResultSet: java.sql.ResultSet = null;
  	  var errorMsg: String = null;
  		logger.info( " - Query - " + Query);
  		try {
    		    if ( oracleConnection == null ){
      				oracleConnection = OracleDataSource.OracleConnectionPool.getConnection();       //getInstance().getOracleConneciton();
      			}
      			//createOracleConnection();
      		  oracleConnection = OracleDataSource.OracleConnectionPool.getConnection()
      		  val executionQuery = oracleConnection.prepareStatement(Query)
      		  //returnResultSet = .executeQuery();			
      		  
      		  parameters.map(f => 
      		                        //val parameterType = f._2.split(parameterValueSplitter).apply(0)
      		                        //val parameterValue = f._2.split(parameterValueSplitter)[1]
      		                        if ( f._2.split(parameterValueSplitter).apply(0).equalsIgnoreCase(TimestampType) ){
                          					executionQuery.setTimestamp( Integer.parseInt(f._1), convertStringTimestamp( f._2.split(parameterValueSplitter).apply(1) ) );				
                          				} 
      		                        else if ( f._2.split(parameterValueSplitter).apply(0).equalsIgnoreCase(DateType) ){
                          					executionQuery.setDate( Integer.parseInt(f._1), convertStringDate( f._2.split(parameterValueSplitter).apply(1)  ) );				
                          				}
                          				else if ( f._2.split(parameterValueSplitter).apply(0).equalsIgnoreCase(StringType) ){
                          					executionQuery.setString( Integer.parseInt(f._1), f._2.split(parameterValueSplitter).apply(1)  );				
                          				} 
                          				else if ( f._2.split(parameterValueSplitter).apply(0).equalsIgnoreCase(IntegerType) ){
                          					executionQuery.setInt( Integer.parseInt(f._1), Integer.parseInt( f._2.split(parameterValueSplitter).apply(1)  ) );				
                          				} 
                          				else if ( f._2.split(parameterValueSplitter).apply(0).equalsIgnoreCase(LongType) ){
                          					executionQuery.setLong( Integer.parseInt(f._1), f._2.split(parameterValueSplitter).apply(1).toLong );				
                          				}
                          				else if ( f._2.split(parameterValueSplitter).apply(0).equalsIgnoreCase(BooleanType) ){
                          					executionQuery.setBoolean( Integer.parseInt(f._1), f._2.split(parameterValueSplitter).apply(1).toBoolean );				
                          				}
      		                   )
      		  executionQuery.setQueryTimeout(60);
    		  	//logger.info("Final executionQuery - " + executionQuery);
    			 returnResultSet = executionQuery.executeQuery();
  		  //OracleDataSource.OracleConnectionPool.
  		} catch{
  		    case e @ (_ :Exception | _ :Error | _ :java.sql.SQLException ) =>
        			errorMsg = e.getMessage();
        			logger.error("Run Query Error - " + e.getMessage());
        			e.printStackTrace();
  		} 
  		
  		val returnStatus = returnResult.apply(errorMsg, Query, returnResultSet)
  		/*logger.info( "Error Msg - " + errorMsg + "\n"
	               + "Query - " + Query + "\n"
	               + "Result Set Size - " + returnResultSet.getFetchSize + "\n"
	              )*/
  		
  		returnStatus
	} 
	
  def executeQuery(source: String, query: String, parameters: MMap[String, String] ): returnResult = {
		logger.info( "------------Inside executeQuery------------------");
		var returnStatus: returnResult = null;
		if (source.equalsIgnoreCase("ORACLE") ){
		  returnStatus = runQuery(query, parameters);
		}else if( source.equalsIgnoreCase("CASSANDRA") ){
			//
		}
		returnStatus
	} 
  // Query Processing End
  
	def getConfigurtionList(): java.sql.ResultSet = {
	  logger.info( "------------Inside getConfigurtionList------------------");
	  
	   val queryParameters = MMap[String, String]();
  	 val setupQuery: String = s"""SELECT identification_name, process_type, process_query, node_relations 
	    		                              ,constraint_type, constraint_action, constraint_properties
                                        ,config_id, source_system_name, last_run_timestamp, parameter_list
	                                  FROM aud_neo4j_configurations 
	    		                         WHERE NVL( active, 'Y' ) = 'Y' 
	                                   AND ( ( SYSDATE - last_run_timestamp ) * ( 24 * 3600 ) ) > run_frequency_in_sec
                                     AND identification_name = 'MEETING_ISSUES'  --'PROJECTS'
	    		                      ORDER BY config_id 
                               """
	                      ;
	    val returnStatus = executeQuery("ORACLE",setupQuery, queryParameters);
	    
	    /*logger.info( "Return Error Msg - " + returnStatus.msg + "\n"
	               + "Return Query - " + returnStatus.data + "\n"
	               + "Return Result Set Size - " + returnStatus.result.getFetchSize + "\n"
	              )*/
	    returnStatus.result
	    
	}
	
	// Neo4J Processing Start
	def checkNodeExistence(NodeName: String ,attributes: MMap[String, String] ): Int = {
		logger.info("-------------------checkNodeExistence---------------");
		try{ 
			 var query: String = "";
			 var attributeSize: Int = 0;
			
  			query = query.concat("MATCH ( w:"+  NodeName);
  			//Add Destination Node Attributes
  			if (attributes.size > 0) {
  				query = query.concat(" {");
  			}
  			attributes.map { x => if ( attributeSize > 0 ) query = query.concat(",") 
			                      query = query.concat( x._1 + ":'" + x._2 + "'" );
			                      attributeSize = attributeSize + 1; 
			                }
  			if (attributes.size > 0) {
  				query = query.concat("} ");
  			}
  			query = query.concat(" ) RETURN w");
  			//End Destination Node Attributes
  			
  			logger.info(" - Adhoc query - " + query );
  			val result: StatementResult = neo4JSession.run(query);
  			logger.info("Adhoc Query ran");
  			if ( result.hasNext() ) 1;
  			else  0;			
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Adhoc Query Error - " + e.getMessage());
  			return 0;
		}
	}	
	
	def  createIndex(action: String, Node: String, properties: List[String] ): returnResult  = {
		logger.info("-------------createIndex--------------");
		//var returnData: returnResult = new returnResult(null, null, null);
		var query: String = "" ;
		var size: Int = 0;
		try{
		  // Added on 02/01/2018
			var actionTaken = "";
			var property = "";
			
			if ( action.equalsIgnoreCase("CREATE")) {
				query = query.concat("CREATE ");
				actionTaken = "created";
			} else if (action.equalsIgnoreCase("DROP")) {
				query = query.concat("DROP ");
				actionTaken = "dropped";
			}
			//query = query.concat("CREATE INDEX ON :" + Node );
			// Ended on 02/01/2018
			
			if (properties.size > 0) {
				property = property.concat(" (");
			}
			properties.map { x => if ( size > 0 ) property = property.concat(",") 
			                      property = property.concat( x ); 
			                      size = size + 1; 
			                }
			if (properties.size > 0) {
				property = property.concat(")");
			}
	    
			query = query.concat(" INDEX ON :" + Node + property );	
			logger.info(" - Create Index query - " + query);
			neo4JSession.run( query );
			logger.info("Neo4J Index Created");
		  
			val returnData: returnResult = new returnResult(null, query, null);
      returnData
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Index Creation Error - " + e.getMessage() );
  			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
  			returnData
		}
	}
	
	def  createUniqueConstraint(action: String, Node: String, properties: List[String]  ): returnResult = {
		logger.info("-------------createUniqueConstraint--------------");
		//returnResult returnData = new returnResult(null, null, null);
		var query: String = "" ;	
		var size: Int = 0
		var actionTaken: String = ""
		var property: String = ""	
		try{
			
		  //query = if ( action.equalsIgnoreCase("CREATE") ) query.concat("CREATE ") else ( action.equalsIgnoreCase("DROP") ) query.concat("DROP ") 
		     
			if ( action.equalsIgnoreCase("CREATE")) {
				query = query.concat("CREATE ");
				actionTaken = "created";
			} else if (action.equalsIgnoreCase("DROP")) {
				query = query.concat("DROP ");
				actionTaken = "dropped";
			}
			
			if (properties.size > 0) {
				property = property.concat(" (");
			}
			properties.map { x => if ( size > 0 ) property = property.concat(","); 
			                      property = property.concat( "c." + x );   //+ "'"  Removed on 02/01/2018
			                      size = size + 1; 
			                }
			if (properties.size > 0) {
				property = property.concat(")");
			}
			
			query = query.concat(" CONSTRAINT ON (c:" + Node + ") ASSERT " + property + " IS UNIQUE" );
				
			logger.info(" - Create Unique Constraint query - " + query);
			neo4JSession.run( query );
			logger.info("Neo4J Unique Constraint " + actionTaken);
		  val returnData: returnResult = new returnResult(null, query, null);
      returnData			
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Unique Constraint Error - " + e.getMessage() );
  			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
  			returnData
		}
	}
	
	def propertyExistenceConstraint(action: String, Node: String, properties: List[String] ): returnResult = {
		logger.info("-------------propertyExistenceConstraint--------------");
		var query: String = "" ;
		var size: Int = 0;
		var actionTaken: String = "";
		var property: String = "";
		try{

			if ( action.equalsIgnoreCase("CREATE")) {
				query = query.concat("CREATE ");
				actionTaken = "created";
			} else if (action.equalsIgnoreCase("CREATE")) {
				query = query.concat("DROP ");
				actionTaken = "dropped";
			}
			
			if (properties.size > 0) {
				property = property.concat(" (");
			}
			properties.map { x => if ( size > 0 ) property = property.concat(","); 
			                      property = property.concat( "c." + x );   //+ "'"   Removed on 02/01/2018
			                      size = size + 1;   
			                }
			if (properties.size > 0) {
				property = property.concat(")");
			}
			
			query = query.concat(" CONSTRAINT ON (c:" + Node + ") ASSERT exists " + property );
				
			logger.info(" - Property Existence Constraint query - " + query);
			neo4JSession.run( query );
			logger.info("Neo4J Property Existence Constraint " + actionTaken);
			
		  val returnData: returnResult = new returnResult(null, query, null);
      returnData			
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Property Existence Constraint Error - " + e.getMessage() );
  			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
  			returnData
		}
	}
	
	def  nodeKeyConstraint(action: String, Node:String, properties: List[String]): returnResult = {
		logger.info("-------------nodeKeyConstraint--------------");
		var query: String = "" ;
		var size: Int = 0;
		var actionTaken: String = "";
		
		try{
			
			if ( action.equalsIgnoreCase("CREATE")) {
				query = query.concat("CREATE ");
				actionTaken = "created";
			} else if (action.equalsIgnoreCase("CREATE")) {
				query = query.concat("DROP ");
				actionTaken = "dropped";
			}
			query = query.concat(" CONSTRAINT ON (c:" + Node + ") ASSERT" );
			
			if (properties.size > 0) {
				query = query.concat(" (");
			}
			properties.map { x => if ( size > 0 ) query = query.concat(",") 
			                      query = query.concat( "c." + x );  // "'" + "'" Removed on 02/01/2018
			                      size = size + 1; 
			                }
			if (properties.size > 0) {
				query = query.concat(")");
			}
			query = query.concat(" IS NODE KEY");
			
			logger.info(" - Node Key Constraint query - " + query);
			neo4JSession.run( query );
			logger.info("Neo4J Node Key Constraint " + actionTaken);
			
		  val returnData: returnResult = new returnResult(null, query, null);
      returnData			
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Node Key Constraint Error - " + e.getMessage() );
  			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
  			returnData
		}
	}
	
	def createConstraint( constraintType: String, action: String, Node: String, properties: List[String] ): returnResult = {
		logger.info("-------------createConstraint--------------");
		var returnData: returnResult = null
		if ( constraintType.equalsIgnoreCase("INDEX") ){
		  returnData = createIndex(action, Node, properties);
		}else if ( constraintType.equalsIgnoreCase("UNIQUE_CONSTRAINT") ){
		  returnData = createUniqueConstraint(action, Node, properties);
		}else if ( constraintType.equalsIgnoreCase("PROPERTY_EXISTENCE") ){
			 returnData = propertyExistenceConstraint(action, Node, properties);
		}else if ( constraintType.equalsIgnoreCase("NODE_KEY") ){
			 returnData = nodeKeyConstraint(action, Node, properties);
		}
		return returnData;
	}
	
	def createNodesDynamic( NodeName: String, attributes: MMap[String, String], sourceNodeKey: Map[String, String]  ): returnResult = {
		logger.info("-------------createNodesDynamic--------------");
	  val errorMsg: String = null;
		var query: String = "" ;
		var size: Int = 0;
		try{ 
		  var sourceNodeKeySize = 0;
			var properties = "";
			query = query.concat("MERGE ( w:" + NodeName );
			
			// Added on 02/01/2018
			if (sourceNodeKey.size > 0) {
				query = query.concat(" {");
				//Added on 02/01/2018
				sourceNodeKey.foreach{x =>  if ( sourceNodeKeySize > 0 ){ query = query.concat(",") } 
				                        
				                        query = query.concat( x._1 + ":'" + x._2 + "'" );  	
				                        sourceNodeKeySize = sourceNodeKeySize + 1;
			                      	}
				query = query.concat("} ) ");
				//Ended on 02/01/2018 
			}else{
				query = query.concat(" ) ");
			}
			// Ended on 02/01/2018			
			
			attributes.map { x => if ( size > 0 ) properties = properties.concat(",") 
			                      properties = properties.concat( x._1 + ":'" + x._2 + "'");
			                      size = size + 1; 
			                }
			if (attributes.size > 0) {
				  //query = query.concat("}");
			    query = query.concat( " ON CREATE SET " + properties + " ON MATCH SET " + properties );
			}
			
			//query = query.concat(" )");
			logger.info(" - Create Node query - " + query);
			neo4JSession.run( query );
			logger.info("Neo4J Node Created");
			
			/*StatementResult result = neo4JSession.run( "MATCH (w:Warehouse) WHERE w.name = {name} " +
					   "RETURN w.name AS name, w.desc AS desc"
                       ,parameters( "name", attributeValue ) );
			
			logger.debug("Get Neo4j Node Data");
			while ( result.hasNext() )
			{
			    Record record = result.next();
			    logger.debug( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
			}*/
		  val returnData: returnResult = new returnResult(null, query, null);
      returnData
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Node Creation Error - " + e.getMessage() );
  			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
  			returnData
		}
		
	}
	
	def createRelationshipDynamic( destinationNodeName: String
                                ,destinationNodeAttributes: MMap[String, String]
                                ,destRelationShipType: String
                                ,RelationShipName: String		                                    
                                ,relationshipAttributes: MMap[String, String] 
                                ,sourceNodeName: String
                                ,sourceNodeAttributes: MMap[String, String]
                                ,sourceRelationShipType: String 
                                ,sourceNodeKey: MMap[String, String]
								                ,destNodeKey: MMap[String, String]
								                ,relationKey: MMap[String, String]
                                ): returnResult = {  //: returnResult 
		logger.info("------------createRelationshipDynamic-------------");
		var errorMsg: String = null;
		var query: String = "" ;
		var sourceNodeSize: Int = 0;
		var sourceNodeKeySize = 0;
		var destNodeSize: Int = 0;
		var destNodeKeySize: Int = 0
		var relationShipSize: Int = 0;
	  // Added on 02/01/2018
		var sourceProperties = "";   
		var destinationProperties = "";
		// Ended on 02/01/2018		
		try{ 
			
			/*logger.info("Create Neo4J Relation - " + "\n"
				+ " - SourceNodeName - " + sourceNodeName + "\n"
				+ " - destinationNodeName - " + destinationNodeName + "\n"
				+ " - RelationShipName - " + RelationShipName + "\n"
				+ " - sourceRelationShipType - " + sourceRelationShipType + "\n"
				+ " - destRelationShipType - " + destRelationShipType + "\n"
				+ " - sourceNodeAttributes.size() - " + sourceNodeAttributes.size() + "\n"
				+ " - destinationNodeAttributes.size() - " + destinationNodeAttributes.size() + "\n"
				+ " - relationshipAttributes.size() - " + relationshipAttributes.size() + "\n"
				);*/
			
			/*int checkExistence = checkNodeExistence(sourceNodeName
									          ,sourceNodeAttributes
									           );
			logger.info("checkExistence - " + checkExistence);
			
			if ( checkExistence == 0 ){
				query = "";
				createNodesDynamic(sourceNodeName, sourceNodeAttributes);
			}*/
			
			query = query.concat("MERGE (s:" + sourceNodeName );
			//Add Source Node Existence Attributes
			if (sourceNodeAttributes.size > 0) {
				query = query.concat(" {");
			}
			
			//Add Source Node Existence Attributes
			if (sourceNodeKey.size > 0) {
				query = query.concat(" {");
				//Added on 02/01/2018
				sourceNodeKey.foreach{f => if ( sourceNodeKeySize > 0 ){ query = query.concat(",");} 
				                           query = query.concat( f._1 + ":'" + f._2 + "'" );  	
				                           sourceNodeKeySize = sourceNodeKeySize + 1;
				                      }
				query = query.concat("} ) ");
				//Ended on 02/01/2018 
			}else{
				query = query.concat(" ) ");
			}
			
			sourceNodeAttributes.map { x => if ( sourceNodeSize > 0 ) sourceProperties = sourceProperties.concat(",") 
			                      sourceProperties = sourceProperties.concat( x._1 + ":'" + x._2 + "'");
			                      sourceNodeSize = sourceNodeSize + 1; 
			                }
     
			if (sourceNodeAttributes.size > 0) {
			   query = query.concat( " ON CREATE SET " + sourceProperties + " ON MATCH SET " + sourceProperties );
			}
			
		  query = query.concat("MERGE (d:" + destinationNodeName );
		  //Add Destination Node Attributes
			if (destinationNodeAttributes.size > 0) {
				query = query.concat(" {");
			}
			
			//Add Dest Node Existence Attributes
			if (destNodeKey.size > 0) {
				query = query.concat(" {");
				//Added on 02/01/2018
				destNodeKey.foreach{f => if ( destNodeKeySize > 0 ){ query = query.concat(",");} 
				                           query = query.concat( f._1 + ":'" + f._2 + "'" );  	
				                           destNodeKeySize = destNodeKeySize + 1;
				                      }
				query = query.concat("} ) ");
				//Ended on 02/01/2018 
			}else{
				query = query.concat(" ) ");
			}
			
			destinationNodeAttributes.map { x => if ( destNodeSize > 0 ) destinationProperties = destinationProperties.concat(",") 
			                      destinationProperties = destinationProperties.concat( x._1 + ":'" + x._2 + "'");
			                      destNodeSize = destNodeSize + 1; 
			                }
			if (destinationNodeAttributes.size > 0) {
			    query = query.concat( " ON CREATE SET " + destinationProperties + " ON MATCH SET " + destinationProperties );
			}
			
			query = query.concat(" MERGE (s) " );
			
			query = query.concat(  sourceRelationShipType + " [m:" + RelationShipName );
			
			//Add Relation Attributes
			if (relationshipAttributes.size > 0) {
				query = query.concat(" {");
			}
			relationshipAttributes.map { x => if ( relationShipSize > 0 ) query = query.concat(",") 
              			                      query = query.concat( x._1 + ":'" + x._2 + "'");
              			                      relationShipSize = relationShipSize + 1; 
              			                }
			/*for ( key <- relationshipAttributes.keySet ){
				if ( relationShipSize > 0 ){
					query = query.concat(" ,");
				}
				query = query.concat( key + ":'" + relationshipAttributes.get(key) + "'" );
				relationShipSize = relationShipSize + 1;
			}*/
			if (relationshipAttributes.size > 0) {
				query = query.concat("} ");
			}
			//Add Relation Attributes
			
			query = query.concat("] " + destRelationShipType + " (d)" );
			
			logger.info(" - query - " + query);
			neo4JSession.run( query );
			logger.info("Neo4J Relation Created");
			
			/*StatementResult result = neo4JSession.run( "MATCH (w:Warehouse) WHERE w.name = {name} " +
			"RETURN w.name AS name, w.desc AS desc"
			,parameters( "name", attributeValue ) );
			
			logger.debug("Get Neo4j Node Data");
			while ( result.hasNext() )
			{
			Record record = result.next();
			logger.debug( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
			}*/
		  val returnData: returnResult = new returnResult(null, query, null);
      returnData
		}
		catch {
  		  case e @ ( _:Neo4jException | _:Exception ) => 
  			logger.error("Node Relation Error - " + e.getMessage() );
  			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
  			returnData
		}
	}
	
	def deleteNodeDynamic( NodeName: String									      
									      ,attributes: MMap[String, String]
									            ) = {
		logger.debug("-------------deleteNodeDynamic--------------");
		var errorMsg: String = null;
		var returnData = new returnResult(null, null, null);
		var query = "" ;
		try{ 				
				var size = 0;
				/*logger.info(" - Create Neo4J Node - " + "\n"
				+ " - NodeName - " + NodeName + "\n"
				+ " - attributes.size() - " + attributes.size()
				);*/
				query = query.concat("MATCH ( w" );
				
				if ( NodeName != null ) {  //!(NodeName.equalsIgnoreCase("")) || 
					query = query.concat(":" + NodeName );
				}
								
				if (attributes.size > 0) {
					query = query.concat("{");
				}
				attributes.foreach{f => if ( size > 0 ){query = query.concat(",") }
				                        query = query.concat( f._1 + ":'" + f._2 + "'");
					                      size = size + 1;
				                 }
				if (attributes.size > 0) {
					query = query.concat("}");
				}
				
				query = query.concat(" ) DETACH DELETE w");
				
				logger.info(" - Delete Node query - " + query);
				neo4JSession.run( query );
				logger.debug("Neo4J Node Deleted");
				
				/*StatementResult result = neo4JSession.run( "MATCH (w:Warehouse) WHERE w.name = {name} " +
				"RETURN w.name AS name, w.desc AS desc"
				,parameters( "name", attributeValue ) );
				
				logger.debug("Get Neo4j Node Data");
				while ( result.hasNext() )
				{
				Record record = result.next();
				logger.debug( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
				}*/
				val returnData: returnResult = new returnResult(null, query, null);
				returnData
		}
		catch {
		  case e @ ( _:Neo4jException | _:Exception ) => 
		  logger.error("Node deletion Error - " + e.getMessage());
			errorMsg = "Node deletion Error - " + e.getMessage();
			val returnData: returnResult = new returnResult(e.getMessage(), query, null);
			returnData
		} 
	}	
	
	def deleteRelationshipDynamic( sourceNodeName: String
                                ,sourceNodeAttributes: MMap[String, String]
                                ,sourceRelationShipType: String
                                ,relationName: String		                                    
                                ,relationAttributes: MMap[String, String] 
                                ,destinationNodeName: String
                                ,destNodeAttributes: MMap[String, String]
                                ,destRelationShipType: String 
                                ): returnResult = {
		logger.debug("------------deleteRelationshipDynamic-------------");
		var errorMsg: String = "";
		var returnData = new returnResult(null, null, null);
		var query = "" ;
		try{ 
		
			var sourceNodeSize = 0;
			var destNodeSize = 0;
			var relationShipSize = 0;
			/*logger.info("Create Neo4J Relation - " + "\n"
			+ " - SourceNodeName - " + sourceNodeName + "\n"
			+ " - destinationNodeName - " + destinationNodeName + "\n"
			+ " - relationName - " + relationName + "\n"
			+ " - sourceRelationShipType - " + sourceRelationShipType + "\n"
			+ " - destRelationShipType - " + destRelationShipType + "\n"
			+ " - sourceNodeAttributes.size() - " + sourceNodeAttributes.size() + "\n"
			+ " - destNodeAttributes.size() - " + destNodeAttributes.size() + "\n"
			+ " - relationAttributes.size() - " + relationAttributes.size() + "\n"
			);*/
			
			/*int checkExistence = checkNodeExistence(sourceNodeName
			          ,sourceNodeAttributes
			           );
			logger.info("checkExistence - " + checkExistence);
			
			if ( checkExistence == 0 ){
			query = "";
			createNodesDynamic(sourceNodeName, sourceNodeAttributes);
			}*/
			
			query = query.concat("MATCH (s");
			
			if ( sourceNodeName != null ) {  //!(sourceNodeName.equalsIgnoreCase("")) ||
				query = query.concat(":" + sourceNodeName );
			}
			
			//Add Source Node Existence Attributes
			if (sourceNodeAttributes.size > 0) {
				query = query.concat(" {");
			}
			sourceNodeAttributes.foreach{ f => if ( sourceNodeSize > 0 ){ query = query.concat(" ,") }
                                  			query = query.concat( f._1 + ":'" + f._2 + "'" );
                                  			sourceNodeSize = sourceNodeSize + 1;
			}
			if (sourceNodeAttributes.size > 0) {
				query = query.concat("} ");
			}
			//End Source Node Existence Attributes
			
			query = query.concat(")" + sourceRelationShipType + " [m" );
			
			if ( relationName != null ) {  //!(relationName.equalsIgnoreCase("")) || 
				query = query.concat(":" + relationName );
			}
			
			//Add Relation Attributes
			if (relationAttributes.size > 0) {
				query = query.concat(" {");
			}
			relationAttributes.foreach{ f => if ( relationShipSize > 0 ){query = query.concat(" ,") }
				query = query.concat( f._1 + ":'" + f._2 + "'" );
				relationShipSize = relationShipSize + 1;
			}
			if (relationAttributes.size > 0) {
				query = query.concat("} ");
			}
			//Add Relation Attributes
			
			query = query.concat("] " + destRelationShipType + " (d" );
			
			if ( destinationNodeName != null ) {  //!(destinationNodeName.equalsIgnoreCase("")) ||
				query = query.concat(":" + destinationNodeName );
			}
						
			//Add Destination Node Attributes
			if (destNodeAttributes.size > 0) {
				query = query.concat(" {");
			}
			destNodeAttributes.foreach{ f => if ( destNodeSize > 0 ){query = query.concat(" ,")}
				                               query = query.concat( f._1 + ":'" + f._2 + "'" );
				                               destNodeSize = destNodeSize + 1;
				}
			if (destNodeAttributes.size > 0) {
				query = query.concat("} ");
			}
			query = query.concat(") DELETE m ");
			//End Destination Node Attributes
			
			logger.info(" - query - " + query);
			neo4JSession.run( query );
			logger.debug("Neo4J Relation Deleted");
			
			/*StatementResult result = neo4JSession.run( "MATCH (w:Warehouse) WHERE w.name = {name} " +
			"RETURN w.name AS name, w.desc AS desc"
			,parameters( "name", attributeValue ) );
			
			logger.debug("Get Neo4j Node Data");
			while ( result.hasNext() )
			{
			Record record = result.next();
			logger.debug( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
			}*/
			val returnData: returnResult = new returnResult(null, query, null);
				returnData
		}
		catch {
		  case e @ ( _:Neo4jException | _:Exception ) => 
		    logger.error("Node Relation Error - " + e.getMessage());
			  errorMsg = "Node Relation Error - " + e.getMessage()
		  	val returnData: returnResult = new returnResult(e.getMessage(), query, null);
			  returnData
		} 

	}
	
	def deleteNodesRelations( constraintType: String
	                        ,sourceNodeName: String
                          ,sourceNodeAttributes: MMap[String, String]
                          ,sourceRelationShipType: String
                          ,relationName: String		                                    
                          ,relationAttributes: MMap[String, String] 
                          ,destinationNodeName: String
                          ,destNodeAttributes: MMap[String, String]
                          ,destRelationShipType: String 
                          ): returnResult = {
		logger.debug("-------------deleteNodesRelations--------------");
		var errorMsg: String = "";
		var returnData = new returnResult(null, null, null);
		var query = "" ;		
		try{

			if ( constraintType.equalsIgnoreCase("DELETE_NODE") ){
				returnData = deleteNodeDynamic(sourceNodeName, sourceNodeAttributes);
			}else if ( constraintType.equalsIgnoreCase("DELETE_RELATION") ){
				returnData = deleteRelationshipDynamic(sourceNodeName
												      ,sourceNodeAttributes
												      ,sourceRelationShipType
												      ,relationName
												      ,relationAttributes
												      ,destinationNodeName
												      ,destNodeAttributes
												      ,destRelationShipType
											           );
			}else if ( constraintType.equalsIgnoreCase("DELETE_ALL") ){
				returnData = deleteNodeDynamic(sourceNodeName, sourceNodeAttributes);;
			}
			
		}
		catch {
		  case e @ ( _:Neo4jException | _:Exception ) => 
		    logger.error("Delete Node Relation Error - " + e.getMessage());
			  errorMsg = "Delete Node Relation Error - " + e.getMessage()
		  	returnData = returnResult(e.getMessage(), query, null);
		} 
				
		return returnData;
	}
	// Neo4J Processing End
   
}