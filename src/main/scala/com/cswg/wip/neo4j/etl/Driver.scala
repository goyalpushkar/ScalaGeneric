package com.cswg.wip.neo4j.etl


import org.slf4j.{LoggerFactory, MDC }
import org.apache.spark.streaming.{StreamingContext, Duration, Time, Seconds}

import org.apache.spark.ui._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
//import scala.concurrent.duration._

import org.json.simple.{JSONArray, JSONObject}
import org.json.simple.parser.{JSONParser, ParseException }
import java.util.ListIterator
import java.io.{ Reader, IOException }
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import scala.collection.mutable.{ArrayBuffer => MArrayBuffer}

class Driver {
  
}

object Driver {
  val logger = LoggerFactory.getLogger(this.getClass())   //"com.cswg.wip.neo4j.etl")
  val currentTimeToday = getCurrentTime()
  val currentTime = getCurrentTime() //getCurrentTime() //removeHours(currentTimeToday, -16) //getPreviousOpsDate(currentTimeToday, -1)    //getCurrentTime()
     
   // 0 -> Job Id
   // 1 -> Environment Name
   // 2 -> Logging Enabled -> Y or N
   // 3 -> Conf File Name
  
   // 1 -> Neo4J Environment Name
   // 2 -> Cassandra Environment Name
   // 3 -> Oracle Environment Name  
   // 3 -> Oracle Connection Name
  def main(args: Array[String]) {
    MDC.put("fileName","M5000_WCNeo4JStreaming_" + args(0).toString() )  
    
    logger.debug("Main for Neo4J Streaming")    
    println( ClassLoader.getSystemResource("logback.xml") )

     val jobId = if ( args(0) == null ) "1" else args(0).toString() 
     environment = if ( args(1) == null ) "dev" else args(1).toString
     loggingEnabled = if ( args(2) == null ) "N" else args(2).toString()
     confFilePath = if ( args(3) == null ) "/opt/cassandra/applicationstack/applications/wip/resources/" else args(3).toString()
     //neo4JEnvironment = if ( args(1) == null ) "dev" else args(1).toString 
     //cassandraEnvironment = if ( args(2) == null ) "dev" else args(2).toString      
     //oracleEnvironment = if ( args(3) == null ) "xxwip_csdwd" else args(3).toString()
     
     var jobStatus = "Started" 
     val streamingDuration = Seconds(5) ///Duration.apply(5000)
     
     logger.debug(  "\n"
                     + " - Job Id - " + jobId  + "\n"
                     + " - environment - " + environment + "\n"
                     + " - loggingEnabled - " + loggingEnabled + "\n"
                     //+ " - oracleEnvironment - " + oracleEnvironment + "\n"
                    );
    
    getEnvironmentName(environment)
    //val config = new SparkConf().setAppName("streaming").setMaster("local[*]")
    //val sc = new SparkContext(config)
    val ( conf, sc )  = sparkContextCassandra("M5000_WCNeo4JStreaming_" + args(0).toString())    // environment
                        //sparkContextNeo4J( "M5000_WCNeo4JStreaming_" + args(0).toString())
    neo4JSession = createNeo4JConnection() ///Neo4JDataSource.Neo4JConnectionPool.getConnection()
    /*//val (hostName, userName, password) = getCassandraConnectionProperties()
     val conf = new SparkConf()
                    .setAppName("M5000_WCNeo4JStreaming_" + args(0).toString())
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
                    //.set("spark.cassandra.output.ignoreNulls","true")
                    //.set("spark.driver.allowMultipleContexts","true")
                    //.set("spark.eventLog.enabled", "true" )
                    .set("spark.cassandra.connection.host",hostName)
                    .set("spark.cassandra.auth.username", userName)
                    .set("spark.cassandra.auth.password", password)
                    .set("spark.ui.port", "7080")   // Required for Mac
                    .setMaster("local[*]") // This is required for IDE run
    val sc = new SparkContext(conf)
    */
    
    val ssc = new StreamingContext(sc, streamingDuration )  //Seconds(5)
    //val ssc = new StreamingContext(conf, streamingDuration )
    
    val hiveContext: SQLContext = new HiveContext(sc)
    logger.info("Spark Streaming is defined" + "\n"
               + "spark driver host - " + sc.getConf.get("spark.driver.host") + "\n"
               + "spark driver port - " + sc.getConf.get("spark.driver.port") + "\n"
               //+  "spark ui port - " + sc.getConf.get("spark.ui.port")
                )
                     
    import hiveContext.implicits._
    
    sys.ShutdownHookThread {
              logger.info( " - Gracefully stopping Spark Streaming for Neo4J - ")
              ssc.stop(true, true)
              closeOracleConnection()
              logger.info( " - Application stopped - ")
    }
    
    try{
          /*val dfJobControl =  hiveContext.read
                               .format("org.apache.spark.sql.cassandra")
                               .options( Map( "table" -> "rcv_job_control"
                                            , "keyspace" -> "wip_rcv_ing") )
                               .load()
                               .filter(s"job_id = '$jobId' AND enabled = 'Y' ")  
          
          dfJobControl.show()
          logger.debug( "Job Control")*/
          
          //val congifList = getConfigurtionList()
          /*while(congifList.next()){
               logger.info( "-- Setup Details -- " + "\n" 
                        				               + "      Identification    " + congifList.getString(1) + "\n"
                        				               + "      Process Type      " + congifList.getString(2) + "\n"
                        				               + "      Process Query     " + congifList.getString(3) + "\n"
                        				               //+ "      Node Relations    " + x.node_relations + "\n"
                        				               + "      Constraint Type    " + congifList.getString(5) + "\n"
                        				               + "      Constraint Action    " + congifList.getString(6) + "\n"
                        				               //+ "      Constraint     " + x.constraint_properties + "\n"
                        				               + "      Id    " + congifList.getString(8) + "\n"
                        				               + "      Source System    " + congifList.getString(9) + "\n"
							                               );
            
          }*/
          val processReceiveStream = ssc.receiverStream( new StreamReceiver()  )  //congifList
          //logger.debug(" - processReceiveStream.size - " + processReceiveStream.count())
          //println(" - processReceiveStream.size - " + processReceiveStream.count())
          //val streamRecevierV = new StreamReceiver(congifList)
          //streamRecevierV.onStart()
          //val consumedData = new StreamConsumer()
          //consumedData.consumer(processReceiveStream)
          //consumedData.processJsonString()
          //processReceiveStream.foreachRDD( partitionedRDD => partitionedRDD.foreach { x => consumedData.processRDD(x) } )
          //processReceiveStream.foreachRDD( partitionedRDD => consumedData.consumeRDD(partitionedRDD) )
          
          //processReceiveStream.foreachRDD( partitionedRDD => partitionedRDD.foreach { x => println("Print Stream - " + x.identification_name  ) }  )
          processReceiveStream.foreachRDD( partitionedRDD => 
                                              partitionedRDD.foreach { streamed => 
                                                                         println( "-- Processed Details -- " + "\n" 
                                                              				               + "      Identification    " + streamed.identification_name + "\n"
                                                              				               + "      Process Type      " + streamed.process_type + "\n"
                                                              				               + "      Process Query     " + streamed.process_query + "\n"
                                                              				               //+ "      Node Relations    " + x.node_relations + "\n"
                                                              				               + "      Constraint Type    " + streamed.constraint_type + "\n"
                                                              				               + "      Constraint Action    " + streamed.constraint_action + "\n"
                                                              				               //+ "      Constraint     " + x.constraint_properties + "\n"
                                                              				               + "      Id    " + streamed.config_id + "\n"
                                                              				               + "      Source System    " + streamed.source_system + "\n"
                                                              				               + "      Error Message    " + streamed.error_msg + "\n"
                                                              				               + "      Result data size " + streamed.resultData.size + "\n"
                                      							                               )
                                                                          }
                                           )
                                            
                                                                          
          /*processReceiveStream.foreachRDD( partitionedRDD => 
                                              partitionedRDD.foreach { ArrayBuf => 
                                                                          ArrayBuf.foreach { streamed => 
                                                                                                       println( "-- Processed Details -- " + "\n" 
                                                                                            				               + "      Identification    " + streamed.identification_name + "\n"
                                                                                            				               + "      Process Type      " + streamed.process_type + "\n"
                                                                                            				               + "      Process Query     " + streamed.process_query + "\n"
                                                                                            				               //+ "      Node Relations    " + x.node_relations + "\n"
                                                                                            				               + "      Constraint Type    " + streamed.constraint_type + "\n"
                                                                                            				               + "      Constraint Action    " + streamed.constraint_action + "\n"
                                                                                            				               //+ "      Constraint     " + x.constraint_properties + "\n"
                                                                                            				               + "      Id    " + streamed.config_id + "\n"
                                                                                            				               + "      Source System    " + streamed.source_system + "\n"
                                                                                            				               + "      Error Message    " + streamed.error_msg + "\n"
                                                                                            				               + "      Result data size " + streamed.resultData.size + "\n"
                                                                    							                               );
                                                                          }
                                              }
                                        )*/
                                        
          //val streamR = new StreamReceiver(congifList)
          //streamR.onStart()
                                                  
          ssc.start()
          ssc.awaitTermination()
          //ssc.awaitTerminationOrTimeout(3000)
    }
    catch{
       case e @ (_ :Exception | _ :Error | _ :java.io.IOException  ) =>
          logger.error("Error while executing Main at : " +  e.getMessage + " " + e.printStackTrace() )
          ssc.stop(true, true)
          closeOracleConnection()
          jobStatus = "Error"
     }
    //congifList.
    //val processReceiveStream = ssc.receiverStream(new StreamReceiver(congifList))
    //val streamReceiver = new StreamReceiver(congifList)
    //streamReceiver.receive(congifList)
    //sc.stop()
  }  // main
  


}  // object Driver


/*
while ( congifList.next() ){ 
        				 logger.info( "-- Setup Details -- " + "\n" 
      				               + "      Identification    " + congifList.getString(1) + "\n"
      				               + "      Process Type      " + congifList.getString(2) + "\n"
      				               + "      Process Query     " + congifList.getString(3) + "\n"
      				               //+ "      Node Relations    " + congifList.getMetaData(4) + "\n"
      				               + "      Constraint Type    " + congifList.getString(5) + "\n"
      				               + "      Constraint Action    " + congifList.getString(6) + "\n"
      				               //+ "      Constraint Properties    " + congifList.getClob(7) + "\n"
      				               + "      Config Id        " + congifList.getInt(8) + "\n"
      				               + "      Source System    " + congifList.getString(9) + "\n"
      							   );
                 val id = congifList.getInt(8)
        				 val sourceSytem = congifList.getString(9)
        				 val processType = congifList.getString(2)
        				 val query = congifList.getString(3)
        				 
        				 if( processType.equalsIgnoreCase("QUERY") ) {
        						val rdrs: Reader = congifList.getClob(4).getCharacterStream();
        						val parser = new JSONParser()
        						var nodeJSONObject: JSONObject= null;
        						var nodeJSONArray: JSONArray = null; 
        						//logger.debug( " rdrs - " +  rdrs )
        						try {
        						    nodeJSONObject = parser.parse(rdrs).asInstanceOf[JSONObject]
        						    nodeJSONArray = nodeJSONObject.get("Nodes").asInstanceOf[JSONArray]
        						    //nodeJSONArray.forEach(  )
        							  val processReceiveStream = ssc.receiverStream(new StreamReceiver(congifList, sourceSytem, query))
        							  //processReceiveStream.print()
        							  //processReceiveStream.foreachRDD( rddEach => rddEach.foreach { x => logger.debug(x) } )
        							  //val computeTime = new Time(600)
        							  //val processReceiveRdd = processReceiveStream.compute(Time(600))
        							  //logger.debug( "Rdd Size - " + processReceiveRdd.size )
        							  val consumer = new StreamConsumer().consumer(processReceiveStream)
        							  
        							  ssc.start()
        							} catch  {
        						  case e @ ( _:IOException | _:ParseException | _:Exception) => 
      						    	//Log error for Data failure for each Node in the Setup Row;
      					       	//config_id, lastRunTime, status, ErrorData, errorMsg
      					     	  //statusRow = RowFactory.create(id, processTimeStamp, "E", nodeJSONArray, e.getMessage());
      					    	  //listStatusRows.add(statusRow);
      							    logger.error("Error while streaming - " + e.getMessage());
      							    e.printStackTrace();
      						} 
      						
        		   }  // processType
               
      			}  //congifList.next()
*/