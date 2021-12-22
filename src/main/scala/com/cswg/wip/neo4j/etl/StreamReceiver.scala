package com.cswg.wip.neo4j.etl

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.receiver

import org.slf4j.{LoggerFactory, MDC }

//import org.json.simple.{JSONArray, JSONObject}
//import org.json.simple.parser.{JSONParser, ParseException }
import java.util.ListIterator
import java.sql.ResultSet
import java.io.{ Reader, IOException }

import java.sql.Timestamp
//import scala.util.parsing.json.{Parser, JSONArray, JSONObject, JSON}
//import scala.util.logging._
//import play.api.libs.json._

import scala.collection.mutable.{ArrayBuffer => MArrayBuffer}
import scala.collection.mutable.{Map => MMap}
//import scala.collection.mutable.Iterable

import net.liftweb.json._
import net.liftweb.json.Serialization.write

/*import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException*/

   case class resultSetColumns( columnName: String, columnType: Int, columnData: String )
   
   case class streamedData( config_id: Int, identification_name: String, source_system: String, process_type: String
                           ,process_query: String, node_relations: Reader, constraint_type: String, constraint_action: String
                           ,constraint_properties: Reader, last_run_timestamp: Timestamp, error_msg: String
                           ,resultData: MArrayBuffer[ MArrayBuffer[resultSetColumns] ], recordNumber: Int
                          )  
 
  //MArrayBuffer
  class StreamReceiver() extends Receiver[ streamedData ](StorageLevel.MEMORY_AND_DISK) with Logging {  //congifList: java.sql.ResultSet
     //setupData: java.sql.ResultSet, sourceSytem: String, query: String
     //MArrayBuffer[queryResult] 
     val logger = LoggerFactory.getLogger(this.getClass())   //"com.cswg.wip.neo4j.etl")
     logger.info( " - Inside StreamReceiver - " + getCurrentTime() ) 
     
     def onStart() {
        println( " - Start - " + getCurrentTime()) 
        //onStart()
        new Thread("Setup Receiver") {
             override def run() { receive() } //setupData, sourceSytem, query
         }.start()
        
     }
     
     def onStop() {
         logger.info( " - Streaming Stopped - " ) 
     }
     
     def receive(){  //congifList: java.sql.ResultSet setupData: java.sql.ResultSet, sourceSytem: String, query: String
         println( " - Inside receive - " +  getCurrentTime() + "\n"
                    // + " - setupData - " + setupData.getFetchSize + "\n"
                    // + " - sourceSytem - " + sourceSytem + "\n"
                    // + " - query - " + query + "\n"
                     )
         
         val queryParameters = MMap[String, String]();
         val batchSize = 1000
         try{
               val congifList = getConfigurtionList()
               var rowList: streamedData = null
               var streamedDataList: MArrayBuffer[streamedData] = MArrayBuffer[streamedData]()
               var queryColumnList: MArrayBuffer[resultSetColumns] = MArrayBuffer[resultSetColumns]() 
               var completeDataQuery: MArrayBuffer[ MArrayBuffer[resultSetColumns] ] = MArrayBuffer[ MArrayBuffer[resultSetColumns] ]() 
               while ( !isStopped() ){   // Loop 1 for streaming continuity
                     
                   if ( congifList != null ){
                   
                         while ( congifList.next() ){   // Loop 2 for each configuration entry
                    				 println( "-- Setup Details -- " + "\n" 
                  				               + "      Identification    " + congifList.getString(1) + "\n"
                  				               + "      Process Type      " + congifList.getString(2) + "\n"
                  				               + "      Process Query     " + congifList.getString(3) + "\n"
                  				               //+ "      Node Relations    " + congifList.getMetaData(4) + "\n"
                  				               + "      Constraint Type    " + congifList.getString(5) + "\n"
                  				               + "      Constraint Action    " + congifList.getString(6) + "\n"
                  				               //+ "      Constraint Properties    " + congifList.getClob(7) + "\n"
                  				               + "      Config Id        " + congifList.getInt(8) + "\n"
                  				               + "      Source System    " + congifList.getString(9) + "\n"
                  				               + "      Parameter List    " + congifList.getString(11) + "\n"
                  				               +  "-- End Setup Details -- "
                  							   );
                    				 //queryParameters.clear()
                             val id = congifList.getInt(8)
                    				 val sourceSytem = congifList.getString(9)
                    				 val processType = congifList.getString(2)
                    				 val query = congifList.getString(3)
                    				 //logger.info( "- queryParameters cleared - " + queryParameters.size );
					                   logger.info( "- queryParameters cleared - ")
                    					if ( congifList.getString(11) == null ){
                    						queryParameters.put("1", TimestampType + ";" + congifList.getString(10) );
                    					}else{
                    						val parameterList = congifList.getString(11);
                    						logger.info("parameterList - " + parameterList);
                    						
                    						/*val parser = new JSONParser();
                    						val parameterListJson = parser.parse(parameterList)  //:JSONObject
                    						logger.info("parameterListJson - " + parameterListJson);*/
                    						
                    						val nodeParentObject = parse(parameterList)
                                logger.debug( "- nodeParentObject - " + nodeParentObject)
                    						
                    						/*for ( parameterKey <- nodeParentObject.children ){
                    							logger.info("parameterKey - " + parameterKey + "\t" + " - parameterListJson.get(parameterKey) - " + parameterListJson.get(parameterKey));
                    							if ( parameterListJson.get(parameterKey).toString().equalsIgnoreCase("last_run_timestamp") ){
                    								queryParameters.put(parameterKey.toString(), TimestampType + ";" + congifList.getString(10) );
                    							}else{
                    								queryParameters.put(parameterKey.toString(), parameterListJson.get(parameterKey).toString() );
                    							}
                    						}*/
                    						//return;
                    						//queryParameters.put(setupData.getString(11));
                    					}
					
                    				 /*if( processType.equalsIgnoreCase("QUERY") ) {
                    				     println(" - Run Query - ")
                    						 val processData = executeQuery(sourceSytem, query, queryParameters)
                    						 val queryData = processData.result
                    						 val metaData = queryData.getMetaData
                                 val numberOfColumns = metaData.getColumnCount
                                 
                                 println( " - numberOfColumns - " +  numberOfColumns + "\n"
                                      //+   " - queryData.count - " + queryData.
                                        //+ " - metaData - " + metaData
                                       //+ " - metaData.getColumnName - " + metaData.getColumnName(1)
                                       //+ " - metaData.getColumnType - " + metaData.getColumnType(1)
                                       //+ " - queryData.getString(i) - " + queryData.getString(1)
                                        )
                                 while ( queryData.next() ){     // Loop 3 for each resultset entry
                                     
                                     if ( !(queryColumnList.isEmpty) ){
                                        queryColumnList.clear()
                                     }
                                     //println(" - Cleared Class - ")
                                     
                                     for ( i <- 1 to numberOfColumns ){
                                         //println(" - i - " + i + " - " + queryData.getString(i))
                                         val columnList = resultSetColumns(metaData.getColumnName(i)
                                                                          ,metaData.getColumnType(i)
                                                                          ,queryData.getString(i)
                                                                          //,if ( queryData. == null ) null else queryData.getString(i)
                                                                          )
                                         queryColumnList ++= Seq(columnList)
                                     }
                                     /*queryColumnList.foreach { x => println( "\n" 
                                                                               + "Column Name - " + x.columnName + "\n"
                                                                               + "Column Data - " + x.columnData + "\n"
                                                                               + "Column Type - " + x.columnType + "\n"
                                                                               ) }*/
                                     println(" - Got Column List - ")
                                     
                                     /*val rowList = streamedData( congifList.getInt(8), congifList.getString(1), congifList.getString(9)
                                                                ,congifList.getString(2), congifList.getString(3), congifList.getClob(4).getCharacterStream
                                                                ,congifList.getString(5), congifList.getString(6), null
                                                                ,congifList.getTimestamp(10), processData.msg, queryColumnList, 1
                                                               )*/
                                     println(" - Created Class - ")
                                     //rowList.resultData.foreach { x => println( 1 + " : " + x.columnName + " :rowList " + x.columnData) }
                                     
                                     //streamedDataList ++= Seq(rowList)
                                     //println(" - Saved Class in Array - " + streamedDataList.size)
                                     
                                     //store(rowList)
                                     /*if ( streamedDataList.size == batchSize ){
                                        println(" - Before batch data - ")
                                        store(streamedDataList)
                                        streamedDataList.clear()
                                        println(" - Saved batch - ")
                                     }*/
                                     
                                     completeDataQuery ++= Seq(queryColumnList)
                      					}    // End Loop 3 for each eresultSet entry 
                    				     
                    				    rowList = streamedData( congifList.getInt(8), congifList.getString(1), congifList.getString(9)
                                                            ,congifList.getString(2), congifList.getString(3), congifList.getClob(4).getCharacterStream
                                                            ,congifList.getString(5), congifList.getString(6), null
                                                            ,congifList.getTimestamp(10), processData.msg, completeDataQuery, 1
                                                           ) 
                    				     
                    		     }
                    				 else{
                    				     println(" - Run Constraint - ")
                    		         rowList = streamedData( congifList.getInt(8), congifList.getString(1), congifList.getString(9)
                                                            ,congifList.getString(2), congifList.getString(3), null
                                                            ,congifList.getString(5), congifList.getString(6), congifList.getClob(7).getCharacterStream
                                                            ,congifList.getTimestamp(10), null, null, 1
                                                           )
                                 //store(rowList)
                                 /*streamedDataList.+=(rowList)
                                 if ( streamedDataList.size == batchSize ){
                                        store(streamedDataList)
                                        streamedDataList.clear()
                                 }*/
                    		       
                    		     } */  // End processType
                    				 
                    				 //store(rowList)                    				 
                  		  }    // Loop 2 for each configuration entry
                         
                        // Save any pending streamedDataList
                        /*println(" - Before final data - ")
                        streamedDataList.foreach { x => x.resultData.foreach { y => println( x.identification_name + " :stremedData " + y.columnName + " - " + y.columnData) } }
                        store(streamedDataList)
                        streamedDataList.clear()         
                        println(" - Saved final data - ")*/
                   }    // End If congifList != null
               
             }   // End Loop 1 for streaming continuity
               
             //val connection = OracleDataSource.OracleConnectionPool.getConnection
             //http://www.tek-tips.com/viewthread.cfm?qid=1181382
             
             /*val queryResultList: MArrayBuffer[queryResult] = null 
             val metaData = processData.result.getMetaData
             val numberOfColumns = metaData.getColumnCount
             
             while ( !isStopped() ){ 
                 for ( i <- 1 to numberOfColumns ){
                     val row = queryResult.apply(metaData.getColumnName(i), metaData.getColumnType(i).toString(), processData.result.getString(i))
                     queryResultList.+=(row)
                 }
                 
                 // store will take parameter based on the value defined in extend Receiver[MArrayBuffer[queryResult]]
                 store(queryResultList)
             
             }  */         

             println( "Stopped Streaming" )
             restart("Trying to Connect Again")
         }catch {
           case e @ ( _:Exception ) => 
                println( "Error occurred while streaming data: " + e.getMessage )
         }
         
     }
  
}

   /*class StreamReceiver() extends Receiver[ streamedData ](StorageLevel.MEMORY_AND_DISK_SER_2) with Logging {
         val logger = LoggerFactory.getLogger(this.getClass())   //"com.cswg.wip.neo4j.etl")
         logger.info( " - Inside StreamReceiver - " + getCurrentTime() ) 
         
         def onStart() {
            println( " - Start - " + getCurrentTime()) 
            //onStart()
            new Thread("Setup Receiver") {
                 override def run() { receive() } //setupData, sourceSytem, query
             }.start()
            
         }
         
         def onStop() {
         logger.info( " - Streaming Stopped - " ) 
         }
         
         def receive(){ 
             println( " - Inside receive - " +  getCurrentTime() + "\n"
                        // + " - setupData - " + setupData.getFetchSize + "\n"
                        // + " - sourceSytem - " + sourceSytem + "\n"
                        // + " - query - " + query + "\n"
                         )
                     
             val batchSize = 1000
             try{
                   val congifList = getConfigurtionList()
                   var streamedDataList: MArrayBuffer[streamedData] = MArrayBuffer[streamedData]()
                   var queryColumnList: MArrayBuffer[resultSetColumns] = MArrayBuffer[resultSetColumns]() 
                   //while ( !isStopped() ){   // Loop 1 for streaming continuity
                         
                       if ( congifList != null ){
                       
                             while ( congifList.next() ){   // Loop 2 for each configuration entry
                        				 println( "-- Setup Details -- " + "\n" 
                      				               + "      Identification    " + congifList.getString(1) + "\n"
                      				               + "      Process Type      " + congifList.getString(2) + "\n"
                      				               + "      Process Query     " + congifList.getString(3) + "\n"
                      				               //+ "      Node Relations    " + congifList.getMetaData(4) + "\n"
                      				               + "      Constraint Type    " + congifList.getString(5) + "\n"
                      				               + "      Constraint Action    " + congifList.getString(6) + "\n"
                      				               //+ "      Constraint Properties    " + congifList.getClob(7) + "\n"
                      				               + "      Config Id        " + congifList.getInt(8) + "\n"
                      				               + "      Source System    " + congifList.getString(9) + "\n"
                      				               +  "-- End Setup Details -- "
                      							   )
                      							   
                      						val rowList = streamedData( congifList.getInt(8), congifList.getString(1), congifList.getString(9)
                                                                ,congifList.getString(2), congifList.getString(3), congifList.getClob(4).getCharacterStream
                                                                ,congifList.getString(5), congifList.getString(6), null
                                                                ,congifList.getTimestamp(10), null, queryColumnList, 1
                                                               )	   
                      					  store(rowList)
                             }
                       }
                   restart("Restarted")
             }catch {
                 case e @ ( _:Exception ) => 
                      println( "Error occurred while streaming data: " + e.getMessage )
               }
          }
   }*/