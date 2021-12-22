package com.cswg.wip.neo4j.etl

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.rdd._

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{ArrayBuffer => MArrayBuffer}

import org.slf4j.{LoggerFactory, MDC }

//import org.json.simple.{JSONArray, JSONObject}
//import org.json.simple.parser.{JSONParser, ParseException }

import java.io.BufferedReader

import net.liftweb.json._
import net.liftweb.json.Serialization.write

class StreamConsumer extends java.io.Serializable {
      val logger = LoggerFactory.getLogger(this.getClass())   //"com.cswg.wip.neo4j.etl")
      
      def consumer( receiveStream: ReceiverInputDStream[ streamedData ] ) = { 
          logger.debug( "- Inside Consumer - ")
          try{
              receiveStream.foreachRDD { x => x.foreach { y => processRDD(y) } }
          }catch {
            case e @ (_ :Exception | _ :Error | _ :java.io.IOException  ) =>
                  logger.error("Error while executing consumer at : " +  e.getMessage + " " + e.printStackTrace() )           
          }
          
      }
      
      def consumeRDD( receiveStream: RDD[ streamedData ] ) = {
          receiveStream.foreach { x => processRDD(x) }
      }
      
      def processRDD( receivedData: streamedData  )  = {
          logger.debug( "- Inside processRDD - ")
          
          logger.debug( "Receive Data Size - " + receivedData )
          /*val oracleResultSet = oracleData.asInstanceOf[java.sql.ResultSet]          
          while ( oracleResultSet.next() ) {
                logger.info( "-- Setup Details -- " + "\n" 
				               + "      Identification    " + oracleResultSet.getString(1) + "\n"
				               + "      Process Type      " + oracleResultSet.getString(2) + "\n"
				               + "      Process Query     " + oracleResultSet.getString(3) + "\n"
				               + "      Node Relations    " + oracleResultSet.getString(4) + "\n"
				               + "      Constraint Type    " + oracleResultSet.getString(5) + "\n"
				               + "      Constraint Action    " + oracleResultSet.getString(6) + "\n"
				               + "      Constraint     " + oracleResultSet.getString(7) + "\n"
				               + "      Id    " + oracleResultSet.getString(8) + "\n"
				               + "      Source System    " + oracleResultSet.getString(9) + "\n"
							   );
          }*/
         try{ 
                  //receivedData. { x => 
                                        logger.info( "-- Processed Details -- " + "\n" 
                            				               + "      Identification    " + receivedData.identification_name + "\n"
                            				               + "      Process Type      " + receivedData.process_type + "\n"
                            				               + "      Process Query     " + receivedData.process_query + "\n"
                            				               //+ "      Node Relations    " + receivedData.node_relations + "\n"
                            				               + "      Constraint Type    " + receivedData.constraint_type + "\n"
                            				               + "      Constraint Action    " + receivedData.constraint_action + "\n"
                            				               //+ "      Constraint     " + receivedData.constraint_properties + "\n"
                            				               + "      Id    " + receivedData.config_id + "\n"
                            				               + "      Source System    " + receivedData.source_system + "\n"
                            				               + "      Error Message    " + receivedData.error_msg + "\n"
                            				               + "      Result data size " + receivedData.resultData.size + "\n"
    							                               );
                                        val nodeCharacterStream = new BufferedReader( receivedData.node_relations )
                                        val nodeString = Stream.continually( nodeCharacterStream.readLine() ).takeWhile { x => x!= null }.mkString("\n")
                                                            
                                        processNodes( nodeString, receivedData.resultData )  
                                       /* val parser = new JSONParser();
                                        
                                        if ( x.process_type.equalsIgnoreCase("QUERY") ) {
                                            val nodeJSONObject =  parser.parse(x.node_relations)    //.asInstanceOf[JSONObject];
                          						      nodeJSONObject.get("Nodes")
                                        }else if ( x.process_type.equalsIgnoreCase("CONSTRAINT") ){
                                            val nodeJSONObject =  parser.parse(x.node_relations)    //.asInstanceOf[JSONObject];
                          						      nodeJSONObject.get("Constraint")
                                          
                                        }else if ( x.process_type.equalsIgnoreCase("DELETE") ){
                                            val nodeJSONObject =  parser.parse(x.node_relations)   //.asInstanceOf[JSONObject];
                                            val nodes = nodeJSONObject.get("Nodes")                //.asInstanceOf[JSONArray]
                                            nodes.forEach()
                                            val iterateOnNodes = nodes.listIterator()
                                            while( iterateOnNodes.hasNext() ){
                                              
                                              
                                            }
                                            //nodes.forEach( processNodes() )
                          						      /*for ( nodes <- nodeJSONObject.get("Nodes").asInstanceOf[JSONArray] )
                                            {                         						         
                          						        
                                            }*/
                          						      
                                        } */
                                                                      
                                 // }
         }catch{
            case e @ (_ :Exception | _ :Error | _ :java.io.IOException  ) =>
                  logger.error("Error while executing processRDD at : " +  e.getMessage + " " + e.printStackTrace() )
         }
          
      }
      
      /*def processJsonString() = {
         logger.debug("- Inside processJsonString -")
         try{
             val congifList = getConfigurtionList()
             logger.debug("returned configlist")
             
             val streamedDataList: MArrayBuffer[streamedData] = MArrayBuffer[streamedData]()
             val queryColumnList: MArrayBuffer[resultSetColumns] = MArrayBuffer[resultSetColumns]() 
             if ( congifList != null ){
             
                   while ( congifList.next() ){   // Loop 2 for each configuration entry
                	     logger.debug( "-- Setup Details -- " + "\n" 
                               + "      Identification    " + congifList.getString(1) + "\n"
                               + "      Process Type      " + congifList.getString(2) + "\n"
                               + "      Process Query     " + congifList.getString(3) + "\n"
                               + "      Constraint Type    " + congifList.getString(5) + "\n"
                               + "      Constraint Action    " + congifList.getString(6) + "\n"
                               + "      Config Id        " + congifList.getInt(8) + "\n"
                               + "      Source System    " + congifList.getString(9) + "\n"
                               +  "-- End Setup Details -- "
                			   );
                       val id = congifList.getInt(8)
                    	 val sourceSytem = congifList.getString(9)
                    	 val processType = congifList.getString(2)
                    	 val query = congifList.getString(3)
            	 
                    	 if( processType.equalsIgnoreCase("QUERY") ) {
                    	     logger.debug(" - Run Query - ")
                    			 val processData = executeQuery(sourceSytem, query, parameterList)
                    			 val queryData = processData.result
                    			 val metaData = queryData.getMetaData
                           val numberOfColumns = metaData.getColumnCount
                           var recordNum: Int = 0
                            while ( queryData.next() ){     // Loop 3 for ResultSet
                                 if ( queryData.getString("project_title").equalsIgnoreCase("Independent Ordering - Remaining Regions") 
                                    ||queryData.getString("project_title").equalsIgnoreCase("GS – Coppell – Warehouse ") ){    
                                     if ( !(queryColumnList.isEmpty) ){
                                        queryColumnList.clear()
                                        logger.debug(" - Cleared Class - ")
                                     }
                                     recordNum = recordNum.+(1)
                                     for ( i <- 1 to numberOfColumns ){
                                         //println(" - i - " + i + " - " + queryData.getString(i))
                                         val columnList = resultSetColumns(metaData.getColumnName(i)
                                                                          ,metaData.getColumnType(i)
                                                                          ,queryData.getString(i)
                                                                          //,if ( queryData. == null ) null else queryData.getString(i)
                                                                          )
                                         queryColumnList.+=(columnList)
                                     }
                                     /*queryColumnList.foreach { x => logger.debug(recordNum + "\n" 
                                                                               + "Column Name - " + x.columnName + "\n"
                                                                               + "Column Data - " + x.columnData + "\n"
                                                                               + "Column Type - " + x.columnType + "\n"
                                                                               ) }*/
                                     logger.debug(" - Got Column List - ")
                                     
                                     val rowList = streamedData( congifList.getInt(8), congifList.getString(1), congifList.getString(9)
                                                                ,congifList.getString(2), congifList.getString(3), congifList.getClob(4).getCharacterStream
                                                                ,congifList.getString(5), congifList.getString(6), null
                                                                ,congifList.getTimestamp(10), processData.msg, queryColumnList, recordNum
                                                               )
                                     logger.debug(" - Created Class - ")
                                     rowList.resultData.foreach { x => logger.debug( recordNum + " : " + x.columnName + " :rowList " + x.columnData) }
                                     
                                     streamedDataList.+=(rowList)
                                     
                                     streamedDataList.foreach { x => x.resultData.foreach { y => logger.debug( x.recordNumber + " :stremedData " + y.columnData) } }
                                 }
                            }  // Loop 3 for ResultSet

                    	 }    // If QUERY check
                       
                       /*streamedDataList.foreach { x => x.resultData.foreach { y => logger.debug( x.recordNumber + " :stremedData " + y.columnData) }
                    	                                     val nodeCharacterStream = new BufferedReader( x.node_relations )
                                                           val nodeString = Stream.continually( nodeCharacterStream.readLine() ).takeWhile { x => x!= null }.mkString("\n")
                                                            
                                                           processNodes( nodeString, x.resultData )  
                    	                              }*/
                       
                   }    // Loop 2 for each configuration entry
             }
         }catch{
            case e @ (_ :Exception | _ :Error | _ :java.io.IOException  ) =>
                  logger.error("Error while executing processJsonString at : " +  e.getMessage + " " + e.printStackTrace() )
         }
                
      }*/
      
      case class Nodes(
          sourceNodeName: String
         ,sourceNodeProperties: JObject
         ,destNodeName: String
         ,destNodeProperties: JObject
         ,relationName: String
         ,relationProperties: Option[JObject] 
         ,relationType: String      
         ,sourceNodeKeys: JObject
         ,destNodeKeys: JObject
         ,relationKeys: JObject
      )
      
      def processNodes( nodeObjects: String, resultData: MArrayBuffer[ MArrayBuffer[resultSetColumns] ]) = {
          logger.debug( "- Inside processNodes - " )
          
          implicit val formats = DefaultFormats
          try{
             
              val sourceNodeAttributes: MMap[String, String] = MMap[String, String]()
              val destNodeAttributes: MMap[String, String] = MMap[String, String]()
              val relationAttributes: MMap[String, String] = MMap[String, String]()
              val sourceNodeKey: MMap[String, String] = MMap[String, String]()
              val destNodeKey: MMap[String, String] = MMap[String, String]()
              val relationKey: MMap[String, String] = MMap[String, String]()
              val nodesRelation: MArrayBuffer[relationShips] = MArrayBuffer()
              
              //logger.debug( "- Inside processNodes - " + nodeObjects)
              val nodeParentObject = parse(nodeObjects)
              logger.debug( "- nodeParentObject - " + nodeParentObject)
              
              val nodeNodeObject = ( nodeParentObject \\ "Nodes")
              logger.debug( "- nodeNodeObject - " + nodeNodeObject) 
              for ( nodeNodeArray <- nodeNodeObject.children ){   // Array of Nodes
                  logger.debug( "- nodeNodeArray - " + nodeNodeArray) 
                  
                  //val nodeSubChilds = nodeNodeArray.children
                  for ( subChild <- nodeNodeArray.children ){    // Each node in the Relation
                    
                      for( rows <- resultData )
                      {                                          // Each Row in the Data
                          logger.debug( "- subChild - " + subChild)
                          val m = subChild.extract[Nodes]  //
                          logger.debug( "\n" 
                                      + "Source Node Name - " + m.sourceNodeName + "\n"
                                      + "Destination Node Name - " + m.destNodeName + "\n"
                                      + "Relation Name - " + m.relationName + "\n"
                                      + "Source Node Properties - " + m.sourceNodeProperties + "\n"
                                      + "Dest Node Properties - " + m.destNodeProperties + "\n"
                                      + "Relation Properties - " + m.relationProperties + "\n"
                                     ) 
                           val sourceNodeName =  m.sourceNodeName         
                           val destNodeName =  m.destNodeName 
                           val relationName =  m.relationName 
                           val relationType =  m.relationType 
                           
                           if ( !(m.sourceNodeProperties.obj.isEmpty) ){
                               m.sourceNodeProperties.obj.map { x => //logger.debug("Property Name: " + x.name + " -> Property Value: " + x.value.extract[String] ) 
                                                                 rows.map { y => //logger.debug("Check if column exists")
                                                                                       if ( y.columnName.equalsIgnoreCase(x.value.extract[String]) ){
                                                                                        //logger.debug("Column - " + y.columnName + " found with value " + y.columnData )
                                                                                        sourceNodeAttributes ++= MMap( x.name.toString() -> y.columnData )
                                                                                       }
                                                                                } 
                                                          }  
                           }
                          // Added on 02/01/2018
    							    		if ( !(m.sourceNodeKeys.obj.isEmpty) ){
    							    		  m.sourceNodeKeys.obj.map { x => //logger.debug("Property Name: " + x.name + " -> Property Value: " + x.value.extract[String] ) 
                                                                 rows.map { y => //logger.debug("Check if column exists")
                                                                                       if ( y.columnName.equalsIgnoreCase(x.value.extract[String]) ){
                                                                                        //logger.debug("Column - " + y.columnName + " found with value " + y.columnData )
                                                                                        sourceNodeKey ++= MMap( x.name.toString() -> y.columnData )
                                                                                       }
                                                                                } 
                                                          }  
    							    		}
    									  	// Ended on 02/01/2018
                          if ( !(m.destNodeProperties.obj.isEmpty) ){
                              m.destNodeProperties.obj.map { x => //logger.debug("Property Name: " + x.name + " -> Property Value: " + x.value.extract[String] ) 
                                                                 rows.map { y => //logger.debug("Check if column exists")
                                                                                       if ( y.columnName.equalsIgnoreCase(x.value.extract[String]) ){
                                                                                        //logger.debug("Column - " + y.columnName + " found with value " + y.columnData )
                                                                                        destNodeAttributes ++= MMap( x.name.toString() -> y.columnData )
                                                                                       }
                                                                                } 
                                                          }
                          }
                          // Added on 02/01/2018
    							    		if ( !(m.destNodeKeys.obj.isEmpty) ){
    							    		  m.destNodeKeys.obj.map { x => //logger.debug("Property Name: " + x.name + " -> Property Value: " + x.value.extract[String] ) 
                                                                 rows.map { y => //logger.debug("Check if column exists")
                                                                                       if ( y.columnName.equalsIgnoreCase(x.value.extract[String]) ){
                                                                                        //logger.debug("Column - " + y.columnName + " found with value " + y.columnData )
                                                                                        destNodeKey ++= MMap( x.name.toString() -> y.columnData )
                                                                                       }
                                                                                } 
                                                          }  
    							    		}
    									  	// Ended on 02/01/2018
                          if ( !(m.relationProperties.isEmpty) ){
                              m.relationProperties.getOrElse(null).obj.map { x => //logger.debug("Property Name: " + x.name + " -> Property Value: " + x.value.extract[String] ) 
                                                                     rows.map { y => //logger.debug("Check if column exists")
                                                                                           if ( y.columnName.equalsIgnoreCase(x.value.extract[String]) ){
                                                                                            //logger.debug("Column - " + y.columnName + " found with value " + y.columnData )
                                                                                            relationAttributes ++= MMap( x.name.toString() -> y.columnData )
                                                                                           }
                                                                                    } 
                                                              }
                          }
                          // Added on 02/01/2018
    							    		if ( !(m.relationKeys.obj.isEmpty) ){
    							    		  m.relationKeys.obj.map { x => //logger.debug("Property Name: " + x.name + " -> Property Value: " + x.value.extract[String] ) 
                                                                 rows.map { y => //logger.debug("Check if column exists")
                                                                                       if ( y.columnName.equalsIgnoreCase(x.value.extract[String]) ){
                                                                                        //logger.debug("Column - " + y.columnName + " found with value " + y.columnData )
                                                                                        relationKey ++= MMap( x.name.toString() -> y.columnData )
                                                                                       }
                                                                                } 
                                                          }  
    							    		}
    									  	// Ended on 02/01/2018
                          sourceNodeAttributes.foreach( f => logger.debug( "SourceNode Propertry Name: " + f._1 +  " -> SourceNode Propertry Value: " + f._2))
                          
                          val nodeRelation = relationShips( sourceNodeName, sourceNodeAttributes
                                                           ,destNodeName, destNodeAttributes
                                                           ,relationName, relationAttributes
                                                           ,relationType
                                                           ,sourceNodeKey
                                                           ,destNodeKey
                                                           ,relationKey
                                                         ) 
                          nodesRelation.+=(nodeRelation)
                      }    // // Each Row in the Data
                  }     // // Each node in the Relation
              }     // // Array of Nodes
              
              nodesRelation.foreach { x => createRelationshipDynamic( x.destNodeName
                                                                     ,x.destNodeAttributes 
                                                                     ,x.destinationRelationType
                              											                 ,x.relationName
                              									                     ,x.relationAttributes
                              											                 ,x.sourceNodeName
                              											                 ,x.sourceNodeAttributes
                              											                 ,"-"
                              											                 ,x.sourceNodeKey
                              											                 ,x.destNodeKey
                              											                 ,x.relationKey
									                                                ) 
                                       }
              
          }catch{
            case e @ (_ :Exception | _ :Error | _ :java.io.IOException  ) =>
                  logger.error("Error while executing processNodes at : " +  e.getMessage + " " + e.printStackTrace() )
         }
      }
}


                                     /*for ( sourceProperties <- m.sourceNodeProperties.children ){
                           logger.debug( "sourceProperties - " + sourceProperties + "\n"
                                      + "sourceProperties.values - " + sourceProperties.values + "\n"
                                      + "sourceProperties.apply - " + m.sourceNodeProperties.obj + "\n"
                                     // + "sourceProperties.extract - " + sourceProperties.extract + "\n"
                                      )
                       }*/
              /*nodeObjects.keySet().toArray().foreach { x => ??? }
              
              val keys = nodeObjects.keySet().toArray()
              keys.foreach { x => 
                                 if ( x.toString().equalsIgnoreCase("sourceNodeName") )
                                    sourceNode = nodeObjects.
                
                            }*/
              /*for ( nodeKey <- keys ){
                
              }*/
              //forEach( x => if ( x)
                                    /*logger.debug( "Source Node Name - " + subChild.\\("sourceNodeName") + "-> " + subChild.\("sourceNodeName")  + "\n"
                                 +  "Dest Node Name -o " + subChild.\\("destNodeName") + "-> " + subChild.\("destNodeName")  + "\n"
                                 +  "Relation Name - " + subChild.\\("relationName") + "-> " + subChild.\("relationName") + "\n"
                                 +  "Source node Properties - " + subChild.\\("sourceNodeProperties") 
                                )
                      
                       subChild.\\("sourceNodeProperties").values.map( f => println( f._1 + " - " + f._2) )
                       */
                      /*for ( sourceAttributes <- subChild.\\("sourceNodeProperties").values.map(f) ){
                          logger.debug( "sourceAttributes - " + sourceAttributes + " -> " )
                      }*/
                                
                     //relationShips( subChild.\\("sourceNodeName"), subChild.\("sourceNodeName").toString()
                     //     
                         //              )