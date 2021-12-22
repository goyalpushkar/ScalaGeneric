package com.cswg.testing

//Modified on 08/22/2017 to change wwms_warehouse_number with wms_invoice_site

import java.util.Date
import java.util.HashMap

import scala.reflect._
import scala.reflect.runtime.universe._


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import org.apache.log4j.Level
//import com.cswg.wip.shipping.util.ShippingUtil

import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.slf4j.MDC

class TrailerInventoryByWarehouse {
  
}
object TrailerInventoryByWarehouse {

  //val logger: Logger = LoggerFactory.getLogger(this.getClass())
  var logger = LoggerFactory.getLogger("Examples");

  
  def main(args: Array[String]) { 
      MDC.put("fileName", "M4033_WCSHTrailerInventory")
      val currrentTime =  if(args.length>0) getCurrentTime(args(0)).toString() else getCurrentTime.toString()
      logger.debug("currrentTime: "+currrentTime.toString());
      val conf = new SparkConf()
          .setAppName("M4033_WCSHTrailerInventory")
          .setMaster("local[6]")
          .set("spark.cassandra.connection.host", "10.0.42.21,10.0.42.22,10.0.43.20")
          .set("spark.cassandra.auth.username", "cssparkadmin")
			    .set("spark.cassandra.auth.password", "devspark") 
          .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")   
          .set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
          //.set("spark.cassandra.output.ignoreNulls","true")
  
      val sc = new SparkContext(conf);
      val sqlContext = new org.apache.spark.sql.SQLContext(sc);
      var status ="Error"
      try {
        //logger.debug("Trailer Inventory By Warehouse Started for "+ new Date())
        logger.debug("Trailer Inventory By Warehouse Started for "+ new Date())
        
        // Fetch & Load "yms_by_warehouse" table to start processing Trailer inventory with Processed flag as 'N' and then make them as Processed 'P'.
        var dfYMS = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "yms_by_warehouse", "keyspace" -> "wip_shipping_ing"))
            .load();
        dfYMS.registerTempTable("YMSWhseAll");
        
        // Fetch & Load "location_master_by_warehouse_number" table to lookup YardId/Name with wms_whse_no.
        var dfLocMasterAll = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "location_master_by_warehouse_number", "keyspace" -> "ebs_master_data"))
            .load();
        dfLocMasterAll.registerTempTable("LocMasterAll");
        
        //var dfWhse = dfYMSWhseAll.select("warehouse").distinct().cache(); // Fetch distinct Yard names as they are stored as 'warehouse' in YMS table.
        var dfWhse = sqlContext.sql("select distinct warehouse FROM YMSWhseAll").cache()
        dfWhse.registerTempTable("DistinctWhseList");
        dfWhse.show();
        var whseList = dfWhse.rdd.map(x => (x(0).toString())).collect();
        logger.debug("whseList.isEmpty:"+whseList.isEmpty + " & Length:" + whseList.length);
                
        // Fetch & Load "warehouse_operation_time" table to lookup Whse_Id/Start_time data with Operational data.
        var dfWhseOperTime = sqlContext.read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> "warehouse_operation_time", "keyspace" -> "wip_shipping_ing"))
            .load();
        dfWhseOperTime.registerTempTable("WhseOperationTime");
        
        whseList.foreach ( x =>{

            var whse_tmp = x.toString();
            logger.debug("Inside loop whse_tmp: "+whse_tmp);
            
            //Modified on 08/22/2017 - wms_warehouse_number is replaced with wms_invoice_site 
            var dfLocLookup = sqlContext.sql("select cast(wms_invoice_site as Int) wms_invoice_site,yms_yard_name FROM LocMasterAll where yms_yard_name = '"+whse_tmp.toString()+"'").cache()
            
            var whse = dfLocLookup.first().get(0).toString();
            logger.debug("dfLocWmsWhseNo.first().get(0): "+whse.toString());
            
//            if (null == whse) {
//                if (null != whse_tmp && "Chester".equalsIgnoreCase(whse_tmp)) {
//                  whse = "44";
//                } else if (null != whse_tmp && "Newburgh".equalsIgnoreCase(whse_tmp)) {
//                  whse = "29";
//                } else if (null != whse_tmp && "Bethlehem".equalsIgnoreCase(whse_tmp)) {
//                  whse = "76";
//                } else {
//                  whse = whse_tmp;
//                }
//            }
            
            // Step 0: Find out Ops date based on Current time and Whse_id:
            var operTimeDF = dfWhseOperTime.filter("facility = '"+whse.toString()+"'")
                          .filter(date_format(dfWhseOperTime("end_time"), "yyyy-MM-dd HH:mm:ss") >= getCurrentTime(currrentTime) )
                          .filter(date_format(dfWhseOperTime("start_time"), "yyyy-MM-dd HH:mm:ss") <= getCurrentTime(currrentTime)).cache();
            operTimeDF.show();
            
            if (!operTimeDF.rdd.isEmpty) {
            var var_ops_date = operTimeDF.select("ops_date").first().get(0).toString();
            logger.debug("whse: "+whse.toString()+", Ops_date:"+var_ops_date);
            
            // 1. Get total count based on LoadType per warehouse.
            var dfLoadTypes = sqlContext.sql("select '"+whse.toString()+"' as warehouse_id, '"+var_ops_date.toString()+"' as ops_date, loadtype as load_type, count(vehiclenumber) as total_availability FROM YMSWhseAll where is_processed = 'N' and warehouse='" +whse_tmp.toString()+"' group by warehouse,loadtype ").cache()
            dfLoadTypes.registerTempTable("load_types");
             
            // 2. Get count of trailers based on LoadType and Profiles per warehouse.
            var dfProfiles = sqlContext.sql("select '"+whse+"' as warehouse_id,loadtype, profile, count(vehiclenumber) as count FROM YMSWhseAll where is_processed = 'N' and warehouse='" +whse_tmp.toString()+"' group by warehouse,loadtype, profile order by loadtype").cache()
            dfProfiles.registerTempTable("profiles");
            //dfProfiles.show();  Commented on 08/22/2017

            var profileList = dfProfiles.rdd.map(r=>(r(1).toString(),r(2).toString(),r.getLong(3))).collect();
            
            var dfDistinctLoads = sqlContext.sql("select distinct loadtype FROM profiles order by loadtype").cache();
            //dfDistinctLoads.show();  Commented on 08/22/2017
             
            // 3. Get all distinct profiles to get profile list for All Load types.
            var dfDisProfiles = sqlContext.sql("select distinct profile FROM profiles order by profile").cache()
            //dfDisProfiles.show();  Commented on 08/22/2017
            
            var disProfileList = dfDisProfiles.rdd.map(r=>(r(0).toString())).collect();
            
            val mapAvailInvSchema = {
                    val whid = StructField("warehouse_id", StringType)
                    val opsdate = StructField("ops_date", StringType)
                    val loadtype = StructField("load_type", StringType)
                    val availMap = StructField("availability_by_profile", DataTypes.createMapType(StringType, IntegerType))
                    
                    new StructType(Array(whid, opsdate, loadtype, availMap))
            }
                
            logger.debug("No of recs in Profile List:"+profileList.length);
     
             var resultRows = dfDistinctLoads.rdd.repartition(1).map { x =>
                   
                  var load_var = x(0).toString().replace('[', ' ').replace(']', ' ' ).trim();
                  var availByProfileMap = new HashMap[String,Integer]();
                  profileList.foreach(row => {
                      if (row._1.toString().equalsIgnoreCase(load_var.toString())) {
                          availByProfileMap.put(row._2.toString(), row._3.toString().toInt);
                      }
                  });
                  
                  var profile_value = "";
                  disProfileList.foreach(x => {
                      if (!availByProfileMap.containsKey(x.toString())) {
                        availByProfileMap.put(x.toString(), 0);
                      }
                  });
                   
                  Row(whse.toString(),var_ops_date.toString(),load_var,availByProfileMap);              
             }
                
             var dfAvailMap = sqlContext.createDataFrame(resultRows, mapAvailInvSchema);
             dfAvailMap.registerTempTable("loadsWithAvailability");
             logger.debug("Displaying profiles vs availability map");
             //dfAvailMap.show();  Commented on 08/22/2017
             
            // Join Profiles with Availability Map to reduce to 1 record per LoadType. , '"+currrentTime+"' as , loadtype as , count(vehiclenumber) as 
            var dfTrailerAvailability = sqlContext.sql("select lt.warehouse_id, lt.ops_date, lt.load_type, lt.total_availability, lwa.availability_by_profile FROM load_types lt, loadsWithAvailability lwa" 
                                        +" where lt.warehouse_id = lwa.warehouse_id and lt.ops_date = lwa.ops_date and lt.load_type = lwa.load_type order by lt.load_type").cache();
            dfTrailerAvailability.registerTempTable("TrailerAvailability");
            //dfTrailerAvailability.show();  Commented on 08/22/2017
            
            // 4. Get distinct LoadType per warehouse to find out who do not exist at current processing.
            var dfLoadTypesAll=sqlContext.sql("select '"+whse.toString()+"' as warehouse_id, '"+var_ops_date.toString()+"' as ops_date, loadtype as load_type, count(vehiclenumber) as total_availability FROM YMSWhseAll where warehouse='" +whse_tmp.toString()+"' group by warehouse,loadtype ").cache()
            dfLoadTypesAll.registerTempTable("loadtypes_all");
            
            // 5. 
            var dfRemovable = sqlContext.sql("select lta.warehouse_id, lta.ops_date, lta.load_type, null as total_availability, null as availability_by_profile FROM loadtypes_all lta LEFT JOIN TrailerAvailability ta ON lta.load_type = ta.load_type where ta.load_type IS NULL and lta.warehouse_id = ta.warehouse_id and lta.ops_date = ta.ops_date ").cache()
            dfRemovable.registerTempTable("Removable_LoadTypes");
            //dfRemovable.show();  Commented on 08/22/2017
            
            var dfPrcsdYMS = sqlContext.sql("select warehouse, loadtype, profile, vehicleid, 'Y' as is_processed, '"+currrentTime.toString()+"' as processed_time FROM YMSWhseAll where is_processed = 'N' and warehouse='" +whse_tmp.toString()+"' ").cache()
            
            saveDataFrame(dfTrailerAvailability.select("warehouse_id","ops_date","load_type","total_availability","availability_by_profile"),"wip_shipping_prs", "trailer_availability_by_warehouse");
            saveDataFrame(dfPrcsdYMS, "wip_shipping_ing","yms_by_warehouse"); // Save data as processed.
        }
        });
        
        sqlContext.clearCache()
        //logger.debug("Trailer Inventory By Warehouse Completed at: "+ new Date())
        logger.debug("Trailer Inventory By Warehouse Completed at: "+ new Date())
        
        status ="Completed"
      } catch {
        case e:Exception  => 
          logger.error("Error while executing Trailer inventory by warehouse at : " +  currrentTime + e.getMessage)
      }
      logger.debug("CSSparkJobStatus:" + status)
     
  }
  
  @throws(classOf[Exception])
   def saveDataFrame(dataFrame: DataFrame,keyspaceName: String, tablename: String): Boolean = {     
      dataFrame.write.format("org.apache.spark.sql.cassandra")
        .mode("append").option("table", tablename)
        .option("keyspace", keyspaceName).save() 

      return true;

    }    
  
}