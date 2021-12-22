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
