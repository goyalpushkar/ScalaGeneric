package com.cswg.fileProcess


import org.slf4j.{LoggerFactory, Logger, MDC}

//import org.apache.poi.POIXMLException;
//import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

//import org.apache.poi.hssf.usermodel.{HSSFCell,HSSFSheet,HSSFWorkbook}
//import org.apache.poi.xssf.usermodel.{XSSFCell,XSSFSheet,XSSFWorkbook}
import org.apache.poi.ss.usermodel.{Row, Cell, Sheet, Workbook, WorkbookFactory, DataFormatter, DateUtil, BuiltinFormats, CellStyle, IndexedColors, BorderStyle, FillPatternType, DataValidation, DataValidationConstraint, Comment, ClientAnchor}
//import org.apache.poi.hssf.usermodel.{HSSFWorkbook, HSSFSheet, HSSFCellStyle, HSSFRow, DVConstraint, HSSFDataValidation, HSSFComment, HSSFPatriarch, HSSFClientAnchor, HSSFRichTextString}
import org.apache.poi.xssf.usermodel.{XSSFWorkbook, XSSFSheet, XSSFCellStyle, XSSFRow, XSSFDataValidation, XSSFComment, XSSFClientAnchor, XSSFRichTextString, XSSFDataValidationConstraint, XSSFDataValidationHelper}
import org.apache.poi.xssf.streaming.{SXSSFWorkbook, SXSSFSheet}  // Added on 09/25/2018
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTDataValidation
import org.apache.poi.ss.util.{DateFormatConverter, CellRangeAddressList, CellAddress}
import org.apache.poi.util.IOUtils

import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject, JSONValue}

//import java.util.HashMap;
import java.io.{InputStreamReader, Reader, InputStream, File, FileInputStream, FileOutputStream}
import java.lang.Object
import oracle.jdbc.{OracleCallableStatement, OracleTypes, OracleConnection, OraclePreparedStatement}
import java.nio.charset
//import oracle.jbo.domain.BlobDomain;

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.{HashMap, LinkedHashMap, ArrayBuffer}
import scala.collection.JavaConversions._
import oracle.sql.{ARRAY, ArrayDescriptor, STRUCT, StructDescriptor, BLOB}
import java.text.DateFormat
import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.poi.ss.usermodel.Drawing
import java.nio.charset.Charset
    
class FileProcessing {
  
}

///Users/goyalpushkar/Library/Mobile Documents/com~apple~CloudDocs/Documents/ardpmsl docs/Expenses/Modified
///Users/goyalpushkar/opt/general
//C:/opt/general/data
case class uploadFileSchema ( filePathName: String = "/Users/goyalpushkar/opt/general/data", fileType: String = "csv", var fileName: String = "January2020_7123.csv"
                             ,fileSchema: Reader = null
                             ,noOfColumns: Int = 7
                             ,rowsToSkip: Int = 1, fileData: InputStream = null //Reader
                             ,uploadedBy: String = "Pushkar Goyal"
                             ,excelFileName: String = "Expense Sheet - 2019-US.xlsx"
                             ,author: String = "Program"
                              )

case class downloadFileSchema ( webUrl: String, connectionTimeout: Integer, readTimeout: Integer
                             ,filePathName: String, fileType: String, fileSchema: Reader
                             ,fileName: String, webserviceParams: Reader, searchValues: Reader
                             ,lovColumns: Reader
                             ,jobId: Int
                              )
                              
case class returnProcCall( passingParameters: HashMap[String, Any], status: String, errMessage: String )

object FileProcessing{
  var logger = LoggerFactory.getLogger(this.getClass);    //"Examples"
  logger.debug(" - FileProcessing Started - ")
  
  val currentRunTime = getCurrentTime(dateTimeFormat)
  var controlType = "Upload"
  var loggingEnabled = "N"
  var processFileName: String = ""
  
  //0 Success
  //1 Error
  def main(args: Array[String]): Unit = {
    // logger file name 
    MDC.put("fileName", "M0999_FileProcessing_" + args(2).toString() )  //jobName added on 09/08/2017
    
    val environment = if ( args(0) == null ) "dev" else args(0).toString 
    confFilePath = if (args(1) != null && args(1) != "" ) args(1).toString else "/opt/wildfly/rcm/properties/"  ///opt/general/data
    loggingEnabled = if (args(2) != null && args(2) != "" ) args(2).toString else "N"
    val fileName = if (args(3) != null && args(3) != "" ) args(3).toString else ""
    logger.info( " - environment - " + environment + "\n"
              + " - controlType - " + controlType + "\n"
              + " - confFilePath - " + confFilePath + "\n" 
              + " - loggingEnabled - " + loggingEnabled + "\n"
              + " - fileName - " + fileName
             )
             
     try{
          
          val uploadFileData = uploadFileSchema()
          
          if ( ! ( "".equalsIgnoreCase(fileName) ) ){
             uploadFileData.fileName = fileName
          }
          uploadFile(uploadFileData)
          
     }catch {
          case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>
			    	logger.error("Error in main at : " + e.getLocalizedMessage + ": " + e.printStackTrace() )
            //throw new Exception("Error in fileProcessing at : " + e.getLocalizedMessage + ": " + e.printStackTrace())
			    	//return "1"
         }
    //return "0"
    
  }
  
  
  
  def uploadFile(uploadFileData: uploadFileSchema): returnProcCall = {
    logger.info(  "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " + "\n" 
                + "                    uploadFile                     " + "\n" 
                + "+++++++++++++++++++++++++++++++++++++++++++++++++++ " )  
    
     var status: String = success
     var errMessage: String = null
     
    try{
        
        import scala.collection.mutable._
        
        var workbook: XSSFWorkbook = null
        var fileInputStream: FileInputStream = null
        if ( uploadFileData.fileData != null ) {
           logger.info("File data is extracted from column")
           workbook = WorkbookFactory.create( uploadFileData.fileData ).asInstanceOf[XSSFWorkbook]
        }else{
           logger.info("File data is extracted from file")
           val fileData = new File( uploadFileData.filePathName + "/" + uploadFileData.excelFileName )
           fileInputStream = new FileInputStream(fileData)
           workbook = WorkbookFactory.create(fileInputStream).asInstanceOf[XSSFWorkbook]
        }  
        
        if ( !( workbook.getNumberOfSheets() > 0 ) ) {
            logger.info("Number of Sheets not > 0")
        }else{
        
            logger.debug( "File Name - " + uploadFileData.filePathName + "/" + uploadFileData.fileName + "\n"
                        //+ "" + io.Source.fromFile(uploadFileData.filePathName + "/" + uploadFileData.fileName)
                        )
            import java.nio.charset.CodingErrorAction
            import scala.io.Codec
            
            implicit val codec = Codec("UTF-8")
            codec.onMalformedInput(CodingErrorAction.REPLACE)
            codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

            //Read csv file from bank
            import scala.io.Source._
            val fileData = io.Source.fromFile(uploadFileData.filePathName + "/" + uploadFileData.fileName)  //, Charset.forName("ISO8859_1")
            for( lines <- fileData.getLines().drop(1) ){
                 val rowData = lines.split(",") //.map(_.trim())
                  logger.info( "******************************************************" + "\n"
                             + "Date - " + rowData.apply(0) + "\t" + "Comment - " + rowData.apply(2) + "\t" + "Amount - " + rowData.apply(4) + "\n"
                             + "Expense Type - " + rowData.apply(5) + "\n" //+ "Expense Row - " + rowData.apply(6) + "\n"
                             + "Date Month - " + getStringtoDate(rowData.apply(0), "MM/dd/yyyy").getMonth + "\t" + convertCamelCase( getMonthName(getStringtoDate(rowData.apply(0), "MM/dd/yyyy").getMonth + 1) ) + "\t" //Giving Jan as 0
                             + "Date Date - " + getStringtoDate(rowData.apply(0), "MM/dd/yyyy").getDate + "\t"
                             //+ "Date Day - " + getStringtoDate(rowData.apply(0), "MM/dd/yyyy").getDay //1 = Monday
                             )
                  
                  val sheetName = convertCamelCase( getMonthName(getStringtoDate(rowData.apply(0), "MM/dd/yyyy").getMonth + 1) )           
                  val sheet = workbook.getSheet(sheetName)
                  if (sheet == null) {
                      logger.info(sheetName + " sheet not found in Expense Report")
                    
                  }else{
                      logger.info(sheetName + " sheet Found" + "\n"
                               // + "Sheet.first Row - " + sheet.getFirstRowNum 
                               // + " :Sheet.last Row - " + sheet.getLastRowNum                                
                                )
                      val rowIterator = sheet.iterator()
                      var cellIndex = 1
                      rowIterator.filter(p =>  p.getRowNum >= 3 && p.getRowNum <= 30 )
                      .foreach{ row => 	
                          if ( row != null ){
                              
                              //logger.info( " Expense Type - " + row.getCell(0).getRichStringCellValue.toString() )
                              //if ( row.getRowNum == 4)
                              if ( "Date".equalsIgnoreCase( row.getCell(0).getRichStringCellValue.toString() ) ){
                                 //var cellIndex = 1
                                 import scala.util.control.Breaks._
                                 breakable{
                                   for( cell <- row.cellIterator().drop(1) ) //.foreach
                                   //for ( cellIndexL <- 1 to 50 )
                                   {   
                                       //val cell = row.getCell(cellIndexL)
                                       //logger.info( "Cell Value - " + cell.getNumericCellValue.toInt + "\t" + cell.getColumnIndex) //.toString()
                                       if ( getStringtoDate(rowData.apply(0), "MM/dd/yyyy").getDate.toInt == cell.getNumericCellValue.toInt ){ 
                                          cellIndex = cell.getColumnIndex  //cellIndexL
                                          logger.info( "cellIndex - " + cellIndex )
                                          break()
                                       }
                                   }    // Loop through Dates Cells
                                 }   //breakable
                              }  //Date Check 
                              /*logger.info( "rowData.apply(5) - " + rowData.apply(5) + "\n"
                                         + "row.getCell(0).getRichStringCellValue.toString() - " + row.getCell(0).getRichStringCellValue.toString()
                                          )*/
                              if ( row.getCell(0).getRichStringCellValue.toString().equalsIgnoreCase(rowData.apply(5)) ){
                                 logger.info( "Expense Name Matched - " + row.getCell(0).getRichStringCellValue + "\n"  //row.getCell(0) + "\t" + 
                                            //+ "CellIndex - " + cellIndex + "\t" + "Cell - " + row.getCell(cellIndex) + "\t" + "Cell Comment - "  + row.getCell(cellIndex).getCellComment + "\n" 
                                            //+ (row.getCell(cellIndex)+"").toString().length() + " " + (row.getCell(cellIndex).getCellComment+"").toString().length() + "\n"
                                            //+ "Cell Index null? - " + ( (row.getCell(cellIndex)+"").toString().length() == 0 ) + "\n"
                                            //+ "Cell Comment null? - " + (row.getCell(cellIndex).getCellComment == null )  //( (row.getCell(cellIndex).getCellComment+"").toString().length() == 0 ) + " " + 
                                           )
                                 
                                 //Set Cell Value
                                 val existingValue = if ( (row.getCell(cellIndex)+"").toString().length() == 0 ) 0 else row.getCell(cellIndex).getNumericCellValue
                                   //if ( row.getCell(cellIndex) == null ) 0 else row.getCell(cellIndex).getNumericCellValue
                                   //if ( (row.getCell(cellIndex) +" ").trim.toString().length() == 0 ) 0 else row.getCell(cellIndex).getNumericCellValue
                                 val newValue = existingValue + (-1 * getStringtoDouble( rowData.apply(4) ).get )
                                 logger.info("existingValue - "+existingValue + "\t" + " :newValue - " + newValue ) 
                                 row.getCell(cellIndex).setCellValue( newValue )  //getStringtoDouble().get 
                                 
                                 //Create Comment Box
                                 val requiredCommentBox = sheet.asInstanceOf[XSSFSheet].createDrawingPatriarch();
                                 val anchor = new XSSFClientAnchor()   //0, 0, 0, 0, 4, 2, 6, 5   0,0,0,0, requiredCol.toShort, 0, (requiredCol + 1).toShort, 1
                                 anchor.setAnchorType( ClientAnchor.AnchorType.MOVE_AND_RESIZE )
                                 anchor.setCol1(cellIndex)
                                 anchor.setRow1(0)
                                 anchor.setCol2(cellIndex + 4)   //requiredCol + 1
                                 anchor.setRow2(4)    
                                 val requiredComment: Comment  = requiredCommentBox.createCellComment(anchor)   //.createComment(anchor)
                                 val existingComment: String = ( if ( (row.getCell(cellIndex)+"").toString().length() == 0 ) "" else ( if ( row.getCell(cellIndex).getCellComment == null) "" else row.getCell(cellIndex).getCellComment.getString.toString() ) ) 
                                   //if ( row.getCell(cellIndex) == null ) "" else row.getCell(cellIndex).getCellComment.getString
                                   //if ( (row.getCell(cellIndex)+"").toString().length() == 0 ) "" else row.getCell(cellIndex).getCellComment.getString
                                 val newComment = ( if ( "".equalsIgnoreCase(existingComment) ) "" else existingComment + "       " ) + (-1 * getStringtoDouble( rowData.apply(4) ).get ) + " -> " + rowData.apply(2).toString()
                                 logger.info("existingComment - "+ existingComment + "\t" + " :newComment- " + newComment )
                                 requiredComment.setAuthor(uploadFileData.author)
                                 requiredComment.setString(new XSSFRichTextString(newComment) )
                                 requiredComment.setAddress(new CellAddress(row.getCell(cellIndex) ) )
                                 
                                 //Set cell Comment
                                 row.getCell(cellIndex).setCellComment(requiredComment)
                              }                          
                          }  // row != null
                      }  //rowIterator
                  }   // sheet != null
                 
                  logger.info("******************************************************")
            }  // lines in the transactions
        }  //workbook.sheets > 0 

        fileInputStream.close();
 
        logger.info("Create New File")
        val outputStream = new FileOutputStream(uploadFileData.filePathName + "/" + uploadFileData.excelFileName);  // + "_1"
        workbook.write(outputStream);
        workbook.close();
        outputStream.close();
        logger.info("File Saved")
 
      }catch {
          case e @ (_ :Exception | _ :Error | _ :java.io.IOException ) =>{
            status = error
            errMessage = "Error in uploadFile at : " + e.getLocalizedMessage
			    	logger.error(errMessage + ": " + e.printStackTrace() )
            //throw new Exception("Error in fileProcessing at : " + e.getLocalizedMessage + ": " + e.printStackTrace())
          }
       }
      
      return returnProcCall( null, status, errMessage)
  } 
  
}