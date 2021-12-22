package com.cswg.testing

import scala.io.Source.{ fromFile, fromString }
import java.io.File
import org.slf4j.LoggerFactory
import scala.sys.process._
import java.net.URL

class FileHandling {
  
}

object FileHandling {
    val logger = LoggerFactory.getLogger("Examples");
    var propertiesFilePath = "/opt/cassandra/default/Properties/"
    //var propertiesFilePathstring = "opt\cassandra\default\Properties\"
    
    def main(args: Array[String]) {
      logger.debug("Main for FileHandling")   
      
      //fileStringReader
      //listFiles(propertiesFilePath)
      playSound
      
      logger.debug("Finished") 
    }
    
    def fileStringReader {
        //Read from File
      val fileName = propertiesFilePath + "environment.conf"
      val getFile = fromFile(fileName)
      logger.debug("Read File - " + fileName )
      for ( lines <- getFile.getLines() ) {
          logger.debug(lines)
      }
      
      //logger.debug( "String - " + "\n" + getFile.getLines().mkString)
      //logger.debug( "String - " + "\n" + getFile)
      
      getFile.close()
      
      //Read from String
      val getString = fromString(fileName)
      logger.debug("Read String - " + fileName )
      for ( lines <- getString.getLines() ) {
          logger.debug(lines)
      }
    }
    
    def listFiles(directoryName: String) = {
        logger.debug( "Directory Name - " + propertiesFilePath )
        val dir = new File(directoryName)
        var listOfFiles = null: Option[List[File]];
        if ( dir.exists() && dir.isDirectory() ){
           listOfFiles = Some(dir.listFiles().filter { _.isFile() }.toList)
        }else{
           logger.debug( "Directory does not exists or provided name is not Directory" )
        }
        
        listOfFiles.get.foreach { x => println( x + " - " + x.getName.endsWith("conf") ) }
    }
    
    def playSound() = {
        val filePath = "C:/Pushkar/Songs"
        val dir = new File(filePath)
        logger.debug( " dir - " +  dir ) 
        if ( dir.exists() && dir.isDirectory() ){
           for ( internalDir <- dir.listFiles().filter( _.isDirectory() ).toList ){
               logger.debug( " ************************************************* "  ) 
               logger.debug( " internalDir - " +  internalDir ) 
               for ( internalFiles <- internalDir.listFiles().filter{ _.isFile() }.toList ) {
                 logger.debug( " --------------------------- "  )   
                 logger.debug( " internalFiles - " +  internalFiles ) 
                   if ( internalFiles.getName.endsWith("mp3") ) {
                       val cmd = "start wmplayer " + "\"" + internalFiles + "\""
                       logger.debug( " cmd - " +  cmd ) 
                       //ProcessBuilder cmdProcess = new ProcessBuilder("start","wmplayer","\"", internalFiles, "\"")
                       try{ 
                         /*val stdout = new StringBuilder
                         val stderr = new StringBuilder
                         val status = Seq(cmd) ! ProcessLogger( stdout append _, stderr append _ )
                         logger.debug( " status - " + status )
                         logger.debug( " stdout - " + stdout )
                         logger.debug( " stderr - " + stderr )*/
                         val status = Seq("/bin/sh", "-c", cmd ).!!
                         //cmd.toString().lines_!
                         //cmd.toString().!!
                         //val process = Process(cmd).lines
                         //logger.debug( " process - " + process )
                       }catch {
                         case e @ (_ :Exception | _ :java.io.IOException ) => 
                           logger.debug( "Exception occurred while running command " + e.getMessage )
                       }
                   }
               }
           }
        }
    }
}