package com.cswg.practice

import java.io._
import scala.collection.mutable.HashMap
object HackerRankMergeFile {
  
  import scala.io.Source
  
  val mergedFile = new HashMap[Int, ( String, String, Int ) ]()
  val duplicateCounter = new HashMap[Int, Int]()
  val duplicateFile =  new HashMap[Int,( String, String, Int )]()
  
  def readFile(fileName: String ) = {
      for ( lines <- Source.fromFile(fileName).getLines ) {
          val recordValues = lines.split(",");
          
          mergedFile.put( recordValues.apply(0).toInt, ( recordValues.apply(1), recordValues.apply(2), recordValues.apply(3).trim().toInt ) )
          duplicateCounter.put( recordValues.apply(0).toInt, duplicateCounter.getOrElse( recordValues.apply(0).toInt, 0 ) + 1 )
          
          //if ( duplicateCounter.getOrElse( recordValues.apply(0).toInt, 0) > 1 ) 
          //  duplicateFile.put( recordValues.apply(0).toInt, ( recordValues.apply(1), recordValues.apply(2), recordValues.apply(3).trim().toInt ) )
      }
  }
  
  def putDuplicates {
     
    //case(id, (firstName, lastName, grade) )
     mergedFile.foreach { f => 
          if ( duplicateCounter.getOrElse(f._1, 0 ) > 1 )
             duplicateFile.put( f._1, ( f._2._1, f._2._2, f._2._3 ) )
             
     }
  }
  
  def main( args: Array[String]) = {
    val stdin = scala.io.StdIn
    System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankMergeFiles.txt"));
     
    for( i <- 1 to 3 ){
      val fileName = stdin.readLine.trim
      readFile( fileName )
    }
    
    putDuplicates
    
    
    import java.io.FileWriter
    import java.io.BufferedWriter
    val fileWriterMerged = new File("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/mergedFile.txt");
    val writerMerged = new BufferedWriter(new FileWriter(fileWriterMerged) );
    val fileWriterDuplicate = new File("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/duplicateFile.txt");
    val writerDuplicate = new BufferedWriter(new FileWriter(fileWriterDuplicate) );
  
    mergedFile.foreach { f => writerMerged.write( f._1 + "," + f._2._1 + "," +  f._2._2 + "," +  f._2._3 )
                              writerMerged.newLine()
                   }
    duplicateFile.foreach { f => writerDuplicate.write( f._1 + "," + f._2._1 + "," +  f._2._2 + "," +  f._2._3 )
                              writerDuplicate.newLine()
                   }
    
    writerMerged.close();
    writerDuplicate.close();
    
  }
}