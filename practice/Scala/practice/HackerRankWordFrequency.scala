package com.cswg.practice

import scala.io.BufferedSource
import java.io.FileInputStream

object HackerRankWordFrequency {
  
  import scala.collection.mutable.HashMap  
  val wordFrequency = HashMap[String, Int]()
  
  def calcWordFreq( reader: BufferedSource ): String = {  //reader: BufferedSource
       
      val words = reader.getLines().flatMap( x => x.split(" ") )
      for ( word <- words ) {
         wordFrequency.put(  word, wordFrequency.getOrElse(word, 0) + 1 )
      }

      return ""
  }
  
  def main( args: Array[String]) = {
       val stdin = scala.io.StdIn
       System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankWordFrequency.txt"));
       var line = stdin.readLine()
       while( line != null ){
         //System.out.println( line )
         val words = line.trim().split(" ")
         for ( word <- words ) {
             wordFrequency.put(  word, wordFrequency.getOrElse(word, 0) + 1 )
          } 
         line = stdin.readLine()
       }  
       wordFrequency.foreach(f => println( f._1 + ":" + f._2 ) )
       
  }
  
}