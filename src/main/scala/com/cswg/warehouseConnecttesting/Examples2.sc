package com.cswg.testing

import scala.io.Source
   
object Examples2 {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  def widthOfLength(s : String) = s.length.toString.length
                                                  //> widthOfLength: (s: String)Int
  
  def main(args: Array[String]) {
		  if ( args.length > 0 ){
		     val lines = Source.fromFile(args(0)).getLines.toList
		     
		     val longestLine = lines.reduceLeft( (a, b) => if ( a.length > b.length ) a else b )
		       
		     val maxWidth = widthOfLength(longestLine)
		     
		     for (line <- lines) {
		        val numSpaces = maxWidth - widthOfLength(line)
		        val padding = " " * numSpaces
		        print( padding + line.length + " | " + line )
		     }
		  }
		  else
		     println("Please enter File Name")
	}                                         //> main: (args: Array[String])Unit
}