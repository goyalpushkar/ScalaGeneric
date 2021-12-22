package com.cswg.testing

object Examples1 {
  println("Welcome to the Scala worksheet")
  
  def main(args: Array[String]) {
  //while.scala
		var i = 0
		while( i < args.length){
		   if ( i != 0 )
		      print(" ")
		   print( args(i) )
		   i += 1
		}
		print()
		
		//foreach.scala
		args.foreach( arg => println(arg) )
		args.foreach(println)
		
		val numdec = 12;
  }
}