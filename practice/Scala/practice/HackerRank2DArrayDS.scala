package com.cswg.practice

import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._

class HackerRank2DArrayDS {
  
}

object HackerRank2DArrayDS {
  
    // Complete the hourglassSum function below.
    def hourglassSum(arr: Array[Array[Int]]): Int = {

        def getSum( row: Int, col: Int ): Int = {
            var sum = 0
            println( "row - " + row + "\t" + " :col - " + col )
            for ( i <- row until row + 3 )
            {
               for ( j <- col until col + 3 ){
                  if ( ( i == (row + 1) ) && ( j == col || j == col + 2 ) ){
                    sum = sum + 0
                  }else{
                    sum = sum + arr.apply(i).apply(j)
                  }
               }
                
            }
            println( "sum - " + sum )
            return sum
        }
        
        var maxValue = -1000
        for ( r <- 0 to ( arr.length - 3 ) ){
            for ( j <- 0 to ( arr.apply(r).length - 3 ) ){
                val hourGlassSum = getSum( r, j )
                if ( maxValue < hourGlassSum ) 
                    maxValue = hourGlassSum 
                    
                println( "maxValue - " +  maxValue  + "\n" )
            }
        }
        return maxValue

    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val arr = Array.ofDim[Int](6, 6)

        for (i <- 0 until 6) {
            //arr(i) = stdin.readLine.split(" ").map(_.trim.toInt)
            arr(i) = args.apply(i).split(" ").map(_.trim.toInt)
        }
        /*for ( i <- 0 to arr.length - 1 )
          for ( j <- 0 to arr.apply(i).length - 1 ){
              println( " ["+i +"]["+j+"] - " + arr.apply(i).apply(j) )
          }
        */
        //println( "arr.length - " + arr.length + "\t" + " :arr.apply(0).length - " + arr.apply(0).length)

        val result = hourglassSum(arr)

        //printWriter.println(result)
        //printWriter.close()
    }
}
