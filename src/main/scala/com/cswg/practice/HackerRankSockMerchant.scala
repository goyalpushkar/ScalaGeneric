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

class HackerRankSockMerchant {
  
}

/*
 * John works at a clothing store. He has a large pile of socks that he must pair by color for sale. Given an array of integers representing the color of each sock, determine how many pairs of socks with matching colors there are.

For example, there are  socks with colors . There is one pair of color  and one of color . There are three odd socks left, one of each color. The number of pairs is .

Function Description

Complete the sockMerchant function in the editor below. It must return an integer representing the number of matching pairs of socks that are available.

sockMerchant has the following parameter(s):

n: the number of socks in the pile
ar: the colors of each sock
Input Format

The first line contains an integer , the number of socks represented in . 
The second line contains  space-separated integers describing the colors  of the socks in the pile.

Constraints

 where 
Output Format

Print the total number of matching pairs of socks that John can sell.
 */
object HackerRankSockMerchant {

    // Complete the sockMerchant function below.
    def sockMerchant(n: Int, ar: Array[Int]): Int = {
        //println( "n - " + n )
        import scala.collection.immutable.Stack
        var stackOfSocks = Stack[Int]()
        var numberOfPairs = 0
        val sortedArray = ar.sortWith( _ < _ )
        for( f <- sortedArray ){  //f => 
            println( "f - " + f )
            var poped = -99
            if ( stackOfSocks.isEmpty ){
                poped = -99
            }else{
                poped = stackOfSocks.pop2._1
                stackOfSocks = stackOfSocks.pop2._2
            }
            if ( f == poped ){
               println( "inside if")
               numberOfPairs = numberOfPairs + 1
            }else{
              println( "inside else")
              stackOfSocks = stackOfSocks.push(f)
            }
            println( "numberOfPairs - " + numberOfPairs + "\t" + stackOfSocks.size ) 
        }
       
        return numberOfPairs
    }

    def main(args: Array[String]) {
        println( "Inside main" )
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        //val n = stdin.readLine.trim.toInt
        //val ar = stdin.readLine.split(" ").map(_.trim.toInt)
        val n = args(0).trim.toInt
        val ar = args(1).split(" ").map(_.trim.toInt)
        
        /*println( "n - " + n + "\n" 
               + "ar - "  + ar.size )*/
        val result = sockMerchant(n, ar)
        
        println( "result - " + result ) 
        //printWriter.println(result)

        //printWriter.close()
    }
}
