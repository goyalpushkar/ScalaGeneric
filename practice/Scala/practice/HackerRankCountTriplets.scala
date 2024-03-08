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
import scala.collection.immutable._
import scala.collection.mutable._
import scala.collection.concurrent._
import scala.collection.parallel.immutable._
import scala.collection.parallel.mutable._
import scala.concurrent._
import scala.io._
import scala.math._
import scala.sys._
import scala.util.matching._
import scala.reflect._

/*
 *  Count Triplets
You are given an array and you need to find number of tripets of indices at those indices are in geometric progression for a given common ratio
For example, . If , we have and .
Function Description
such that the elements and .
Complete the countTriplets function in the editor below. It should return the number of triplets forming a geometric progression for a given as an integer.
countTriplets has the following parameter(s): arr: an array of integers
r: an integer, the common ratio
Input Format
The first line contains two space-separated integers
The next line contains space-seperated integers .
Constraints
Output Format
Return the count of triplets that form a geometric progression.
Sample Input 0
42 1224
Sample Output 0
2
Explanation 0
There are triplets in satisfying our criteria, whose indices are
Sample Input 1
63
1 3 9 9 27 81
Sample Output 1
and the common ratio.
6
and , the size of
and
at indices
and

 Explanation 1
            The triplets satisfying are index
Sample Input 2
55
1 5 5 25 125
Sample Output 2
4
Explanation 2
The triplets satisfying are index
, ,
, ,
and .
                                        , ,
, .
                    
 */
class HackerRankCountTriplets {
  
}

object HackerRankCountTriplets {
  
      // Complete the countTriplets function below.
    def countTriplets(arr: Array[Long], r: Long): Long = {

        var numTriplets = 0
          
        for ( index <- arr.size - 1 to 1 by -1 ){
            
            var n = index - 1
            while( n >= 0 ){
               //println( "index - " +  index + " :n - "  + n)
               if ( arr.apply(index) / arr.apply( n ) == r ){
                  //breakable{
                    for( inner <- n -1 to 0 by -1 ) {
                       if( arr.apply(n) / arr.apply(inner) == r ){
                          numTriplets.+=(1)
                          //break
                       }
                    }
                  //}
               }
               n.-=(1)
               //println( numTriplets)
            }
           
        }
        return numTriplets
    }

    def main(args: Array[String]) {
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankCountTriplets.txt"));
        
        val nr = StdIn.readLine.replaceAll("\\s+$", "").split(" ")
        val n = nr(0).toInt
        val r = nr(1).toLong
        val arr = StdIn.readLine.replaceAll("\\s+$", "").split(" ").map(_.trim.toLong)
        //println( n + " " + r )
        //arr.foreach { print}
        val ans = countTriplets(arr, r)

        println(ans)
        //printWriter.println(ans)
        //printWriter.close()
    }
}