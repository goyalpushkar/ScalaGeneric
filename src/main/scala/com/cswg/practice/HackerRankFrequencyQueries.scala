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
 *  Frequency Queries
You are given queries. Each query is of the form two integers described below:
- - -
: Insert x in your data structure.
: Delete one occurence of y from your data structure, if present.
: Check if any integer is present whose frequency is exactly . If yes, print 1 else 0.
The queries are given in the form of a 2-D array of size where contains the
operation, and
Operation Array Output
contains the data element. For example, you are given array
. The results of each operation are:
(1,1) (2,2) (3,2) (1,1) (1,1) (2,1) (3,2)
[1] [1]
0 [1,1]
[1,1,1] [1,1]
1
Return an array with the output: .
Function Description
Complete the solve function in the editor below. It must return an array of integers where each element is a if there is at least one element value with the queried number of occurrences in the current array, or 0 if there is not.
solve has the following parameter(s): queries: a 2-d array of integers
Input Format
The first line contains of an integer , the number of queries.
Each of the next lines contains two integers denoting the 2-d array
Constraints
All
Output Format
Return an integer array consisting of all the outputs of queries of type
Sample Input 0
8 15 16 32 1 10 1 10 16 25 32
.
.

 Sample Output 0
 0 1
Explanation 0
For the first query of type , there is no integer whose frequency is For the second query of type , there are two integers in
(integers = and ). So, the answer is .
Sample Input 1
4
34
2 1003 1 16 31
Sample Output 1
0 1
Explanation 1
(
). So answer is . whose frequency is
                                         For the first query of type For the second query of type
Sample Input 2
10 13 23 32 14 15 15 14 32 24 32
Sample Output 2
0 1 1
Explanation 2
, there is one integer,
of frequency
.
so the answer is .
, there is no integer of frequency
. The answer is
          When the first output query is run, the array is empty. We insert two
output query, so there are two instances of elements occurring twice. We delete a and run the same query. Now only the instances of satisfy the query.
's and two
's before the second
                 
 */
class HackerRankFrequencyQueries {
  
}

object HackerRankFrequencyQueries {
  
    import scala.util.control.Breaks._
    import scala.collection.mutable.HashMap
    import scala.collection.mutable.HashSet
    val valuesCount: HashMap[Int, Int] = HashMap[Int, Int]()
    val counts: HashMap[Int, Int] = HashMap[Int, Int]()
    
    def addValueCounts( value: Int ) = {
        counts.+=( value -> ( counts.getOrElse( value, 0 ) + 1 ) )
    }
    
    def removeValueCounts( value: Int ) = {
      
        if ( counts.getOrElse(value, 0) - 1 > 0){
               counts.+=( value -> ( counts.getOrElse( value, 0 ) - 1 ) )
           }else{
               counts.-=( value ) 
        }
        
    }
    
    def addValue( value: Int ) = {
        val countValue = valuesCount.getOrElse(value, 0)
        
        valuesCount.+=( value -> ( countValue + 1 ) )
        
        //Add new Value in Counts Map
        addValueCounts( countValue + 1 )
        
        //Decrease Count or remove old value frmo Counts Map
        removeValueCounts( countValue )
    }
    
    def removeValue( value: Int ) = {
        //if ( valuesCount.contains(value) ){
         val countValue = valuesCount.getOrElse(value, 0)
         
         if ( countValue - 1 > 0 ){
           valuesCount.+=( value -> ( countValue - 1 ) )
         }else{
           valuesCount.-=(value)
         }
         
         //Add new Value in Counts Map
         if ( ( countValue - 1 ) > 0 ){
            addValueCounts( countValue - 1 )
         }
         
         //Decrease Count or remove old value from Counts Map
         removeValueCounts( countValue )
           
        //}
    }
    
    def checkCount( value: Int ): Int = {
        
        if ( counts.contains(value) )
           return 1
        else 
           return 0
    }
     // Complete the freqQuery function below.
    def freqQuery(queries: Array[Array[Int]]): Array[Int] = {
         //var returnArray = Array[Int]()
         val returnArray = ArrayBuffer[Int]()
         for( row <- 0 to queries.length -1 ) {
             val key = queries.apply(row).apply(0)
             val value = queries.apply(row).apply(1)
             if ( key == 1 ){
                addValue( value ) 
             }else if ( key == 2 ){
                removeValue( value )
             }else if ( key == 3 ){
                val check = checkCount ( value ) 
                returnArray.+=( check )
                //returnArray = returnArray.:+(check)
             }
            
         }
         return returnArray.toArray
    }

    def main(args: Array[String]) {
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankFrequencyQueries.txt"));
        
        val q = StdIn.readLine.trim.toInt
        val queries = Array.ofDim[Int](q, 2)

        for (i <- 0 until q) {
            queries(i) = StdIn.readLine.replaceAll("\\s+$", "").split(" ").map(_.trim.toInt)
        }

        //println( "q - "  + q )
        //queries.foreach( f => print( f.apply(0) ) )
        val ans = freqQuery(queries)
        //ans.foreach { println }
        println(ans.mkString("\n"))
        //printWriter.println(ans.mkString("\n"))
        //printWriter.close()
    }
}