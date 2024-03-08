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

/*
 * Starting with a 1-indexed array of zeros and a list of operations, for each operation add a value to each of the array element between two given indices, inclusive. Once all operations have been performed, return the maximum value in your array.

For example, the length of your array of zeros n=10. Your list of queries is as follows:

    a b k
    1 5 3
    4 8 7
    6 9 1
Add the values of k between the indices a and b inclusive:

index->	 1 2 3  4  5 6 7 8 9 10
	[0,0,0, 0, 0,0,0,0,0, 0]
	[3,3,3, 3, 3,0,0,0,0, 0]
	[3,3,3,10,10,7,7,7,0, 0]
	[3,3,3,10,10,8,8,8,1, 0]
The largest value is 10 after all operations are performed.

Function Description

Complete the function arrayManipulation in the editor below. It must return an integer, the maximum value in the resulting array.

arrayManipulation has the following parameters:

n - the number of elements in your array
queries - a two dimensional array of queries where each queries[i] contains three integers, a, b, and k.
Input Format

The first line contains two space-separated integers n and m, the size of the array and the number of operations. 
Each of the next m lines contains three space-separated integers a, b and k, the left index, right index and summand.

Constraints
3 <= n <= 10^7
1 <= m <= 2*10^5
1 <= a,b <= n
0 <= k <= 10^9

Output Format

Return the integer maximum value in the finished array.

Sample Input

5 3
1 2 100
2 5 100
3 4 100
Sample Output

200
Explanation

After the first update list will be 100 100 0 0 0. 
After the second update list will be 100 200 100 100 100. 
After the third update list will be 100 200 200 200 100. 
The required answer will be 200.
 */
class HackerRankArrayManipulation {
  
}

object HackerRankArrayManipulation {

      def arrayManipulationEfficient(n: Int, queries: Array[Array[Int]]): Long = {

        val newArray: Array[Long] =  Array.fill(n)(0)
        var maxValue: Long = 0
        
        queries.foreach { x => newArray.update( x.apply(0) - 1, newArray.apply( x.apply(0) - 1 ) + x.apply(2) ) 
                               if ( x.apply(1) < n )
                                  newArray.update( x.apply(1), newArray.apply( x.apply(1) ) - x.apply(2) )
                         }
        
        var sum: Long = 0
        newArray.foreach{ f => sum.+=(f)
                               if ( maxValue < sum )
                                  maxValue = sum
                         }
        return maxValue
    }
      
     // Complete the arrayManipulation function below.
    def arrayManipulationRetry(n: Int, queries: Array[Array[Int]]): Long = {

        val newArray: Array[Long] =  Array.fill(n)(0)
        var maxValue: Long = 0
        
        queries.foreach { x => for ( i <- x.apply(0) -1 to x.apply(1) - 1 ){
                                   newArray.update( i, newArray.apply(i) + x.apply(2) ) 
                                   if ( maxValue < newArray.apply(i) )
                                       maxValue = newArray.apply(i)
                               }
                         }
        
        
        return maxValue
    }
    
    // Complete the arrayManipulation function below.
    def arrayManipulation(n: Int, queries: Array[Array[Int]]): Long = {
        var maxValue: Long = 0

        import scala.collection.mutable.Map
        val indexValues:Map[Int, Int] = Map[Int, Int]()
        val newArray: Array[Long] =  Array.fill(n)(0)
        //indexValues.foreach(f => println( f._1 + " -> " + f._2 ) )
        for ( i <- 0 until queries.size ) {
            for ( j <- queries.apply(i).apply(0) - 1 to queries.apply(i).apply(1) - 1 ) {
                /*println( "j - " + j + " :0 - " + queries.apply(i).apply(0) + "\t" + " :1 - " + queries.apply(i).apply(1) + "\n" 
                      + "indexValues.getOrElse(j, 0) - " + indexValues.getOrElse(j, 0) )*/
                val value = queries.apply(i).apply(2) 
                if ( maxValue < ( newArray.apply(j) + value) ){
                    maxValue = ( newArray.apply(j) + value )
                }
                newArray.update(j, ( newArray.apply(j) + value ))
                
                /*if ( maxValue < ( indexValues.getOrElse(j, 0) + queries.apply(i).apply(2) ) ){
                      maxValue = ( indexValues.getOrElse(j, 0) + queries.apply(i).apply(2) )
                }
                indexValues.++=( Map(j -> ( indexValues.getOrElse(j, 0) + queries.apply(i).apply(2) ) ) )*/
                //println( ( indexValues.getOrElse(j, 0) + queries.apply(i).apply(2) ) + " -> " + maxValue)   
            }
            println( maxValue)
        }
        
        return maxValue;
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream( fileLocation + "HackerRankArrayManipulation.txt"));
        ///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankArrayManipulation.txt
        //System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankArrayManipulation.txt"));
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val nm = stdin.readLine.split(" ")

        val n = nm(0).trim.toInt
        val m = nm(1).trim.toInt
        val queries = Array.ofDim[Int](m, 3)
        println( n + " " + m )
        /*val n = args(0).trim.toInt
        val m = args(1).trim.toInt
        val queries = Array.ofDim[Int](m, 3)
        */
        for (i <- 0 until m) {
            queries(i) = stdin.readLine.split(" ").map(_.trim.toInt)
            //queries(i) = args(i+2).split(" ").map(_.trim.toInt)
        }

        //queries.foreach(f => println( f.apply(0) ) )
        val result = arrayManipulationEfficient(n, queries) 
        //arrayManipulation(n, queries)
        println( " Final - " + result )
        //printWriter.println(result)
        //printWriter.close()
    }  
}