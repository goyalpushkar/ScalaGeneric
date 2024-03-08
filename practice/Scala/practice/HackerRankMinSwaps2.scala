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

class HackerRankMinSwaps2 {
  
}

/*
 * You are given an unordered array consisting of consecutive integers  [1, 2, 3, ..., n] without any duplicates. You are allowed to swap any two elements. You need to find the minimum number of swaps required to sort the array in ascending order.

For example, given the array [7,1,3,2,4,5,6] we perform the following steps:

i   arr                         swap (indices)
0   [7, 1, 3, 2, 4, 5, 6]   swap (0,3)
1   [2, 1, 3, 7, 4, 5, 6]   swap (0,1)
2   [1, 2, 3, 7, 4, 5, 6]   swap (3,4)
3   [1, 2, 3, 4, 7, 5, 6]   swap (4,5)
4   [1, 2, 3, 4, 5, 7, 6]   swap (5,6)
5   [1, 2, 3, 4, 5, 6, 7]
It took 5 swaps to sort the array.

Function Description

Complete the function minimumSwaps in the editor below. It must return an integer representing the minimum number of swaps to sort the array.

minimumSwaps has the following parameter(s):

arr: an unordered array of integers
Input Format

The first line contains an integer,n , the size of arr. 
The second line contains n space-separated integers arr[i].

Constraints
1 <=n <= 10^5
1 <= arr[i] <= n

Output Format
Return the minimum number of swaps to sort the given array.
Sample Input 0
4
4 3 1 2
Sample Output 0
3

Explanation 0
Given array  arr [4,3,1,2]
After swapping  (0,2) we get arr [1,3,4,2] 
After swapping  (1,2) we get     [1,4,3,2]
After swapping  (1,3) we get     [1,2,3,4]
So, we need a minimum of 3 swaps to sort the array in ascending order.

Sample Input 1
5
2 3 4 1 5
Sample Output 1
3

Explanation 1

Given array  [2,3,4,1,5]
After swapping (2,3) we get  [2,3,1,4,5]
After swapping (0,1) we get  [3,2,1,4,5]
After swapping (0,2) we get [1,2,3,4,5]  
So, we need a minimum of 3 swaps to sort the array in ascending order.

Sample Input 2

7
1 3 5 2 4 6 7
Sample Output 2

3
Explanation 2

Given array  [1,3,5,2,4,6,7]
After swapping (1,3) we get  [1,2,5,3,4,6,7]
After swapping (2,3) we get  [1,2,3,5,4,6,7]
After swapping (3,4) we get [1,2,3,4,5,6,7]  
So, we need a minimum of 3 swaps to sort the array in ascending order.
 */
object HackerRankMinSwaps2 {
  
    // Complete the minimumSwaps function below.
    def minimumSwaps(arr: Array[Int]): Int = {

        var count = 0
        var index = 0
        
        def swap( minPos: Int, maxPos: Int )= {
            //println( "minPos - " + minPos + " \t" + " :maxPos - " + maxPos +  "\t" 
            //       + "arr.apply(minPos) - " + arr.apply(minPos) + "\t" + " :arr.apply(maxPos) - " + arr.apply(maxPos) )
            val temp = arr.apply(minPos)
            arr.update(minPos, arr.apply(maxPos))
            arr.update(maxPos, temp)
            //arr.foreach(println)
        }
       
        
        //for ( i <- 0 to arr.size - 1 ) {
        while( index < arr.size - 1 ){ 
          //println( "index - " + index  + " :arr.apply(index) - " + arr.apply(index) )  //+  " :i - " + i
          if ( !( arr.apply(index) == index + 1 ) ){
             swap( index, arr.apply(index) - 1 )
             count = count.+(1)
             index = if ( index.-(1) < 0 ) index else index.-(1)
          }else{
             index = index.+(1)  
          }
          //println( "index - "+ index + " :count -  " + count + "\n" )
        }
        //arr.foreach(println)
        return count
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        //val n = stdin.readLine.trim.toInt
        //val arr = stdin.readLine.split(" ").map(_.trim.toInt)

        val n = args(0).trim.toInt
        val arr = args(1).split(" ").map(_.trim.toInt)        
        val res = minimumSwaps(arr)

        println( res ) 
        //printWriter.println(res)
        //printWriter.close()
    }  
}