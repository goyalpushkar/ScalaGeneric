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
 * Merge Sort: Counting
Inversions
In an array, , the elements at indices and (where ) form an inversion if . In
other words, inverted elements and are considered to be "out of order". To correct an
inversion, we can swap adjacent elements.
For example, consider the dataset . It has two inversions: and . To sort the
array, we must perform the following two swaps to correct the inversions:
Given datasets, print the number of inversions that must be swapped to sort each dataset on a new
line.
Function Description
Complete the function countInversions in the editor below. It must return an integer representing the
number of inversions required to sort the array.
countInversions has the following parameter(s):
arr: an array of integers to sort .
Input Format
The first line contains an integer, , the number of datasets.
Each of the next pairs of lines is as follows:
1. The first line contains an integer, , the number of elements in .
2. The second line contains space-separated integers, .
Constraints
Output Format
For each of the datasets, return the number of inversions that must be swapped to sort the dataset.
Sample Input
2
5
1 1 1 2 2
5
2 1 3 1 2
Sample Output
0
4
Explanation
We sort the following datasets:
1. is already sorted, so there are no inversions for us to correct. Thus, we print on
a new line.
2.
We performed a total of swaps to correct inversions.
 */
class HackerRankCountingInversion {
  
}

object HackerRankCountingInversion{
  
    
    def merge( arr: Array[Int], low: Int, mid: Int, high: Int  ): Long = {
       /*
        *   
        */
        //println( "merge - " + " :low - " + low + " :mid - " + mid + " :high - " + high )
        var start = low
        var secondStart = mid + 1
        for( index <- low to high ){
           tempArr.update(index, arr.apply(index))
        }
        //tempArr.foreach(println)
        //var numberOfInversions = 0
        for( index <- low to high ){
            if ( start > mid ){
              arr.update( index, tempArr.apply(secondStart) )
              secondStart.+=(1)
            }
            else if ( secondStart > high ){
              arr.update( index, tempArr.apply(start) )
              start.+=(1)
            }
            else if ( tempArr.apply(start) > tempArr.apply(secondStart) ){
              arr.update( index, tempArr.apply(secondStart) )
              secondStart.+=(1)
              numberOfInversions.+=( mid + 1 - start )
            }
            else{
              arr.update( index, tempArr.apply(start) )
              start.+=(1)  
            }
            
        }
        //println( numberOfInversions)
        return numberOfInversions
    }
    
    def sort( arr: Array[Int], low: Int, high: Int ): Long = {
      
          //println( "sort - " + numberOfInversions )
          if ( low >= high ) 
             return 0
          
          val mid = low + ( high - low ) / 2
          sort( arr, low, mid ) 
          sort( arr, mid + 1, high ) 
          merge( arr, low, mid, high )
          
          return numberOfInversions
    }
    
    var tempArr: Array[Int] = Array()
    var numberOfInversions: Long = 0
    // Complete the countInversions function below.
    def countInversions(arr: Array[Int]): Long = {
        numberOfInversions = 0
        tempArr = Array.fill(arr.size)(0)
        numberOfInversions = sort( arr, 0, arr.size - 1 )
        /*arr.foreach(print)
        println("\n")
        tempArr.foreach(print)
        println("\n")*/
        return numberOfInversions
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankCountingInversions.txt"));
        ///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankCountingInversions.txt
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val t = stdin.readLine.trim.toInt

        for (tItr <- 1 to t) {
            val n = stdin.readLine.trim.toInt

            val arr = stdin.readLine.split(" ").map(_.trim.toInt)
            val result = countInversions(arr)
            println( result )
            //printWriter.println(result)
        }

        //printWriter.close()
    }
    
}