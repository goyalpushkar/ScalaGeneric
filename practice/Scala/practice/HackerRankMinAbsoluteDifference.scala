package com.cswg.practice

/*
 *  Minimum Absolute Difference in an Array
Consider an array of integers, . We define the absolute difference between two elements, and (where ), to be the absolute value of .
Given an array of integers, find and print the minimum absolute difference between any two elements in
the array. For example, given the array we can create and . The absolute differences for these pairs are
pairs of numbers: ,
and . The minimum absolute difference is
Input Format
The first line contains a single integer , the size of The second line contains space-separated integers
Constraints
Output Format
.
.
.
Print the minimum absolute difference between any two elements in the array.
Sample Input 0
3
3 -7 0
Sample Output 0
3
Explanation 0
With integers in our array, we have three possible pairs: values of the differences between these pairs are as follows:
, , and
. The absolute
Notice that if we were to switch the order of the numbers in these pairs, the resulting absolute values would still be the same. The smallest of these possible absolute differences is .
Sample Input 1
10
-59 -36 -13 1 -53 -92 -2 -96 -54 75
Sample Output 1

  1
Explanation 1
The smallest absolute difference is
Sample Input 2
5
1 -3 71 68 17
Sample Output 2
3
Explanation 2
The minimum absolute difference is
.
               .
       
 */
object HackerRankMinAbsoluteDifference {
  
   // Complete the minimumAbsoluteDifference function below.
    def minimumAbsoluteDifferenceIE(arr: Array[Int]): Int = {
        
        var minDiff = Math.abs( arr.max )
        for( first <- 0 until arr.size ) {
           for(  second <- first + 1 until arr.size ){
               val diff = Math.abs( arr.apply(first) - arr.apply(second) )
               if ( minDiff > diff )
                  minDiff = diff
           }
         
        }

        return minDiff
    }

    def minimumAbsoluteDifference(arr: Array[Int]): Int = {
        val newArray = arr.sortWith( _ < _ )
        var minDiff = Math.abs( newArray.apply( newArray.size -1 ) )
        for( first <- 0 until arr.size - 1 ) {
            val diff = Math.abs( newArray.apply(first) - newArray.apply(first + 1) )
               if ( minDiff > diff )
                  minDiff = diff
        }
        return minDiff
    }
        
    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val n = stdin.readLine.trim.toInt
        val arr = stdin.readLine.split(" ").map(_.trim.toInt)
        println( n )
        val result = minimumAbsoluteDifference(arr)
       
        println( result )
        //printWriter.println(result)
        //printWriter.close()
    }  
}