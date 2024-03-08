package com.cswg.practice

/*
 *  Max Min
You will be given a list of integers, , and a single integer . You must create an array of length from elements of such that its unfairness is minimized. Call that array . Unfairness of an array is calculated as
Where:
- max denotes the largest integer in - min denotes the smallest integer in
As an example, consider the array .
Testing for all pairs, the solution
with a
of . Pick any two elements, test
possible unfairness.
maxMin has the following parameter(s):
k: an integer, the length of the subarrays arr: an array of integers
Input Format
The first line contains an integer , the number of elements in array The second line contains an integer .
Each of the next lines contains an integer where
Constraints
Output Format
An integer that denotes the minimum possible value of unfairness.
Sample Input 0
  7
  3
  10
  100
  300
  200
  1000
  20
  30
Sample Output 0
. .
20
provides the minimum unfairness of .
Note: Integers in may not be unique.
Function Description
Complete the function maxMin in the editor below. It must return the integer representing the minimum

  Explanation 0
Here ; selecting the integers
max(10,20,30) - min(10,20,30) = 30 - 10 = 20
, unfairness equals
             Sample Input 1
  10
  4
  1
  2
  3
  4
  10
  20
  30
  40
  100
  200
Sample Output 1
3
Explanation 1
Here ; selecting the
integers
, unfairness equals
              max(1,2,3,4) - min(1,2,3,4) = 4 - 1 = 3
Sample Input 2
5 2 1 2 1 2 1
Sample Output 2
0
Explanation 2
Here . or
give the minimum unfairness of .
                              
 */

import java.io._
object HackerRankMaxMin {
      // Complete the maxMin function below.
    def maxMin(k: Int, arr: Array[Int]): Int = {

        val newArr = arr.sortWith( _ < _ )
        var minDiff = newArr.apply(k-1) - newArr.apply(0)
        for( index <- 0 to newArr.size - k) {
            val diff = newArr.apply(k-1+index) - newArr.apply(index)
            if ( minDiff > diff )
               minDiff = diff 
        }
        
        return minDiff

    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream( fileLocation + "HackerRankMaxMin.txt"));
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val n = stdin.readLine.trim.toInt
        val k = stdin.readLine.trim.toInt
        val arr = Array.ofDim[Int](n)

        for (i <- 0 until n) {
            val arrItem = stdin.readLine.trim.toInt
            arr(i) = arrItem}

        val result = maxMin(k, arr)
        println( result )
        //printWriter.println(result)
        //printWriter.close()
    }
}