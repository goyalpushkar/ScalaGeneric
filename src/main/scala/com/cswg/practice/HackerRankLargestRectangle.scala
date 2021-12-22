package com.cswg.practice

/*
 *  Largest Rectangle
Skyline Real Estate Developers is planning to demolish a number of old, unoccupied buildings and construct a shopping mall in their place. Your task is to find the largest solid area in which the mall can be constructed.
There are a number of buildings in a certain two-dimensional landscape. Each building has a height, given by . If you join adjacent buildings, they will form a solid rectangle of area
.
                                                   . A rectangle of height and length can be
Complete the function largestRectangle int the editor below. It should return an integer representing the largest rectangle that can be formed within the bounds of consecutive buildings.
largestRectangle has the following parameter(s):
h: an array of integers representing building heights
Input Format
The first line contains , the number of buildings.
The second line contains space-separated integers, each representing the height of a building.
Constraints
Output Format
Print a long integer representing the maximum area of rectangle formed.
Sample Input
5 12345
Sample Output
9
Explanation
An illustration of the test case follows.
For example, the heights array
constructed within the boundaries. The area formed is .
                      Function Description
                         
                                                    1234
5
 * 
 */

import java.io._
object HackerRankLargestRectangle {
  
     // Complete the largestRectangle function below.
     def largestRectangle(h: Array[Int]): Long = {
         //It cannot be sorted as it will change order of buildings
        //val sortedArray = h.sortWith( _ < _ )
        //sortedArray.foreach( println )
        var minValue = 0
        var maxArea: Long = 0
        for ( index <- 0 to h.size - 1 ){
          
            if ( minValue > h.apply(index) )
               minValue = h.apply(index)

            val area: Long = minValue * ( index + 1 )
              //h.apply(index) * ( h.size - index )
            println( area + " " + maxArea )
            if ( maxArea < area ) 
               maxArea = area
        }

        return maxArea

    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream(fileLocation + "HackerRankLargestRectangle.txt"));

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val n = stdin.readLine.trim.toInt
        val h = stdin.readLine.split(" ").map(_.trim.toInt)
        val result = largestRectangle(h)
        println( result )
        //printWriter.println(result)
        //printWriter.close()
    }
}