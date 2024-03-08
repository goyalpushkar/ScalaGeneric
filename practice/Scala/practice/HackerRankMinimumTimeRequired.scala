package com.cswg.practice

/*
 * Minimum Time
Required
You are planning production for an order. You have a number of machines that each have a fixed number
of days to produce an item. Given that all the machines operate simultaneously, determine the minimum
number of days to produce the required order.
For example, you have to produce items. You have three machines that take
days to produce an item. The following is a schedule of items produced:
Day Production Count
2 2 2
3 1 3
4 2 5
6 3 8
8 2 10
It takes days to produce items using these machines.
Function Description
Complete the minimumTime function in the editor below. It should return an integer representing the
minimum number of days required to complete the order.
minimumTime has the following parameter(s):
machines: an array of integers representing days to produce one item per machine
goal: an integer, the number of items required to complete the order
Input Format
The first line consist of two integers and , the size of and the target production.
The next line contains space-separated integers, .
Constraints
Output Format
Return the minimum time required to produce items considering all machines work simultaneously.
Sample Input 0
2 5
2 3
Sample Output 0
6
Explanation 0
In days can produce items and can produce items. This totals up to .
Sample Input 1
3 10
1 3 4
Sample Output 1
7
Explanation 1
In minutes, can produce items, can produce items and can
produce item, which totals up to .
Sample Input 2
3 12
4 5 6
Sample Output 2
20
Explanation 2
In days can produce items, can produce , and can produce
.
 */

import java.io._

object HackerRankMinimumTimeRequired {
  
     // Complete the minTime function below.
    def minTime(machines: Array[Long], goal: Long): Long = {

        var totalItems: Long = 0
        var totalTime: Long = machines.min
        
        while( totalItems < goal ){
              
             for( days <- machines ){
                   totalItems.+=( totalTime / days )
                   if ( totalItems >= goal )
                      return totalTime
             }
             
             totalTime.+=(1)
             System.out.println( totalTime + " -> " + totalItems )
             totalItems = 0
        }
        
        
        return totalTime
    }
    
    
    def minTimeP(machines: Array[Long], goal: Long): Long = {

        def getTotalItems( noOfDays: Long ): Long = {
            var totalItemsD: Long = 0
            for( days <- machines ){
                   totalItemsD.+=( noOfDays / days )
             }
            
             return totalItemsD
        }
        var noOfDay: Long = 0
        var lowerBound = ( goal * machines.min ) / machines.size
        var upperBound = ( goal * machines.max ) / machines.size
        
        while( lowerBound < upperBound ){
             var noOfDay = lowerBound/2 + upperBound/ 2
             val totalItems = getTotalItems( noOfDay )
             if ( totalItems < goal ) {
                lowerBound = noOfDay + 1
             }else{
               upperBound = noOfDay
             }
        }
        
        return lowerBound
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream( fileLocation + "HackerRankMimumTimeRequired.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val nGoal = stdin.readLine.split(" ")
        val n = nGoal(0).trim.toInt
        val goal = nGoal(1).trim.toLong
        System.out.println( goal )
        val machines = stdin.readLine.split(" ").map(_.trim.toLong)
        val ans = minTimeP(machines, goal)
        
        System.out.println( ans )

    }
    
}