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

class HackerRankFraudulentActivity {
  
}

//Bubble Sort Implementation

/*
HackerLand National Bank has a simple policy for warning clients about possible fraudulent account activity. If the amount spent 
by a client on a particular day is greater than or equal to 2 X the client's median spending for a trailing number of days, 
they send the client a notification about potential fraud. The bank doesn't send the client any notifications until they have at 
least that trailing number of prior days' transaction data.

Given the number of trailing days d and a client's total daily expenditures for a period of n days, find and print the number of times 
the client will receive a notification over all n days.

For example, d=3 and expenditures = [10,20,30,40,50]. On the first three days, they just collect spending data. At day 4, we have 
trailing expenditures of [10,20,30]. The median is 20 and the day's expenditure is 40. Because 40 >= 2 X 20, there will be a 
notice. The next day, our trailing expenditures are [20,30,40] and the expenditures are 50. This is less than 2 X 30 
so no notice will be sent. Over the period, there was one notice sent.

Note: The median of a list of numbers can be found by arranging all the numbers from smallest to greatest. If there is an odd number 
of numbers, the middle one is picked. If there is an even number of numbers, median is then defined to be the average of the two middle values. (Wikipedia)

Function Description
Complete the function activityNotifications in the editor below. It must return an integer representing the number of client notifications.
activityNotifications has the following parameter(s):
expenditure: an array of integers representing daily expenditures
d: an integer, the lookback days for median spending

Input Format
The first line contains two space-separated integers  and , the number of days of transaction data, and the number of trailing days' data used to calculate median spending. 
The second line contains  space-separated non-negative integers where each integer  denotes .
Constraints
1 <= n < 2 X 10^s
1 <= d <= n
0 <= expenditure[i] <= 200

Output Format
Print an integer denoting the total number of times the client receives a notification over a period of  days.

Sample Input 0
9 5
2 3 4 2 3 6 8 4 5

Sample Output 0
2

Explanation 0
We must determine the total number of notifications the client receives over a period of n=9 days. For the first five days, 
the customer receives no notifications because the bank has insufficient transaction data: .
On the sixth day, the bank has d=5 days of prior transaction data, {2,3,4,2,3}, and median = 3  dollars. The client spends 6 dollars, 
which triggers a notification because 6 >= 2 X median: notifications = 0 + 1 + 1.
On the seventh day, the bank has d=5 days of prior transaction data, {3,4,2,3,6}, and median = 3 dollars. The client spends 8 dollars, 
which triggers a notification because 8 >= 2 X median: notifications =  1 + 1 = 2.
On the eighth day, the bank has d=5 days of prior transaction data, {4,2,3,6,8}, and median = 4 dollars. The client spends 4 dollars, 
which does not trigger a notification because 4 < 2 X median: notifications = 2.
On the ninth day, the bank has d=5 days of prior transaction data, {2,3,6,8,4}, and a transaction median of 4 dollars. The client spends 5 dollars, 
which does not trigger a notification because 5 < 2 X median: notifications = 2.
Sample Input 1
5 4
1 2 3 4 4
Sample Output 1
0
There are 4 days of data required so the first day a notice might go out is day 5. Our trailing expenditures are {1,2,3,4} with a median of 2.5
 The client spends 4 which is less than 2 X 2.5 so no notification is sent.
 */

object HackerRankFraudulentActivity {
  
   
   // Complete the activityNotifications function below.
    def activityNotifications(expenditure: Array[Int], d: Int): Int = {
    
        var noOfNotifications = 0
        
        //Quick Sort Implementation
        def quickSort( sortedArray: Array[Int], startIndex: Int, endIndex: Int ): Null = {
            if ( startIndex >= endIndex )
                return null
               
            val pivotValue = sortedArray.apply( (startIndex / 2) + (endIndex / 2) )
            
            //println( startIndex + " -- " + endIndex + " --> " + pivotValue)
            val pivotIndex = partition(sortedArray, startIndex, endIndex, pivotValue)
            quickSort( sortedArray, startIndex, pivotIndex - 1 )
            quickSort( sortedArray, pivotIndex, endIndex )

        }
        
        def exchange( sortedArray: Array[Int], firstIndex: Int, nextIndex: Int ) = {
             
            val temp = sortedArray.apply(firstIndex)
            sortedArray.update(firstIndex, sortedArray.apply(nextIndex))
            sortedArray.update(nextIndex, temp)
        }
        
        def partition( sortedArray: Array[Int], startIndex: Int, endIndex: Int, pivot: Int ): Int = {
            
            var leftSide = startIndex
            var rightSide = endIndex
            
            while( leftSide <= rightSide ){
              while( sortedArray.apply(leftSide) < pivot )
                 leftSide = leftSide.+(1)
  
              while ( sortedArray.apply(rightSide) > pivot )
                 rightSide = rightSide.-(1)               
               
              if ( leftSide <= rightSide ){
                 exchange( sortedArray, leftSide, rightSide)
                 leftSide = leftSide.+(1)
                 rightSide = rightSide.-(1)    
              }
            }
            return leftSide
        }
        
        def getMedian( startIndex: Int, endIndex: Int): Double = {  //sortedValues: Array[Int],            
           //quickSort( sortedValues, 0, d -1 )
           quickSort( expenditure, startIndex, endIndex )
           //sortedValues.foreach(print)
           //println(" ")
           //expenditure.foreach(f => print( f + "\t" ) )
           val medianIndex = startIndex/2.0 + endIndex/2.0
           //println(" ")
           //println( "medianIndex - " + medianIndex + " Math.floor( medianIndex ).toInt  - " + Math.floor( medianIndex ).toInt  + " : Math.ceil( medianIndex ).toInt - " + Math.ceil( medianIndex ).toInt )
           
           if ( d % 2 == 0 ){
              //println ( ( ( sortedValues.apply( d/2 ) / 2 ) ) )
              return ( ( expenditure.apply( Math.floor( medianIndex ).toInt ) / 2.0 ) + ( expenditure.apply( Math.ceil( medianIndex ).toInt ) / 2.0 ) )
           }else{
              return ( expenditure.apply( medianIndex.toInt ) ) 
           }
        }        
        //Quick Sort Implementation Ends
        
        //Priority Queue Implementation
        //case class Elem(var priority: Int, i: Int)
        import scala.collection.mutable.PriorityQueue
        import scala.math.Ordering
        def newOrdering = new Ordering[Int] {
            def compare( a: Int, b: Int ) = -1 * a.compare(b)         //a.priority.compare( b.priority )
        }
        
        val lowerQueue = PriorityQueue[Int]()
        val upperQueue = PriorityQueue[Int]()(newOrdering)
        
        def push (value: Int) = {
            if ( upperQueue.isEmpty )
               upperQueue.+=(value)
               
            if ( value >=  upperQueue.head )
               upperQueue.+=(value)
           else lowerQueue.+=(value)
        }
        
        def rebalance = {
            if ( ( upperQueue.size - lowerQueue.size ) >= 2 ){
                lowerQueue.+=( upperQueue.dequeue() )
            }else if ( ( lowerQueue.size - upperQueue.size ) >= 2 ) {
                upperQueue.+=( lowerQueue.dequeue() )
            }
        }
        
        def getMedianP: Double = {
            if ( upperQueue.size > lowerQueue.size ){
               return upperQueue.head
            }else if ( lowerQueue.size > upperQueue.size ){
               return lowerQueue.head
            }else{
               return ( ( upperQueue.head / 2.0 ) + ( lowerQueue.head / 2.0 ) )
            }
        }
        //Priority Queue Implementation Ends
        
        //CountSort Implementation Starts
        import scala.collection.mutable.Queue
        import scala.util.control.Breaks._
        val countArray = Array.fill(201)(0)
        //val valueMaintenance = Queue[Int]()
        
        def getMedianCS: Double = {
            var index = 0.0
            var center = 0
            //countArray.foreach(print)
            //println("\n")
            if ( d % 2 == 1 ){
               breakable{
                   for ( i <- 0 until countArray.size ){
                       center = center.+( countArray.apply( i ) )
                       if ( center >= (d + 1 )/2 ){ 
                          index = i
                          break()
                       }
                   } 
               }
               
            }else{
               var firstElem = -1
               var secondElem = -1
               breakable{
                   for ( i <- 0 until countArray.size ){
                       center = center.+( countArray.apply( i ) )
                       if ( center >= d/2 && firstElem == -1 ) 
                          firstElem = i
                       if ( center >= d/2 + 1 ){
                          secondElem = i
                          break()
                       }
                   }
                }
                //println( "firstElem - " + firstElem + " :secondElem  - " + secondElem )
                index = ( firstElem / 2.0 + secondElem / 2.0 )
            }
            return index
        }
        //CountSort Implementation Ends
        
        var n = 0
        //var sortedValues: Array[Int] = Array[Int]()
        for ( i <- n to d - 1 ) {
           //push( expenditure.apply(i) ) priority Queue
           //rebalance                    priority Queue
           //valueMaintenance.+=( expenditure.apply(i) )
           countArray.update( expenditure.apply(i), countArray.apply( expenditure.apply(i) ) + 1 ) 
        }
        //countArray.foreach(print)
        //println("\n")
        for ( i <- d to expenditure.size - 1 ){   
            val medianValue = getMedianCS  //getMedianP    
            //val medianValue = getMedian( n, i - 1 ) QuickSort
            
            //println( "n - " + n + " :i - " + i  + " :Median - " + medianValue + "  for expenditure.apply(i) - " + expenditure.apply(i) )
            if ( expenditure.apply(i) >= 2.0 * medianValue )  // Math.multiplyExact(2, ) sortedValues
                noOfNotifications = noOfNotifications.+(1)  
                
            //n = n.+(1) QuickSort
            //push( expenditure.apply(i) ) priority Queue
            //rebalance                    priority Queue
                 
            //val removedElem =  expenditure.apply( i - d )   //valueMaintenance.dequeue()
            countArray.update( expenditure.apply( i - d ) , countArray.apply( expenditure.apply( i - d ) )  - 1 )
            countArray.update( expenditure.apply(i) , countArray.apply( expenditure.apply(i) ) + 1 )
            
            //println( "noOfNotifications - " + noOfNotifications )
         }
   
        return noOfNotifications
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        //System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankFraudulent.txt"));
        System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankFraudulentTest3.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val nd = stdin.readLine.split(" ")
        val n = nd(0).trim.toInt
        val d = nd(1).trim.toInt

        val expenditure = stdin.readLine.split(" ").map(_.trim.toInt)
        val result = activityNotifications(expenditure, d)
   
        println( result )
        //printWriter.println(result)
        //printWriter.close()
    }  
}