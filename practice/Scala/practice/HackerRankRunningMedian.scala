package com.cswg.practice

import java.io.FileInputStream
import java.io.PrintWriter

/*
 * Find the Running
Median
The median of a set of integers is the midpoint value of the data set for which an equal number of integers
are less than and greater than the value. To find the median, you must first sort your set of integers in
non-decreasing order, then:
If your set contains an odd number of elements, the median is the middle element of the sorted
sample. In the sorted set , is the median.
If your set contains an even number of elements, the median is the average of the two middle
elements of the sorted sample. In the sorted set , is the median.
Given an input stream of integers, you must perform the following task for each integer:
1. Add the integer to a running list of integers.
2. Find the median of the updated list (i.e., for the first element through the element).
3. Print the list's updated median on a new line. The printed value must be a double-precision number
scaled to decimal place (i.e., format).
Input Format
The first line contains a single integer, , denoting the number of integers in the data stream.
Each line of the subsequent lines contains an integer, , to be added to your list.
Constraints
Output Format
After each new integer is added to the list, print the list's updated median on a new line as a single
double-precision number scaled to decimal place (i.e., format).
Sample Input
6
12
4
5
3
8
7
Sample Output
12.0
8.0
5.0
4.5
5.0
6.0
Explanation
There are integers, so we must print the new median on a new line as each integer is added to the
list:
1. 2. 3. 4. 5. 6.
 */
class HackerRankRunningMedian {
  
}

object HackerRankRunningMedian {
  
    import scala.collection.mutable.PriorityQueue
    import scala.collection.mutable.ArrayBuffer
    import scala.math.Ordering
    def newOrdering = new Ordering[Int] {
        def compare( a: Int, b: Int ) = -1 * a.compare(b)
    }
    
    val lowerQueue = PriorityQueue[Int]()
    val upperQueue = PriorityQueue[Int]()(newOrdering)
    
    def addElement( elem: Int ) = {
        if ( lowerQueue.isEmpty ){
           lowerQueue.enqueue(elem)
       }else {
          if ( elem <= lowerQueue.head ) {
             lowerQueue.enqueue(elem)
          }else{
              upperQueue.enqueue(elem)
          }
        }
    }
    
    def rebalance = {
        if ( lowerQueue.size - upperQueue.size >= 2 ) {
             upperQueue.enqueue( lowerQueue.dequeue() )
        }else if ( upperQueue.size - lowerQueue.size >= 2 ) {
             lowerQueue.enqueue( upperQueue.dequeue() )
        }
    }
    
    def getMedian: Double = {
        if ( lowerQueue.size > upperQueue.size ) 
           return lowerQueue.head
        else if ( upperQueue.size > lowerQueue.size ) 
           return upperQueue.head
        else return ( lowerQueue.head / 2.0 + upperQueue.head / 2.0 )
    }
    
    /*
     * Complete the runningMedian function below.
     */
    def runningMedian(a: Array[Int]): Array[Double] = {
        /*
         * Write your code here.
         */
        a.foreach(println)
        val returnArray = ArrayBuffer[Double]()
        /*
         * println( "Elem - " + f )
           lowerQueue.foreach(f => print(f + "\t"  ))
           print("\n")
           upperQueue.foreach(f => print(f + "\t"  ))
           print("\n")
         */
        a.foreach{f => addElement(f) 
                       rebalance
                       returnArray.+=( getMedian )
                  }
        return returnArray.toArray
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankRunningMedian.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        
        val aCount = stdin.readLine.trim.toInt
        val a = Array.ofDim[Int](aCount)
        println( "aCount - " + aCount )
        for (aItr <- 0 until aCount) {
            val aItem = stdin.readLine.trim.toInt
            a(aItr) = aItem
        }

        val result = runningMedian(a)
        println(result.mkString("\n"))
        //printWriter.println(result.mkString("\n"))
        //printWriter.close()
    }  
}