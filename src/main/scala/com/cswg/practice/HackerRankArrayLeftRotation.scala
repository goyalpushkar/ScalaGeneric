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
 * 
Check out the resources on the page's right side to learn more about arrays. 
The video tutorial is by Gayle Laakmann McDowell, author of the best-selling interview book Cracking the Coding Interview.

A left rotation operation on an array shifts each of the array's elements  unit to the left. For example, if 2 left rotations are performed on array [1,2,3,4,5), then the array would become [3,4,5,1,2] .

Given an array a of n integers and a number, d, perform d left rotations on the array. Return the updated array to be printed as a single line of space-separated integers.

Function Description

Complete the function rotLeft in the editor below. It should return the resulting array of integers.

rotLeft has the following parameter(s):

An array of integers a.
An integer d, the number of rotations.
Input Format

The first line contains two space-separated integers n and d, the size of a and the number of left rotations you must perform. 
The second line contains n space-separated integers a[i].

Constraints
1 <= n <= 10^5
1 <= d <= n
1 <= a[i] <= 10^6

Output Format

Print a single line of n space-separated integers denoting the final state of the array after performing d  left rotations.

Sample Input
5 4
1 2 3 4 5

Sample Output
5 1 2 3 4

Explanation
When we perform d=4 left rotations, the array undergoes the following sequence of changes:
 [1,2,3,4,5] -> [2,3,4,5,1] -> [3,4,5,1,2] -> [4,5,1,2,3] -> [5,1,2,3,4]
 */
class HackerRankArrayLeftRotation {
  
}

object HackerRankArrayLeftRotation{
  
   // Complete the rotLeft function below.
    def rotLeftEasy(a: Array[Int], d: Int, n: Int): Array[Int] = {
       val newArray: Array[Int] = Array.fill(n)(0);
       for( i <- 0 to a.size - 1 ){
          newArray.update( ( i + n -d ) % n, a.apply(i) );
       }
       
       return newArray;
    }
    
    def rotLeft(a: Array[Int], d: Int): Array[Int] = {

        /*var newArray = a
        def rotation = {
            val temp = a(0)
            for ( i <- 1 to a.length - 1 ){
               newArray.update( i -1 , a.apply(i))
            }
            newArray.update( a.length - 1, temp)
        }
        
        for ( i <- 1 to d ){
            rotation
        }*/
        //a.foreach( println )
        val length = ( a.size - d % a.size )
        val remainder = length % a.size
        import scala.util.control.Breaks._
        var newArray: Array[Int] = Array[Int]()
        var start = 0
        for( i <- 0 to a.size - 1 ){
            //println( "Value - " + a.apply(i) + " == " + " old Index - " + i + " -> " + " new Index - " + ( ( ( a.size - d % a.size ) + i ) % a.size ) )
            if ( ( length + i  ) % a.size == start ) {
                newArray = newArray.:+( a.apply(i) )
                start = start.+(1)
            }
        }
        newArray.foreach(println)
        breakable{
            for( i <- 0 to a.size - 1 ){
                //println( "Value - " + a.apply(i) + " == " + " old Index - " + i + " -> " + " new Index - " + ( ( ( a.size - d % a.size ) + i ) % a.size ) )
                if ( ( length + i  ) % a.size == 0 ){
                    break()
                }else{
                    newArray = newArray.:+( a.apply(i) )
                }
            }
        }
        
        return newArray  //.toArray
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        //val nd = stdin.readLine.split(" ")

        /*val n = nd(0).trim.toInt
        val d = nd(1).trim.toInt
        val a = stdin.readLine.split(" ").map(_.trim.toInt)*/
        val n = args(0).trim.toInt
        val d = args(1).trim.toInt
        val a = args(2).split(" ").map(_.trim.toInt)        
        
        val result = rotLeft(a, d)
        result.foreach{ f => print(f + " " ) }
        //printWriter.println(result.mkString(" "))
        //printWriter.close()
    }  
}