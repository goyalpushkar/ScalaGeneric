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

class HackerRankJumpingClouds {
  
}

/*
Emma is playing a new mobile game that starts with consecutively numbered clouds. Some of the clouds are thunderheads and others are cumulus. She can jump on any cumulus cloud having a number that is equal to the number of the current cloud plus  or . She must avoid the thunderheads. Determine the minimum number of jumps it will take Emma to jump from her starting postion to the last cloud. It is always possible to win the game.

For each game, Emma will get an array of clouds numbered  if they are safe or  if they must be avoided. For example,  
c = [0,1,0,0,0,1,0] indexed from 0..6. The number on each cloud is its index in the list so she must avoid the clouds at indexes 1 and 5. 
She could follow the following two paths: 0 -> 2 -> 4 -> 6 or 0 -> 2 -> 3 -> 4 -> 6. The first path takes 3 jumps while the second takes 4.


Function Description

Complete the jumpingOnClouds function in the editor below. It should return the minimum number of jumps required, as an integer.

jumpingOnClouds has the following parameter(s):

c: an array of binary integers
Input Format

The first line contains an integer , the total number of clouds. The second line contains  space-separated binary integers describing clouds  where .

Constraints

Output Format

Print the minimum number of jumps needed to win the game.
*/

object HackerRankJumpingClouds {

    import scala.annotation.tailrec
            
    // Complete the jumpingOnClouds function below.
    def jumpingOnClouds(c: Array[Int]): Int = {

        @tailrec
        def nextIndex(movingIndexP: Int, noOfJumpsP: Int ): Int = {
            var noOfJumps = noOfJumpsP
            var movingIndex = movingIndexP
            println( "movingIndex - " + movingIndex + "\t" + "noOfJumps - " + noOfJumps )
            
            if ( movingIndex == c.length - 1)
               return noOfJumps
               
            if ( c.apply(movingIndex) == 1 ) {
                movingIndex = movingIndex.-(1)
            }else{
                if ( movingIndex.+(2) > c.length - 1 ){
                   movingIndex = movingIndex.+(1)
                }else{
                   movingIndex = movingIndex.+(2)  
                }
                noOfJumps = noOfJumps.+(1)
            }
            
            nextIndex( movingIndex, noOfJumps )
        }
        var start = 0
        val jumps = nextIndex( start, 0 )
                
        return jumps
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        //val n = stdin.readLine.trim.toInt
        //val c = stdin.readLine.split(" ").map(_.trim.toInt)
        val n = args(0).trim.toInt
        val c = args(1).split(" ").map(_.trim.toInt)
        
        println( "n - " + n + "\t" + " :c - " + c.size ) 
        val result = jumpingOnClouds(c)
        println( "result - " + result ) 
        //printWriter.println(result)
        //printWriter.close()
    }
}

