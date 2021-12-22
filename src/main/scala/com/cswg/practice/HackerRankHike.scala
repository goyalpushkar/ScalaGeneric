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

class HackerRankHike {
  
}

/*
 * Gary is an avid hiker. He tracks his hikes meticulously, paying close attention to small details like topography. During his last hike he took exactly  steps. For every step he took, he noted if it was an uphill, , or a downhill,  step. Gary's hikes start and end at sea level and each step up or down represents a  unit change in altitude. We define the following terms:

A mountain is a sequence of consecutive steps above sea level, starting with a step up from sea level and ending with a step down to sea level.
A valley is a sequence of consecutive steps below sea level, starting with a step down from sea level and ending with a step up to sea level.
Given Gary's sequence of up and down steps during his last hike, find and print the number of valleys he walked through.

For example, if Gary's path is [DDUUUUDD], he first enters a valley  units deep. Then he climbs out an up onto a mountain  units high. Finally, he returns to sea level and ends his hike.

Function Description

Complete the countingValleys function in the editor below. It must return an integer that denotes the number of valleys Gary traversed.

countingValleys has the following parameter(s):

n: the number of steps Gary takes
s: a string describing his path
Input Format

The first line contains an integer , the number of steps in Gary's hike. 
The second line contains a single string , of  characters that describe his path.

Constraints

Output Format

Print a single integer that denotes the number of valleys Gary walked through during his hike.
 */
object HackerRankHike {

    // Complete the countingValleys function below.
    def countingValleys(n: Int, s: String): Int = {
        import scala.collection.mutable.Stack
        //import scala.collection.mutable.{Map => MMap}
        
        var mountains = 0
        var valleys = 0
        var takeDecision = 1 
        var mountainStack = Stack[Char]()
        var valleyStack = Stack[Char]()
        
        //val valuesStack: MMap[String, ( Int, Int )] = MMap[String, ( Int, Int )]()  //Stack[String]()
        
        def valueComparison( value: Char, updateStack: Stack[Char], incrementValue: String): Stack[Char] = {
            //var returnStack: Stack[String] = Stack[String]()
            if ( updateStack.head == value ) {
                updateStack.push(value)
                takeDecision = 0
            }else{
                updateStack.pop()
                if ( updateStack.isEmpty ) {
                   if ( "M".equalsIgnoreCase( incrementValue ) ){
                      mountains = mountains.+(1)
                   }else{
                     valleys = valleys.+(1)
                   }
                   takeDecision = 1
                }else{
                    takeDecision = 0
                }
            }
            
            return updateStack
        }
        
        def decision( verifyChar: Char) = {
             if ( "U".equalsIgnoreCase( verifyChar.toString() ) ) {
                  mountainStack = mountainStack.push(verifyChar) 
              }else{
                 valleyStack = valleyStack.push(verifyChar) 
              }
              takeDecision = 0
        }
        
        //decision( s.apply(0) ) substring(1).
        s.foreach{ f => if ( takeDecision == 1 ){
                            decision(f) 
                         }else{
                             if ( mountainStack.isEmpty ){
                                 valueComparison( f, valleyStack, "V" )
                             }else{
                                 valueComparison( f, mountainStack, "M" )
                             }
                         }
                         println( "f - " + f + "\t" + " :mountainStack.size - " + mountainStack.size + "\t" + " :valleyStack.size - " + valleyStack.size + "\n" 
                               +  "valleys  - " + valleys  + "\t" + " :mountains - " + mountains 
                                )
                 }
        
         return valleys;
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        //val n = stdin.readLine.trim.toInt
        //val s = stdin.readLine
        val n = args(0).trim.toInt
        val s = args(1)
        
        val result = countingValleys(n, s)
        println( result )
        //printWriter.println(result)
        //printWriter.close()
    }
}
