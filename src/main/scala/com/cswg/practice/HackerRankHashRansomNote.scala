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
 *  Hash Tables: Ransom
Note
Harold is a kidnapper who wrote a ransom note, but now he is worried it will be traced back to him through his handwriting. He found a magazine and wants to know if he can cut out whole words from it and use them to create an untraceable replica of his ransom note. The words in his note are case- sensitive and he must use only whole words available in the magazine. He cannot use substrings or concatenation to create the words he needs.
Given the words in the magazine and the words in the ransom note, print Yes if he can replicate his ransom note exactly using whole words from the magazine; otherwise, print No .
For example, the note is "Attack at dawn". The magazine contains only "attack at dawn". The magazine has all the right words, but there's a case mismatch. The answer is .
       Function Description
Complete the checkMagazine function in the editor below. It must print using the magazine, or .
checkMagazine has the following parameters:
magazine: an array of strings, each a word in the magazine note: an array of strings, each a word in the ransom note
Input Format
The first line contains two space-separated integers, and and the ..
The second line contains space-separated strings, each The third line contains space-separated strings, each
Constraints
.
Each word consists of English alphabetic letters (i.e., to
Output Format
if the note can be formed
       , the numbers of words in the
. .
                                                                          and
to ).
      Print Yes if he can use the magazine to create an untraceable replica of his ransom note. Otherwise, print No.
Sample Input 0
64
give me one grand today night give one grand today
Sample Output 0
Yes
   Sample Input 1

  65
two times three is not four two times two is four
Sample Output 1
No
Explanation 1
'two' only occurs once in the magazine.
Sample Input 2
74
ive got a lovely bunch of coconuts ive got some coconuts
Sample Output 2
No
Explanation 2
Harold's magazine is missing the word .
       
 */
class HackerRankHashRansomNote {
  
}

object HackerRankHashRansomNote {
  
     import scala.collection.mutable.HashMap
     import scala.util.control.Breaks._
     val valueCount: HashMap[String, Int] = HashMap[String, Int]()
     
     def addValue( value: String ) = {
         valueCount.+=( ( value -> ( getValue( value ) + 1 ) ) )
     }
     
     def checkValue( value: String ): Boolean = {
         return valueCount.contains(value)
     }
     
     def getValue( value: String ): Int = {
         return valueCount.getOrElse(value, 0)
     }
     
     def validateValue( value: String ): Boolean = {
         if ( checkValue( value ) ) {
            if ( getValue(value) - 1 <= 0 ){
              valueCount.-=( value )
            }else{
              valueCount.update(value, getValue(value) - 1)
            }
            return true
         }else{
            return false
         }
         
     }
     
    // Complete the checkMagazine function below.
    def checkMagazine(magazine: Array[String], note: Array[String]) {
        magazine.foreach { x => addValue(x) }
        valueCount.foreach(println)
        var exists = "Yes"
        breakable{
          for( value <- note ){
              if ( !validateValue( value ) ){
                 exists = "No"
                 break
              }
          }
        }
        
        println( exists )
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankHashRansome.txt"));
        ///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankCountingInversions.txt
        //C:/Pushkar/STS39Workspace/GeneralLearning/
        
        val mn = stdin.readLine.split(" ")
        val m = mn(0).trim.toInt
        val n = mn(1).trim.toInt

        val magazine = stdin.readLine.split(" ")
        val note = stdin.readLine.split(" ")
        checkMagazine(magazine, note)
        
    }  
}