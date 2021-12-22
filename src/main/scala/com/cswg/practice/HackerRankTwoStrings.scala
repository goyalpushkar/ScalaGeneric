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
 *  Two Strings
Given two strings, determine if they share a common substring. A substring may be as small as one character.
For example, the words "a", "and", "art" share the common substring . The words "be" and "cat" do not share a substring.
Function Description
Complete the function twoStrings in the editor below. It should return a string, either YES or NO based on whether the strings share a common substring.
twoStrings has the following parameter(s): s1, s2: two strings to analyze .
Input Format
The first line contains a single integer , the number of test cases. The following pairs of lines are as follows:
The first line contains string .
The second line contains string .
Constraints
and consist of characters in the range ascii[a-z].
Output Format
For each pair of strings, return YES or NO . Sample Input
2 hello world hi world
Sample Output
                                               YES NO
Explanation
We have 1.
2.
pairs to check: ,
.
and are common to both strings.
                             ,
. The substrings
and share no common substrings.
                   
 */
class HackerRankTwoStrings {
  
}

object HackerRankTwoStrings {
    
    import scala.collection.mutable.HashMap
    import scala.util.control.Breaks._
    val stringCount: HashMap[Char, Int] = HashMap[Char, Int]()
    
    def addValue( value: Char ) = {
        if ( !(checkValue(value) ) ){
           stringCount.+=( (value, 1 ) )
        }
    }
    
    def checkValue( value: Char ): Boolean = {
        return stringCount.contains(value)
    }
    
    // Complete the twoStrings function below.
    def twoStrings(s1: String, s2: String): String = {

        var exists = "NO"
        stringCount.clear() 
        
        if ( s1.length() < s2.length() ) {
           s1.foreach { x => addValue(x) }
           //stringCount.foreach(println)
           breakable{
               for ( value <- s2 ){
                  if ( (checkValue( value ) ) ){
                     exists = "YES"
                     break
                  }
               }
           }
           
        }else{
           s2.foreach { x => addValue(x) }
           //stringCount.foreach(println)
           breakable{
               for ( value <- s1 ){
                  if ( checkValue( value ) ){
                     exists = "YES"
                     break
                  }
               }
           }
        }
        
        
        
        return exists
    }
    
    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankTwoStrings.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val q = stdin.readLine.trim.toInt

        for (qItr <- 1 to q) {
            
            val s1 = stdin.readLine
            val s2 = stdin.readLine
            val result = twoStrings(s1, s2)

            println(result)
            //printWriter.println(result)
        }

        //printWriter.close()
    }  
}