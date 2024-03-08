package com.cswg.practice

class HackerRankRepeatedString {
  
}

/*
 * Lilah has a string, s, of lowercase English letters that she repeated infinitely many times.

Given an integer n , find and print the number of letter a's in the first  letters of Lilah's infinite string.

For example, if the string  s = 'abcac' and n=10, the substring we consider is abcacabcac, the first 10 characters of her infinite string. There are 4 occurrences of a in the substring.

Function Description

Complete the repeatedString function in the editor below. It should return an integer representing the number of occurrences of a in the prefix of length n  in the infinitely repeating string.

repeatedString has the following parameter(s):

s: a string to repeat
n: the number of characters to consider
Input Format

The first line contains a single string, . 
The second line contains an integer, .

Constraints

For  of the test cases, .
Output Format
Print a single integer denoting the number of letter a's in the first  letters of the infinite string created by repeating  infinitely many times.
 */
import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._

object HackerRankRepeatedString {

    // Complete the repeatedString function below.
    def repeatedString(s: String, n: Long): Long = {

        /*val count = s.map{f => var count = 0 
                               if ( f.equals('a') ) count = count.+(1)
                               count
                        }.apply(0)*/
        val count = s.count(p => p.equals('a'))     
        println( "A's in curent string - " + count )
        val quotient = n / s.size
        val remainder = n % s.size
        println( "quotient - " + quotient + "\t" + " :remainder - " + remainder )
                        
        val finalCount = ( count * quotient.toLong ) + ( if ( remainder.toLong == 0 ) 0 else s.substring(0, remainder.toInt ).count(p => p.equals('a')) )
        /*println ( " quotient.toLong  - " + quotient.toLong  + "\n"
              + " count * quotient.toLong  - " +  ( count * quotient.toLong ) + "\n"     
              + " ( if ( remainder.toLong == 0 ) 0 else s.substring(0, remainder.toInt).count(p => p.equals('a')) ) - "  
              + ( if ( remainder.toLong == 0 ) 0 else s.substring(0, remainder.toInt).count(p => p.equals('a')) ) )*/
        return finalCount
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        //val s = stdin.readLine
        //val n = stdin.readLine.trim.toLong

        val s = args(0).trim
        val n = args(1).trim().toLong
        
        println( "n - " + n + "\t" + " :s - " + s.size ) 
        val result = repeatedString(s, n)
        println( "result - " + result )
        //printWriter.println(result)
        //printWriter.close()
    }
}
