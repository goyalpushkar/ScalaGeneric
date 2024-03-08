package com.cswg.practice

/*
 *  Sparse Arrays
There is a collection of input strings and a collection of query strings. For each query string, determine how many times it occurs in the list of input strings.
For example, given input and , we find instances of , of and of . For each query, we add an element to our return array, .
Function Description
Complete the function matchingStrings in the editor below. The function must return an array of integers representing the frequency of occurrence of each query string in strings.
matchingStrings has the following parameters: strings - an array of strings to search queries - an array of query strings
Input Format
The first line contains and integer , the size of Each of the next lines contains a string
The next line contains , the size of Each of the next lines contains a string
.
Constraints
Output Format
.
. .
Return an integer array of the results of all queries in order.
Sample Input 0
4 aba baba aba xzxb 3 aba xzxb ab
Sample Output 0
2 1 0
Explanation 0
Here, "aba" occurs twice, in the first and third string. The string "xzxb" occurs once in the fourth string, and "ab" does not occur at all.
Sample Input 1
3
.

  def de fgh 3 de lmn fgh
Sample Output 1
1 0 1
Sample Input 2
13 abcde sdaklfj asdjf na basdn sdaklfj asdjf na asdjf na basdn sdaklfj asdjf 5 abcde sdaklfj asdjf na basdn
Sample Output 2
1 3 4 3 2
   
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

object HackerRankSparseArray {
      // Complete the matchingStrings function below.
    def matchingStrings(strings: Array[String], queries: Array[String]): Array[Int] = {

       import scala.collection.mutable.ArrayBuffer
       //val returnArray = new Array[Int](queries.size);
       val returnArray = new ArrayBuffer[Int]();
        queries.foreach { x => var count = 0
                               strings.foreach { y => if( y.equalsIgnoreCase(x) ) 
                                                         count.+=(1)
                                                }
                           returnArray.+=(count)
                         }
        
        return returnArray.toArray
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankSparseArray.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val stringsCount = stdin.readLine.trim.toInt
        val strings = Array.ofDim[String](stringsCount)

        for (i <- 0 until stringsCount) {
            val stringsItem = stdin.readLine
            strings(i) = stringsItem}

        val queriesCount = stdin.readLine.trim.toInt
        val queries = Array.ofDim[String](queriesCount)
        for (i <- 0 until queriesCount) {
            val queriesItem = stdin.readLine
            queries(i) = queriesItem}

        val res = matchingStrings(strings, queries)
        System.out.print(res.mkString("\n") )
        //printWriter.println(res.mkString("\n"))
        //printWriter.close()
    }
}