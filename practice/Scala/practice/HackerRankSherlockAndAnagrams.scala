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
 *  Sherlock and
Anagrams
Two strings are anagrams of each other if the letters of one string can be rearranged to form the other string. Given a string, find the number of pairs of substrings of the string which are anagrams of each other.
For example , the list of all anagrammatic pairs is at positions respectively.
Function Description
Complete the function sherlockAndAnagrams in the editor below. It must return an integer representing the number of anagrammatic pairs of substrings in .
sherlockAndAnagrams has the following parameter(s): s: a string .
Input Format
The first line contains an integer , the number of queries. Each of the next lines contains a string to analyze.
Constraints
String contains only lowercase letters ascii[a-z].
Output Format
For each query, return the number of unordered anagrammatic pairs.
Sample Input 0
2 abba abcd
Sample Output 0
4 0
Explanation 0
The list of all anagrammatic pairs is and
and at positions respectively.
No anagrammatic pairs exist in the second query as no character repeats.
Sample Input 1
2 ifailuhkqq kkkk

 Sample Output 1
3 10
Explanation 1
For the first query, we have anagram pairs respectively.
and
at positions and
For the second query:
There are 6 anagrams of the form
.
There are 3 anagrams of the form There is 1 anagram of the form
Sample Input 2
1 cdcd
Sample Output 2
5
Explanation 2
There are two anagrammatic pairs of length : There are three anagrammatic pairs of length
at positions
at positions at position
and and .
and :
.
respectively.
.
at positions
 * 
 */
class HackerRankSherlockAndAnagrams {
  
}

object HackerRankSherlockAndAnagrams {
  
    import scala.collection.mutable.HashMap
    val countAnagrams: HashMap[String, Int] = HashMap[String, Int]()
    
    def putValue( value: String ) = {
        countAnagrams.+=( value -> ( countAnagrams.getOrElse(value, -1) + 1 ) )
    }
    
    // Complete the sherlockAndAnagrams function below.
    def sherlockAndAnagrams(s: String): Int = {
      
        var noOfAnagrams = 0
        var num = 1
        countAnagrams.clear()
        while ( num < s.size ){
            for( i <- 0 to s.size - num ){
                putValue( s.substring(i, i + num).sortWith(_ < _) )
            }
            num.+=(1)
        }
        
        /*for( f <- countAnagrams.filter(p => p._2 > 0 ) )
        {
           noOfAnagrams.+=  ( ( f._2 *  ( f._2 + 1 )  ) / 2 )
        }*/
        countAnagrams.foreach(println)
        countAnagrams.filter(p => p._2 > 0 ).foreach{f => noOfAnagrams.+=(  ( f._2 *  ( f._2 + 1 )  ) / 2 ) }
        
        return noOfAnagrams
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankAnagrams.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val q = stdin.readLine.trim.toInt

        for (qItr <- 1 to q) {
            val s = stdin.readLine

            val result = sherlockAndAnagrams(s)
            println( result )
            //printWriter.println(result)
        }

        //printWriter.close()
    }
    
}