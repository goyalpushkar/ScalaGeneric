package com.cswg.practice

class HackerRankAnagrams {
  
}

import java.io._

/*
 * Strings: Making
Anagrams
Alice is taking a cryptography class and finding anagrams to be very useful. We consider two strings to be
anagrams of each other if the first string's letters can be rearranged to form the second string. In other
words, both strings must contain the same exact letters in the same exact frequency For example, bacdc
and dcbac are anagrams, but bacdc and dcbad are not.
Alice decides on an encryption scheme involving two large strings where encryption is dependent on the
minimum number of character deletions required to make the two strings anagrams. Can you help her
find this number?
Given two strings, and , that may or may not be of the same length, determine the minimum number
of character deletions required to make and anagrams. Any characters can be deleted from either of
the strings.
For example, if and , we can delete from string and from string so that both
remaining strings are and which are anagrams.
Function Description
Complete the makeAnagram function in the editor below. It must return an integer representing the
minimum total characters that must be deleted to make the strings anagrams.
makeAnagram has the following parameter(s):
a: a string
b: a string
Input Format
The first line contains a single string, .
The second line contains a single string, .
Constraints
The strings and consist of lowercase English alphabetic letters ascii[a-z].
Output Format
Print a single integer denoting the number of characters you must delete to make the two strings
anagrams of each other.
Sample Input
cde
abc
Sample Output
4
Explanation
We delete the following characters from our two strings to turn them into anagrams of each other:
1. Remove d and e from cde to get c .
2. Remove a and b from abc to get c .
We must delete characters to make both strings anagrams, so we print on a new line.
 */
object HackerRankAnagrams{
  
      // Complete the makeAnagram function below.
    def makeAnagram(a: String, b: String): Int = {

        import scala.collection.mutable.HashMap
        val aMap = new HashMap[Char, Int]()
        val bMap = new HashMap[Char, Int]()
        
        a.foreach(f => aMap.put( f, aMap.getOrElse(f, 0 ) + 1) )
        b.foreach(f => bMap.put( f, bMap.getOrElse(f, 0 ) + 1) )
            
        var totalDec = 0
        aMap.foreach(f => totalDec.+=( ( if ( f._2 - bMap.getOrElse(f._1, 0) > 0 ) f._2 - bMap.getOrElse(f._1, 0)  else 0 ) ) )
        bMap.foreach(f => totalDec.+=( ( if ( f._2 - aMap.getOrElse(f._1, 0) > 0 ) f._2 - aMap.getOrElse(f._1, 0)  else 0 ) ) )
        
        return totalDec
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream( fileLocation + "HackerRankAnaagrams.txt"));
         
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val a = stdin.readLine
        val b = stdin.readLine

        val res = makeAnagram(a, b)

        System.out.println( res )
        //printWriter.println(res)
        //printWriter.close()
    }
}