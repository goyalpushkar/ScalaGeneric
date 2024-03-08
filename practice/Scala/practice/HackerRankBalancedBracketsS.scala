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
 *  Balanced Brackets
A bracket is considered to be any one of the following characters: (, ), {, }, [, or ].
Two brackets are considered to be a matched pair if the an opening bracket (i.e., ( , [ , or { ) occurs to the left of a closing bracket (i.e., ), ], or }) of the exact same type. There are three types of matched pairs of brackets: [], {}, and ().
A matching pair of brackets is not balanced if the set of brackets it encloses are not matched. For example, {[(])} is not balanced because the contents in between { and } are not balanced. The pair of square brackets encloses a single, unbalanced opening bracket, ( , and the pair of parentheses encloses a single, unbalanced closing square bracket, ] .
By this logic, we say a sequence of brackets is balanced if the following conditions are met: It contains no unmatched brackets.
The subset of brackets enclosed within the confines of a matched pair of brackets is also a matched pair of brackets.
Given strings of brackets, determine whether each sequence of brackets is balanced. If a string is balanced, return YES . Otherwise, return NO .
Function Description
Complete the function isBalanced in the editor below. It must return a string: YES if the sequence is balanced or NO if it is not.
isBalanced has the following parameter(s): s: a string of brackets
Input Format
The first line contains a single integer , the number of strings.
Each of the next lines contains a single string , a sequence of brackets.
Constraints
, where is the length of the sequence. All chracters in the sequences ∈ { {, }, (, ), [, ] }.
Output Format
For each string, return YES or NO . Sample Input
3
{[()]} {[(])} {{[[(())]]}}
Sample Output
YES NO YES
                                                            
 Explanation
  1. The string {[()]} meets both criteria for being a balanced string, so we print YES on a new line.
2. The string {[(])} is not balanced because the brackets enclosed by the matched pair { and } are
not balanced: [(]) .
3. The string {{[[(())]]}} meets both criteria for being a balanced string, so we print YES on a new line.
      
 */

class HackerRankBalancedBracketsS {
  
}

object HackerRankBalancedBracketsS {
     // Complete the isBalanced function below.
     import scala.collection.mutable.Stack
    def isBalanced(s: String): String = {
   	     //System.out.println( "New String - "+ s);
    	   val balancedCheck = "YES";
         val stackBrackets = new Stack[Character]();
         
         for( bracket <- s.toCharArray()) {
           //print( bracket + " " )
        	 if ( '['.equals(bracket) || '{'.equals(bracket) || '('.equals(bracket) ){
        		 stackBrackets.push(bracket);
        	 }else{
        	   if ( !stackBrackets.isEmpty ){
          		 val lastBracket = stackBrackets.pop();
          		 //print( "Pop - " + lastBracket + " " )
          		 if ( ']'.equals(bracket) ){
          			 if ( !('['.equals(lastBracket)) )
          				 return "NO";
          		 }else if ( '}'.equals(bracket) ) {
          			 if ( !('{'.equals(lastBracket) ) )
          				 return "NO";
          		 }else if ( ')'.equals(bracket) ) {
          			 if ( !('('.equals(lastBracket) ) )
          				 return "NO";
          		 }
        	   }
        	 }
           //stackBrackets.foreach { println }
         }

         if ( !stackBrackets.isEmpty ) return "NO"
         return balancedCheck;

    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("//Users/goyalpushkar/Documents/STSWorkspace/GeneralLearning/HackerRankBalancedBracket.txt"));
        //C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankBalancedBracket.txt"
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val t = stdin.readLine.trim.toInt

        for (tItr <- 1 to t) {
            val s = stdin.readLine

            val result = isBalanced(s)
            println(result)
            //printWriter.println(result)
        }

        //printWriter.close()
    }
}