package com.cswg.practice;

import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

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
public class HackerRankBalancedBrackets {
	   // Complete the isBalanced function below.

    static String isBalanced(String s) {
    	 System.out.println(s);
    	 String balancedCheck = "YES";
         Stack<Character> stackBrackets = new Stack<Character>();
         for( Character bracket: s.toCharArray()) {
        	 if ( "[".equals(bracket) || "{".equals(bracket) || "(".equals(bracket) ){
        		 stackBrackets.push(bracket);
        	 }else{
        		 Character lastBracket = stackBrackets.pop();
        		 if ( "]".equals(bracket) ){
        			 if ( !("[".equals(lastBracket)) )
        				 return "NO";
        		 }else if ( !("}".equals(bracket)) ) {
        			 if ( "{".equals(lastBracket) )
        				 return "NO";
        		 }else if ( !(")".equals(bracket)) ) {
        			 if ( "(".equals(lastBracket) )
        				 return "NO";
        		 }
        	 }
         }

         return balancedCheck;
    }

    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));
        try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankBalancedBracket.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int t = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
        System.out.println( "t - " + t );
        for (int tItr = 0; tItr < t; tItr++) {
            String s = scanner.nextLine();

            String result = isBalanced(s);
            System.out.println( result );
            //bufferedWriter.write(result);
            //bufferedWriter.newLine();
        }

        //bufferedWriter.close();
        scanner.close();
    }
}
