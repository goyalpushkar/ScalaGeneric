package com.cswg.practice;

/*
 *  Special Palindrome
Again
A string is said to be a special palindromic string if either of two conditions is met: All of the characters are the same, e.g. aaa .
All characters except the middle one are the same, e.g. aadaa .
A special palindromic substring is any substring of a string which meets one of those criteria. Given a string, determine how many special palindromic substrings can be formed from it.
For example, given the string , we have the following special palindromic substrings: .
Function Description
Complete the substrCount function in the editor below. It should return an integer representing the number of special palindromic substrings that can be formed from the given string.
substrCount has the following parameter(s): n: an integer, the length of string s
s: a string
Input Format
The first line contains an integer, , the length of . The second line contains the string .
Constraints
Each character of the string is a lowercase alphabet, .
Output Format
Print a single line containing the count of total special palindromic substrings.
Sample Input 0
5 asasd
Sample Output 0
7
Explanation 0
The special palindromic substrings of are
Sample Input 1
7 abcbaba
                                                                                                   Sample Output 1

  10
Explanation 1
The special palindromic substrings of
Sample Input 2
4 aaaa
Sample Output 2
10
Explanation 2
The special palindromic substrings of
are
                                      are
                                     
 */
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;
import java.util.Map;
import java.util.HashMap;

public class HackerRankSpecialPalindrome {

	static Boolean allAreSame( String s ) {
		 
		 Character prev = 0;
		 for( Character c: s.toCharArray() ){
			 if ( prev != 0 ){
				 if ( prev != c )
					 return false;
			 }
			 prev = c;
			 //characterCount.put( c, characterCount.getOrDefault(c, 0) + 1 );
		 }
		 
	     return true;
	}
	
   static Boolean isPalindrome( String s ) {
	   
	   if( allAreSame(s) )
		   return true;
	   else{
		   if ( s.length() % 2 == 0 )
			   return false;
		   else {
			   int middle = s.length() / 2;
			   if ( allAreSame( s.substring(0, middle) + s.substring(middle + 1, s.length() ) ) ) 
	              return true;
			   else 
				   return false;
		   }
	   }
	   //return true;
   }
   
   static long substrCount(int n, String s) {

	      long totalCount = n;
	      long start = 0;
	      while( start < s.length() ){
	    	  int noPalindromeCount = 0;
	          for( long index = start + 2; index <= s.length(); index++ ){
	        	 if( isPalindrome( s.substring( (int)start, (int)index) ) ){
	        		 //System.out.println( s.substring( (int)start, (int)index) );
	        		 totalCount++;
	        	     noPalindromeCount = 0;
	        	 }else{
	        		 noPalindromeCount++;
	        		 int nonPalindromeLength = s.substring( (int)start, (int)index).length();
	        		 if ( noPalindromeCount >= nonPalindromeLength + 1 )
	        			break;
	        	 }
	          }
	          start++;
	      }
	      return totalCount;
     }
	   
	public static void main(String[] args) {
		// TODO Auto-generated method stub
    	System.out.println(UtilCommon.fileLocation);
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankSpecialPalindrome.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        String s = scanner.nextLine();
        System.out.println( n + " " + s );
        long result = substrCount(n, s);

        System.out.println( result );
	}

}
