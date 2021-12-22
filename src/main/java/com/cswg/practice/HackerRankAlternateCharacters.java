package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;

/*
 * Alternating Characters
You are given a string containing characters and only. Your task is to change it into a string such that there are no
matching adjacent characters. To do this, you are allowed to delete zero or more characters in the string.
Your task is to find the minimum number of required deletions.
For example, given the string , remove an at positions and to make in deletions.
Function Description
Complete the alternatingCharacters function in the editor below. It must return an integer representing the minimum
number of deletions to make the alternating string.
alternatingCharacters has the following parameter(s):
s: a string
Input Format
The first line contains an integer , the number of queries.
The next lines each contain a string .
Constraints
Each string will consist only of characters and
Output Format
For each query, print the minimum number of deletions required on a new line.
Sample Input
5
AAAA
BBBBB
ABABABAB
BABABA
AAABBB
Sample Output
3
4
0
0
4
Explanation
The characters marked red are the ones that can be deleted so that the string doesn't have matching consecutive
characters.

 */
public class HackerRankAlternateCharacters {

	public static int alternatingCharacters( String str ) {
	
		int noOfDeletions = 0;
		Character prev = 0;
		for( Character c: str.toCharArray() ) {
			//System.out.println( "c - " + c + " :prev - " + prev );
			if ( c == prev )
				noOfDeletions++;
			
			prev = c;
					
		}
		return noOfDeletions;
	}
	
	public static void main( String[] args ) {
		
		System.out.println( UtilCommon.fileLocation  );
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankAlternateCharacters.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        int testCases = scanner.nextInt();
        System.out.println(testCases);
        
        for( int index = 0; index < testCases; index++ ) {
        	String str = scanner.next();
        	System.out.println(str);
        	
        	int result = alternatingCharacters ( str );
        	System.out.println(result);
        }
        
        		
        		
	}
}
