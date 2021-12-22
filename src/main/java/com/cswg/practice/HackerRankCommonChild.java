package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;

/*
 *  Common Child
A string is said to be a child of a another string if it can be formed by deleting 0 or more characters from the other string. Given two strings of equal length, what's the longest string that can be constructed such that it is a child of both?
For example, ABCD and ABDC have two children with maximum length 3, ABC and ABD . They can be formed by eliminating either the D or C from both strings. Note that we will not consider ABCD as a common child because we can't rearrange characters and ABCD ABDC .
Function Description
Complete the commonChild function in the editor below. It should return the longest string which is a common child of the input strings.
             commonChild has the following parameter(s): s1, s2: two equal length strings
Input Format
There is one line with two space-separated strings,
Constraints
and .
                      All characters are upper case in the range ascii[A-Z].
 Output Format
Print the length of the longest string
Sample Input
HARRY SALLY
Sample Output
2
Explanation
, such that
is a child of both
and .
        The longest string that can be formed by deleting zero or more characters from , whose length is 2.
Sample Input 1
AA BB
Sample Output 1
0
Explanation 1
and is
              and have no characters in common and hence the output is 0.
    
 Sample Input 2
 SHINCHAN NOHARAAA
Sample Output 2
3
Explanation 2
The longest string that can be formed between and the order is .
Sample Input 3
ABCDEF FBDAMN
Sample Output 3
2
Explanation 3
is the longest child of the given strings.
while maintaining
                        
 */
public class HackerRankCommonChild {

	   //It is based on Longest common subsequence
	   //https://en.wikipedia.org/wiki/Longest_common_subsequence_problem
	   // Complete the commonChild function below.
	    static int commonChild(String s1, String s2) {
	
	    	int[][] commonArray = new int[s1.length()+1][s2.length()+1];
	    	
	    	for( int indexS1 = 0; indexS1 <= s1.length(); indexS1 ++){
	    		commonArray[indexS1][0] = 0;
	    	}
	    	
	    	for( int indexS2 = 0; indexS2 <= s2.length(); indexS2 ++){
	    		commonArray[0][indexS2] = 0;
	    	}
	    	
	    	for( int indexS1 = 1; indexS1 <= s1.length(); indexS1 ++){
	    		for ( int indexS2 = 1; indexS2 <= s2.length(); indexS2 ++){
	    			if ( s1.charAt(indexS1-1) == s2.charAt(indexS2-1) )
	    				commonArray[indexS1][indexS2] = commonArray[indexS1-1][indexS2-1] + 1;
	    			else
	    				commonArray[indexS1][indexS2] = Math.max( commonArray[indexS1-1][indexS2], commonArray[indexS1][indexS2-1]);
	    		}
	    	}

	    	return commonArray[s1.length()][s2.length()];
	
	    }
    
	   public static void main(String[] args) throws IOException {
	        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));
		   System.out.println(UtilCommon.fileLocation);
	        try {
				System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankCommonChild.txt"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        Scanner scanner = new Scanner(System.in);
	        String s1 = scanner.nextLine();
	        String s2 = scanner.nextLine();
	        System.out.println(s1 + " --- \t" + s2 );
	        int result = commonChild(s1, s2);
	        System.out.println(result);
	   }
}
