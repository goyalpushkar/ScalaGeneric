package com.cswg.practice;

/*
 *  Sherlock and the
Valid String
Sherlock considers a string to be valid if all characters of the string appear the same number of times. It is also valid if he can remove just character at index in the string, and the remaining characters will occur the same number of times. Given a string , determine if it is valid.
For example, if , it is a valid string because frequencies are . So is because we can remove one and have of each character in the remaining string. If
                                       however, the string is not valid as we can only remove
occurrence of
. That would leave character
    frequencies of
Input Format
A single string .
Constraints
Each character
Output Format
Print YES if string Sample Input 0
aabbcd
Sample Output 0
NO
Explanation 0
.
                                        is valid, otherwise, print NO .
        Given
abcd , to make it valid. We are limited to removing only one character, so
Sample Input 1
aabbccddeefghi
Sample Output 1
NO
Explanation 1
Frequency counts for the letters are as follows:
{'a': 2, 'b': 2, 'c': 2, 'd': 2, 'e': 2, 'f': 1, 'g': 1, 'h': 1, 'i': 1}
There are two ways to make the valid string:
Remove characters with a frequency of : .
aabb or a and b
, we would need to remove two characters, both c and d
is invalid.
                          
 Remove characters of frequency : . Neither of these is an option.
Sample Input 2
           abcdefghhgfedecba
Sample Output 2
YES
Explanation 2
All characters occur twice except for valid string.
which occurs
times. We can delete one instance of
to have a
    
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.*;
import com.cswg.Util.UtilCommon;
import java.util.Map;
import java.util.HashMap;

public class HackerRankSherlockValidString {
	
	   static String isValid(String s) {

           Map<Character, Integer> characterCount = new HashMap<Character, Integer>();
           
           for( Character c: s.toCharArray() ){
        	   characterCount.put( c, characterCount.getOrDefault(c, 0) + 1 );
           }
           
           int min = 0;
           int max = 0;
           int maxCount = 0;
           int minCount = 0;
           int prev = 0;
           int distinctCounts = 1;
           //int sumOfValues = 0;
           
           characterCount.forEach( (key, value) -> System.out.println( key + " -> " + value ) );
           for( Character keys: characterCount.keySet() ){
        	   
               if ( prev == 0 ){
            	   min = characterCount.get(keys);
            	   max = characterCount.get(keys);
               }
            	   
        	   if ( prev != 0 && prev != characterCount.get(keys) )
        		   distinctCounts = 0;
        	   
        	   if ( max < characterCount.get(keys) ){
        		   max = characterCount.get(keys);
        		   maxCount = 0;
        		   maxCount++;
        	   }else if ( max == characterCount.get(keys) ){
          	      maxCount++;
               }
        	   
        	   if ( min > characterCount.get(keys) ){
        		   min = characterCount.get(keys);
        		   minCount = 0;
        		   minCount++;
        	   }else if ( min == characterCount.get(keys) ){
        		   minCount++;
        	   }
        	   
        	   prev = characterCount.get(keys);
        	   
        	   ///sumOfValues = sumOfValues + characterCount.get(keys) ;
           }
           System.out.println( min + " " + max + " " + minCount + " " + maxCount + " " + distinctCounts);
           
           if ( distinctCounts == 1 )
        	   return "YES";
           else {
        	   
        	   if ( minCount + maxCount != characterCount.size() )
        		   return "NO";
        	   
        	   if ( minCount == 1 && min == 1 )
        		   return "YES";
        	   
        	   if ( maxCount == 1 && ( max - min ) == 1)
        		   return "YES";
        	   
        	   else return "NO";
        		   
           }
        	   
        	   /*if ( maxCount > 1 && minCount > 1 )
        		   if ( max - min >= 1 )
            		   return "NO";
        		   else 
        			   return "YES";
        	   else
        		   if ( max - min >= 1 )
        			   return "NO";
        		   else
        			   return "YES"; 
        			   
        	    * if ( ( sumOfValues % characterCount.size() ) > 1 )
        	   return "NO";
           else return "YES";
        	   
        	    */
	    }

	    //private static final 

	    public static void main(String[] args) throws IOException {
	        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));
	    	System.out.println(UtilCommon.fileLocation);
	        try {
				System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankSherlockValid.txt"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        Scanner scanner = new Scanner(System.in);
	        String s = scanner.nextLine();
            System.out.println( s );
	        String result = isValid(s);
	        System.out.println( result );
	        //bufferedWriter.write(result);
	        //bufferedWriter.newLine();
	        //bufferedWriter.close();

	        scanner.close();
	    }
}
