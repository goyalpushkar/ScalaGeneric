package com.cswg.practice;

/*
 *  Pairs
You will be given an array of integers and a target value. Determine the number of pairs of array elements that have a difference equal to a target value.
For example, given an array of [1, 2, 3, 4] and a target value of 1, we have three values meeting the condition: , , and .
Function Description
Complete the pairs function below. It must return an integer representing the number of element pairs having the required difference.
                  pairs has the following parameter(s): k: an integer, the target difference arr: an array of integers
Input Format
The first line contains two space-separated integers
The second line contains space-separated integers of the array arr.
Constraints
each integer will be unique
Output Format
An integer representing the number of pairs of integers whose difference is
Sample Input
52 15342
Sample Output
3
Explanation
and the target value.
  and , the size of
                                            .
   There are 3 pairs of integers in the set with a difference of 2: [5,3], [4,2] and [3,1] .

 */
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;

public class HackerRankPairs {

    // Complete the pairs function below.
    static int pairs(int k, int[] arr) {
         Map<Integer, Integer> arrItems = new HashMap<Integer, Integer>();
         int totalMatch = 0;
         for( int index = 0; index < arr.length; index++ ){
        	 int target1 = arr[index] + k;
        	 int target2 = arr[index] - k;
        	 
        	 if ( target1 > 0 ){
        		if ( arrItems.containsKey(target1) )
        			totalMatch++;
        	 }
        	 
        	 if ( target2 > 0 ){
        		 if ( arrItems.containsKey(target2) )
         			totalMatch++;
        	 }
        	 
        	 arrItems.put( arr[index], 1);
         }
         
         
         return totalMatch;

    }
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		   System.out.println(UtilCommon.fileLocation);
	        try {
				System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankPairs.txt"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        Scanner scanner = new Scanner(System.in);
	        String[] nk = scanner.nextLine().split(" ");

	        int n = Integer.parseInt(nk[0]);
	        int k = Integer.parseInt(nk[1]);

	        System.out.println( n + "  " + k);
	        int[] arr = new int[n];

	        String[] arrItems = scanner.nextLine().split(" ");
	        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

	        for (int i = 0; i < n; i++) {
	            int arrItem = Integer.parseInt(arrItems[i]);
	            arr[i] = arrItem;
	        }

	        int result = pairs(k, arr);
	        System.out.println(result);
	        scanner.close();
	}
}
