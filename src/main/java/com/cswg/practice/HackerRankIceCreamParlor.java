package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import com.cswg.Util.UtilCommon;

/*
 *  Hash Tables: Ice
Cream Parlor
Each time Sunny and Johnny take a trip to the Ice Cream Parlor, they pool their money to buy ice cream. On any given day, the parlor offers a line of flavors. Each flavor has a cost associated with it.
Given the value of and the of each flavor for trips to the Ice Cream Parlor, help Sunny and Johnny choose two distinct flavors such that they spend their entire pool of money during each visit. ID numbers are the 1- based index number associated with a . For each trip to the parlor, print the ID numbers for the two types of ice cream that Sunny and Johnny purchase as two space-separated integers on a new line. You must print the smaller ID first and the larger ID second.
For example, there are flavors having spend. They would purchase flavor ID's and response.
Note: Two ice creams having unique IDs and
Function Description
. Together they have to for a cost of . Use based indexing for your
may have the same cost (i.e., ).
Complete the function whatFlavors in the editor below. It must determine the two flavors they will
purchase and print them as two space-separated integers on a line. whatFlavors has the following parameter(s):
cost: an array of integers representing price for a flavor
money: an integer representing the amount of money they have to spend
Input Format
The first line contains an integer, , the number of trips to the ice cream parlor. Each of the next sets of lines is as follows:
The first line contains .
The second line contains an integer, , the size of the array The third line contains space-separated integers denoting the
Constraints
There will always be a unique solution.
Output Format
.
.
Print two space-separated integers denoting the respective indices for the two distinct flavors they choose to purchase in ascending order. Recall that each ice cream flavor has a unique ID number in the inclusive range from to .
Sample Input

  2
4
5 14532 4
4 2243
Sample Output
14 12
Explanation
Sunny and Johnny make the following two trips to the parlor:
 1. The first time, they pool together flavors and have a total cost of
2. The second time, they pool together and flavors and have a total cost of
dollars. There are five flavors available that day and .
dollars. There are four flavors available that day .
                            
 */
public class HackerRankIceCreamParlor {

    // Complete the whatFlavors function below.
    static void whatFlavorsTimedOut(int[] cost, int money) {

          for( int start = 0; start < cost.length; start++){
        	  for( int end = cost.length - 1; end > start; end-- ){
        		  if ( cost[start] + cost[end] == money ){
        			  System.out.println( ( start + 1 ) + " " + ( end + 1 ) );
        			  return;
        		  }
        	  }
          }
    }
    
    //2 Sum Way
    static void whatFlavors(int[] cost, int money) {

    	Map<Integer, Integer> indexTrace = new HashMap<Integer, Integer>();
        //for( int start = 0; start < cost.length; start++){
        for( int start = cost.length - 1; start >= 0 ; start--){
      	     int requiredValue = money - cost[start];
      	     if( indexTrace.containsKey(requiredValue) ){
      	    	System.out.println( ( start + 1 ) + " " + ( indexTrace.get(requiredValue) + 1 ) );
  			    return;	 
      	     }
      	     
      	   indexTrace.put(cost[start], start );
      	     
        }
    }
    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		   System.out.println(UtilCommon.fileLocation);
	        try {
				System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankIceCreamParlor.txt"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        Scanner scanner = new Scanner(System.in);
	        int t = scanner.nextInt();
	        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
            System.out.println(t);
	        for (int tItr = 0; tItr < t; tItr++) {
	            int money = scanner.nextInt();
	            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

	            int n = scanner.nextInt();
	            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

	            int[] cost = new int[n];

	            String[] costItems = scanner.nextLine().split(" ");
	            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
                System.out.println( money + " " + n);
	            for (int i = 0; i < n; i++) {
	                int costItem = Integer.parseInt(costItems[i]);
	                cost[i] = costItem;
	            }

	            whatFlavors(cost, money);
	        }
	        
	        scanner.close();
	}

}
