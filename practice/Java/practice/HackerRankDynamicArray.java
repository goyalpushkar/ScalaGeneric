package com.cswg.practice;

/*
 *  Dynamic Array
Create a list, , of elements within each of the
Create an integer,
empty sequences, where each sequence is indexed from to . The sequences also use -indexing.
Output Format
For each type
Sample Input
25 105 117 103 210 211
query, print the updated value of
on a new line.
Sample Output
7
3. Print the new value of
, , and queries, execute each query.
, and initialize it to .
The types of queries that can be performed on your list of sequences ( below:
) are described
.
.
) and assign it to
1. Query: 1 x y
1. Find the sequence, , at index
2. Append integer to sequence
2. Query: 2 x y
1. Find the sequence, , at index
2. Find the value of element .
.
in
in
is the size of
in
on a new line
Task
Given
Input Format
The first line contains two space-separated integers, (the number of sequences) and (the number of queries), respectively.
Each of the subsequent lines contains a query in the format defined above.
Constraints
It is guaranteed that query type will never query an empty sequence or index.
is the bitwise XOR operation, which corresponds to the ^ operator in most languages. Learn more about it on Wikipedia.
Note:
(where

 3
Explanation
Initial Values:
=[] =[]
Query 0:
= [5] =[]
Query 1: = [5] = [7]
Query 2:
= [5, = [7]
Query 3: .
= [5, = [7]
7
Query 4: .
= [5, = [7]
3
Append
Append
Append 3]
to sequence
to sequence
to sequence
.
.
.
Assign the value at index 3]
Assign the value at index 3]
of sequence
of sequence
to , print
to , print

 */
import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;


public class HackerRankDynamicArray {
	
	public static int findIndex( int value, int lastAnswer ){
		return value^lastAnswer;
	}
	
	
    // Complete the dynamicArray function below.
    static List<Integer> dynamicArray(int n, List<List<Integer>> queries) {
    	List<Integer> seqList = new ArrayList<Integer>();
        List<List<Integer>> seqListList = new ArrayList<List<Integer>>();
    	
       for( int index = 0; index < n; index++){
    	   seqListList.add( index, new ArrayList<Integer>());
       }
       
       //System.out.println( seqListList.size() );
    	int lastAnswer = 0;
    	for ( int index = 0; index < queries.size(); index ++ ){
    		List<Integer> queryList = queries.get(index);
    		int seqIndex = ( findIndex( queryList.get(1), lastAnswer) % n );
    		List<Integer> seq = seqListList.get(seqIndex);
    		//System.out.print( queryList.get(0) + " " +  queryList.get(1) + " " +  queryList.get(2) + "\t" + seqIndex + "\n");

    		if ( queryList.get(0) == 1 ){
    			seq.add(queryList.get(2));
    			seqListList.set(seqIndex, seq);
    		}else{
    			if ( seq.isEmpty() )
    				lastAnswer = 0;
    			else 
    				lastAnswer = seq.get( queryList.get(2) % seq.size() );
    			
    			//System.out.println( "last_answer - " +  lastAnswer);
    			seqList.add(lastAnswer);
    		}
    			    			
    	}
    	
    	return seqList;

    }

    public static void main(String[] args) throws IOException {
    	try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankDynamicArray.txt"));
					// "C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankIsBST.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));
        String[] nq = bufferedReader.readLine().replaceAll("\\s+$", "").split(" ");

        int n = Integer.parseInt(nq[0]);
        int q = Integer.parseInt(nq[1]);

        List<List<Integer>> queries = new ArrayList<>();

        for (int i = 0; i < q; i++) {
            String[] queriesRowTempItems = bufferedReader.readLine().replaceAll("\\s+$", "").split(" ");

            List<Integer> queriesRowItems = new ArrayList<>();

            for (int j = 0; j < 3; j++) {
                int queriesItem = Integer.parseInt(queriesRowTempItems[j]);
                queriesRowItems.add(queriesItem);
            }

            queries.add(queriesRowItems);
        }

        System.out.print("n - " + n + "\n");
        //for( queries.forEach{print};
        
        List<Integer> result = dynamicArray(n, queries);

        for (int i = 0; i < result.size(); i++) {
            //bufferedWriter.write(String.valueOf(result.get(i)));
        	System.out.print( String.valueOf(result.get(i)) );

            if (i != result.size() - 1) {
                //bufferedWriter.write("\n");
            	System.out.print("\n");
            }
        }

        //bufferedWriter.newLine();
        System.out.print("\n");
        bufferedReader.close();
        //bufferedWriter.close();
    }
}
