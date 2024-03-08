package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;

/*
 * Find Merge Point of
Two Lists
This challenge is part of a tutorial track by MyCodeSchool
Given pointers to the head nodes of linked lists that merge together at some point, find the Node where
the two lists merge. It is guaranteed that the two head Nodes will be different, and neither will be NULL.
In the diagram below, the two lists converge at Node x :
[List #1] a--->b--->c
\
x--->y--->z--->NULL
/
[List #2] p--->q
Complete the int FindMergeNode(Node* headA, Node* headB) method so that it finds and returns the
data value of the Node where the two lists merge.
Input Format
Do not read any input from stdin/console.
The FindMergeNode(Node*,Node*) method has two parameters, and , which are the nonnull head Nodes of two separate linked lists that are guaranteed to converge.
Constraints
The lists will merge.
.
.
Output Format
Do not write any output to stdout/console.
Each Node has a data field containing an integer. Return the integer data for the Node where the two lists
merge.
Sample Input
The diagrams below are graphical representations of the lists that input Nodes and are
connected to. Recall that this is a method-only challenge; the method only has initial visibility to those
Nodes and must explore the rest of the Nodes using some algorithm of your own design.
Test Case 0
1
\
2--->3--->NULL
/
1
Test Case 1
1--->2
\
3--->Null
/
1
Sample Output
2
3
Explanation
Test Case 0: As demonstrated in the diagram above, the merge Node's data field contains the integer .
Test Case 1: As demonstrated in the diagram above, the merge Node's data field contains the integer .
 */

public class HackerRankFindMerge2LL {

	public static class MergeNode {
		 int data;
		 MergeNode next;
		 
		 MergeNode(int value ) {
			 this.data = value;
			 this.next = null;
		 }
	}
	
	public static class MergedLinkedList{
		MergeNode head;
		MergeNode tail;
		
		MergedLinkedList() {
			this.head = null;
			this.tail = null;
		}
		
		public void insert( int data ) {
			MergeNode newNode = new MergeNode( data );
			 if ( head == null ) {
			     this.head = newNode;
			 }else {
				 this.tail.next = newNode;
			 }
			 
			 this.tail = newNode;

		}
	}
	
    // Complete the findMergeNode function below.

    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     int data;
     *     SinglyLinkedListNode next;
     * }
     *
     */
    static int findMergeNode(MergeNode head1, MergeNode head2) {

    	MergeNode pointer1 = head1;
    	MergeNode pointer2 = head2;
    	
    	//This is wrong as if one list is small and other is big then they will never reach to an agreement
    	/*while( pointer1 != pointer2 ) {
    		//System.out.println("Before Assignement - " + pointer1.data + " " + pointer2.data );
    		if ( pointer1.next != null )
    			pointer1 = pointer1.next;
    		
    		if ( pointer2.next != null )
    			pointer2 = pointer2.next;
    		
    		//System.out.println("After Assignement - " + pointer1.data + " " + pointer2.data );
    	}*/
    	
    	while( pointer1.next != null ) {
    		//System.out.println("Before Assignement - " + pointer1.data + " " + pointer2.data );
    		
    		while( pointer2.next != null ) {
    			if ( pointer1 == pointer2 )
    				return pointer1.data;
    			else
    				pointer2 = pointer2.next;
    		}
    		pointer2 = head2;
    		pointer1 = pointer1.next;
    		//System.out.println("After Assignement - " + pointer1.data + " " + pointer2.data );
    	}

    	return pointer1.data;
    }
    
	public static void main( String[] args ) {
		
		System.out.println( UtilCommon.fileLocation  );
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankFindMerge.txt"));
			///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        
        int t = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
        System.out.println( t );
        for( int testIndex = 0; testIndex < t; testIndex++ ) {
        	int index = scanner.nextInt();
            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        	MergedLinkedList llist1 = new MergedLinkedList();
        	int nodes1 = scanner.nextInt();
        	System.out.println( nodes1 );
            for( int nodeIndex = 0; nodeIndex < nodes1; nodeIndex++ ) {
            	int llist1Item = scanner.nextInt();
                scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
                llist1.insert(llist1Item);
            }
            
            MergedLinkedList llist2 = new MergedLinkedList();
        	int nodes2 = scanner.nextInt();
        	System.out.println( nodes2 );
            for( int nodeIndex = 0; nodeIndex < nodes2; nodeIndex++ ) {
            	int llist2Item = scanner.nextInt();
                scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
                llist2.insert(llist2Item);
            }
            
            MergeNode ptr1 = llist1.head;
            MergeNode ptr2 = llist2.head;

            System.out.println( index );
            for (int i = 0; i < nodes1; i++) {
                if (i < index) {
                    ptr1 = ptr1.next;
                }
            }

            for (int i = 0; i < nodes2; i++) {
                if (i != nodes2-1) {
                    ptr2 = ptr2.next;
                }
            }

            ptr2.next = ptr1;

            int result = findMergeNode(llist1.head, llist2.head);
        	
            System.out.println("Result - " + result);
        }
        
	}
	
	
}
