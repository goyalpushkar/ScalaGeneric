package com.cswg.practice;

/*
 * Reverse a doubly
linked list
Youre given the pointer to the head node of a doubly linked list. Reverse the order of the nodes in the
list. The head node might be NULL to indicate that the list is empty. Change the next and prev pointers
of all the nodes so that the direction of the list is reversed. Return a reference to the head node of the
reversed list.
Function Description
Complete the reverse function in the editor below. It should return a reference to the head of your
reversed list.
reverse has the following parameter(s):
head: a reference to the head of a DoublyLinkedList
Input Format
The first line contains an integer , the number of test cases.
Each test case is of the following format:
The first line contains an integer , the number of elements in the linked list.
The next lines contain an integer each denoting an element of the linked list.
Constraints
Output Format
Return a reference to the head of your reversed list. The provided code will print the reverse array as a
one line of space-separated integers for each test case.
Sample Input
1
4
1
2
3
4
Sample Output
4 3 2 1
Explanation
The initial doubly linked list is:
The reversed doubly linked list is:
This challenge is part of a tutorial track byMyCodeSchool

 */

import java.io.*;
import java.util.Scanner;
import java.util.Stack;

import com.cswg.Util.UtilCommon;

public class HackerRankReverseDoublyLinkedList {

	   static class DoublyLinkedListNode {
	        public int data;
	        public DoublyLinkedListNode next;
	        public DoublyLinkedListNode prev;

	        public DoublyLinkedListNode(int nodeData) {
	            this.data = nodeData;
	            this.next = null;
	            this.prev = null;
	        }
	    }

	    static class DoublyLinkedList {
	        public DoublyLinkedListNode head;
	        public DoublyLinkedListNode tail;

	        public DoublyLinkedList() {
	            this.head = null;
	            this.tail = null;
	        }

	        public void insertNode(int nodeData) {
	            DoublyLinkedListNode node = new DoublyLinkedListNode(nodeData);

	            if (this.head == null) {
	                this.head = node;
	            } else {
	                this.tail.next = node;
	                node.prev = this.tail;
	            }

	            this.tail = node;
	        }
	    }

	    public static void printDoublyLinkedList(DoublyLinkedListNode node, String sep) throws IOException {  //, BufferedWriter bufferedWriter
	        while (node != null) {
	            //bufferedWriter.write(String.valueOf(node.data));
                System.out.print(String.valueOf(node.data));
	            node = node.next;

	            if (node != null) {
	                //bufferedWriter.write(sep);
	                System.out.print( sep );
	            }
	        }
	    }

	    // Complete the reverse function below.

	    /*
	     * For your reference:
	     *
	     * DoublyLinkedListNode {
	     *     int data;
	     *     DoublyLinkedListNode next;
	     *     DoublyLinkedListNode prev;
	     * }
	     *
	     */
	    static DoublyLinkedListNode reverse(DoublyLinkedListNode head) {
               
	    	DoublyLinkedListNode temp = head;
	    	DoublyLinkedListNode prev;
	    	Stack<Integer> stackNodes = new Stack<Integer>();
	    	
	    	while( temp != null ) {
	    		stackNodes.push( temp.data );
	    		temp = temp.next;
	    	}
	    	
	    	DoublyLinkedList llist = new DoublyLinkedList();
	    	while( !stackNodes.isEmpty() ) {
	    		llist.insertNode( stackNodes.pop() );
	    	}
	    	  
	    	return llist.head;
	    }

	    
    //private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));
        System.out.println( UtilCommon.fileLocation  );
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankReverseLinkedList.txt"));
			///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        
        int t = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
        
        System.out.println( t );
        for (int tItr = 0; tItr < t; tItr++) {
            DoublyLinkedList llist = new DoublyLinkedList();

            int llistCount = scanner.nextInt();
            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
            System.out.println( llistCount );
            for (int i = 0; i < llistCount; i++) {
                int llistItem = scanner.nextInt();
                scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

                llist.insertNode(llistItem);
            }

            DoublyLinkedListNode llist1 = reverse(llist.head);

            printDoublyLinkedList(llist1, " ");  //, bufferedWriter
            //bufferedWriter.newLine();
            System.out.println( "\n" );
        }

        //bufferedWriter.close();
        scanner.close();
    }
}
