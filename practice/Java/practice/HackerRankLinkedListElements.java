package com.cswg.practice;

/*
 * Print the Elements of
a Linked List
This challenge is part of a MyCodeSchool tutorial track and is accompanied by a video lesson.
If you're new to linked lists, this is a great exercise for learning about them. Given a pointer to the head
node of a linked list, print its elements in order, one element per line. If the head pointer is null (indicating
the list is empty), dont print anything.
Input Format
The first line of input contains , the number of elements in the linked list. The next lines contain one
element each, which are the elements of the linked list.
Note: Do not read any input from stdin/console. Complete the printLinkedList function in the editor below.
Constraints
, where is the element of the linked list.
Output Format
Print the integer data for each element of the linked list to stdout/console (e.g.: using printf, cout, etc.).
There should be one element per line.
 */

import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import com.cswg.Util.UtilCommon;

public class HackerRankLinkedListElements {

	   static class SinglyLinkedListNode {
	        public int data;
	        public SinglyLinkedListNode next;

	        public SinglyLinkedListNode(int nodeData) {
	            this.data = nodeData;
	            this.next = null;
	        }
	    }

	    static class SinglyLinkedList {
	        public SinglyLinkedListNode head;
	        public SinglyLinkedListNode tail;

	        public SinglyLinkedList() {
	            this.head = null;
	            this.tail = null;
	        }

	        public void insertNode(int nodeData) {
	            SinglyLinkedListNode node = new SinglyLinkedListNode(nodeData);

	            if (this.head == null) {
	                this.head = node;
	            } else {
	                this.tail.next = node;
	            }

	            this.tail = node;
	        }
	    }

	    // Complete the printLinkedList function below.

	    /*
	     * For your reference:
	     *
	     * SinglyLinkedListNode {
	     *     int data;
	     *     SinglyLinkedListNode next;
	     * }
	     *
	     */
	    static void printLinkedList(SinglyLinkedListNode head) {
               
	    	SinglyLinkedListNode start = head;
	    	System.out.println ( "Start Empty - " + ( start != null ));
	    	 
	        while( start != null ) {
	        	System.out.println( start.data );
	        	start = start.next;
	        }
	        //System.out.println( start.data );
	    }

	    //private static final Scanner scanner = new Scanner(System.in);

	    public static void main(String[] args) {
	        SinglyLinkedList llist = new SinglyLinkedList();
            System.out.println( UtilCommon.fileLocation  );
	        try {
				System.setIn(new FileInputStream( UtilCommon.fileLocation + "HackerRankLinkedList.txt"));
				///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        Scanner scanner = new Scanner(System.in);
	        int llistCount = scanner.nextInt();
	        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
            System.out.println( "No of Elements - " + llistCount );
	        for (int i = 0; i < llistCount; i++) {
	            int llistItem = scanner.nextInt();
	            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

	            llist.insertNode(llistItem);
	        }

	        printLinkedList(llist.head);

	        scanner.close();
	    }	
}
