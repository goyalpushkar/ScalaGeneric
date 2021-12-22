package com.cswg.practice;

import com.cswg.Util.UtilCommon;

/*
Insert a Node at the Tail of a Linked List
You are given the pointer to the head node of a linked list and an integer to add to the list. Create a new
node with the given integer. Insert this node at the tail of the linked list and return the head node of the
linked list formed after inserting this new node. The given head pointer may be null, meaning that the
initial list is empty.
Input Format
You have to complete the Node* Insert(Node* head, int data) method. It takes two arguments: the head
of the linked list and the integer to insert. You should not read any input from the stdin/console.
Output Format
Insert the new node at the tail and just return the head of the updated linked list. Do not print anything
to stdout/console.
Sample Input
NULL, data =
--> NULL, data =
Sample Output
2 -->NULL
2 --> 3 --> NULL
Explanation
1. We have an empty list, and we insert .
2. We start with a in the tail. When is inserted, then becomes the tail.
Video lesson
This challenge is part of a tutorial track byMyCodeSchool and is accompanied by a video lesson.
 */

/*
 * Insert a node at the head of a linked list
You�re given the pointer to the head node of a linked list and an integer to add to the list. Create a new
node with the given integer, insert this node at the head of the linked list and return the new head node.
The head pointer given may be null meaning that the initial list is empty.
Input Format
You have to complete the Node* Insert(Node* head, int data) method which takes two arguments - the
head of the linked list and the integer to insert. You should NOT read any input from stdin/console.
Output Format
Insert the new node at the head and return the head of the updated linked list. Do NOT print anything to
stdout/console.
Sample Input
NULL , data = 1
1 --> NULL , data = 2
Sample Output
1 --> NULL
2 --> 1 --> NULL
Explanation
1. We have an empty list, on inserting 1, 1 becomes new head.
2. We have a list with 1 as head, on inserting 2, 2 becomes the new head.
Video lesson
This challenge is part of a tutorial track byMyCodeSchool and is accompanied by a video lesson.
 */

/*
 * Insert a node at a specific position in a linked list
You�re given the pointer to the head node of a linked list, an integer to add to the list and the position at
which the integer must be inserted. Create a new node with the given integer, insert this node at the
desired position and return the head node.
A position of 0 indicates head, a position of 1 indicates one node away from the head and so on. The head
pointer given may be null meaning that the initial list is empty.
As an example, if your list starts as and you want to insert a node at position with
, your new list should be
Function Description Complete the function SinglyLinkedListNode in the editor. It must return a
reference to the head node of your finished list.
SinglyLinkedListNode has the following parameters:
head: a SinglyLinkedListNode pointer to the head of the list
data: an integer value to insert as data in your new node
position: an integer position to insert the new node, zero based indexing
Input Format
The first line contains an integer , the number of elements in the linked list.
Each of the next lines contains an integer node[i].data.
The last line contains an integer .
Constraints
, where is the element of the linked list.
.
Output Format
Return a reference to the list head. Locked code prints the list for you.
Sample Input
3
16
13
7
1
2
Sample Output
16 13 1 7
Explanation
The initial linked list is 16 13 7 . We have to insert 1 at the position 2 which currently has 7 in it. The
updated linked list will be 16 13 1 7
 */
import java.io.*;
import java.util.Scanner;

public class HackerRankInsertNodeAtTailHead {

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

        public SinglyLinkedList() {
            this.head = null;
        }
      
    }

    public static void printSinglyLinkedList(SinglyLinkedListNode node, String sep) throws IOException {  //, BufferedWriter bufferedWriter
        while (node != null) {
            //bufferedWriter.write(String.valueOf(node.data));
            System.out.print(String.valueOf(node.data));
            node = node.next;

            if (node != null) {
                //bufferedWriter.write(sep);
                System.out.print(sep);
            }
        }
    }
    
    // Complete the insertNodeAtTail function below.

    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     int data;
     *     SinglyLinkedListNode next;
     * }
     *
     */
    static SinglyLinkedListNode insertNodeAtTail(SinglyLinkedListNode head, int data) {
           
    	   SinglyLinkedListNode newNode = new SinglyLinkedListNode( data );
    	   
    	   if ( head == null ) {
    		   head = newNode;
    	   }else {
    		   SinglyLinkedListNode temp = head;
    		   while( temp.next != null ) {
    			   temp = temp.next;
    		   }
    		   temp.next = newNode;
    	   }
           
    	   
    	   return head;
    }
    
    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     int data;
     *     SinglyLinkedListNode next;
     * }
     *
     */
    static SinglyLinkedListNode insertNodeAtHead(SinglyLinkedListNode llist, int data) {
           
    	SinglyLinkedListNode head = new SinglyLinkedListNode(data);
        if ( llist == null ) {
        	return head;
        }else {
        	head.next = llist;
        }
        	
    	return head;
    }    

    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     int data;
     *     SinglyLinkedListNode next;
     * }
     *
     */
    static SinglyLinkedListNode insertNodeAtPosition(SinglyLinkedListNode head, int data, int position) {
    	   SinglyLinkedListNode newNode = new SinglyLinkedListNode( data );
    	 
    	   SinglyLinkedListNode temp = head;
    	   SinglyLinkedListNode prev = null;
    	   int start = 0;
    	   
    	   while( start != position ) {
    		   prev = temp;
    		   temp = temp.next;
    		   start = start + 1;
    	   }
    	   
    	   newNode.next = temp;
    	   
    	   if ( prev != null )
    		   prev.next = newNode;
    	   else
    		   head = newNode;
    	   
           return head;
    }    
    //private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));

        SinglyLinkedList llist = new SinglyLinkedList();
        System.out.println( UtilCommon.fileLocation  );
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation +  "HackerRankInsertNodePosition.txt"));  //"HackerRankInsertNode.txt"
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        
        int llistCount = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        for (int i = 0; i < llistCount; i++) {
          
            int llistItem = scanner.nextInt();
            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

          SinglyLinkedListNode llist_head = insertNodeAtTail(llist.head, llistItem);
          llist.head = llist_head;
          
          //SinglyLinkedListNode llist_head = insertNodeAtHead(llist.head, llistItem);
          //llist.head = llist_head;
          
        }

        int data = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

        int position = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
        
        SinglyLinkedListNode llist_head = insertNodeAtPosition(llist.head, data, position);
        
        
        printSinglyLinkedList(llist_head, "\n");  //llist.head
        //bufferedWriter.newLine();
        //bufferedWriter.close();
        scanner.close();
    }
    
}
