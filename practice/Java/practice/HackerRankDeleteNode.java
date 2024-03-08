package com.cswg.practice;

/*
 * Delete a Node
Youre given the pointer to the head node of a linked list and the position of a node to delete. Delete the
node at the given position and return the head node. A position of 0 indicates head, a position of 1
indicates one node away from the head and so on. The list may become empty after you delete the node.
Input Format
You have to complete the Node* Delete(Node* head, int position) method which takes two arguments -
the head of the linked list and the position of the node to delete. You should NOT read any input from
stdin/console. position will always be at least 0 and less than the number of the elements in the list.
Output Format
Delete the node at the given position and return the head of the updated linked list. Do NOT print
anything to stdout/console.
Sample Input
1 --> 2 --> 3 --> NULL, position = 0
1 --> NULL , position = 0
Sample Output
2 --> 3 --> NULL
NULL
Explanation
1. 0th position is removed, 1 is deleted from the list.
2. Again 0th position is deleted and we are left with empty list.
Video lesson
This challenge is part of a tutorial track byMyCodeSchool and is accompanied by a video lesson.
 */

import java.io.*;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;

public class HackerRankDeleteNode {
	
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

    public static void printSinglyLinkedList(SinglyLinkedListNode node, String sep) throws IOException { //, BufferedWriter bufferedWriter
        while (node != null) {
            //bufferedWriter.write(String.valueOf(node.data));
            System.out.print( String.valueOf(node.data) );
            node = node.next;

            if (node != null) {
                //bufferedWriter.write(sep);
                System.out.print( sep );
            }
        }
    }

    // Complete the deleteNode function below.

    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     int data;
     *     SinglyLinkedListNode next;
     * }
     *
     */
    static SinglyLinkedListNode deleteNode(SinglyLinkedListNode head, int position) {

    		int start = 0;
    		SinglyLinkedListNode temp = head;
    		SinglyLinkedListNode prev = null;
    		
    		while ( start != position ) {
    			prev = temp;
    			temp = temp.next;
    			start = start + 1;
    		}
    		
    		if ( prev == null  ) 
    			head = head.next;
    			//head = null;
    		else 
    			prev.next = temp.next;

    		temp = null;
    		return head;
    }
    
    //private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));

        SinglyLinkedList llist = new SinglyLinkedList();
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation +  "HackerRankDeleteNode.txt"));  //"HackerRankInsertNode.txt"
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        
        int llistCount = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
        System.out.println(llistCount);
        for (int i = 0; i < llistCount; i++) {
            int llistItem = scanner.nextInt();
            scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

            llist.insertNode(llistItem);
        }

        System.out.println("List before deletion");
        printSinglyLinkedList(llist.head, " ");
        
        int position = scanner.nextInt();
        scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");
        System.out.println( "\n" + position);
        SinglyLinkedListNode llist1 = deleteNode(llist.head, position);

        printSinglyLinkedList(llist1, " ");//bufferedWriter
        //bufferedWriter.newLine();
        //bufferedWriter.close();

        scanner.close();
    }    
}
