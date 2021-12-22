package com.cswg.practice


/*
 * Print in Reverse
You are given the pointer to the head node of a linked list and you need to print all its elements in reverse
order from tail to head, one element per line. The head pointer may be null meaning that the list is empty
- in that case, do not print anything!
Input Format
You have to complete the void ReversePrint(Node* head) method which takes one argument - the head
of the linked list. You should NOT read any input from stdin/console.
Output Format
Print the elements of the linked list in reverse order to stdout/console (using printf or cout) , one per line.
Sample Input
1 --> 2 --> NULL
2 --> 1 --> 4 --> 5 --> NULL
Sample Output
2
1
5
4
1
2
Explanation
1. First list is printed from tail to head hence 2,1
2. Similarly second list is also printed from tail to head.
Video lesson
This challenge is part of a tutorial track byMyCodeSchool and is accompanied by a video lesson.
 */

import java.io._

object HackerRankLinkedListPrintReverse {

      class SinglyLinkedListNode(var data: Int, var next: SinglyLinkedListNode = null) {
       }

    class SinglyLinkedList(var head: SinglyLinkedListNode = null, var tail: SinglyLinkedListNode = null) {
        def insertNode(nodeData: Int) = {
            val node = new SinglyLinkedListNode(nodeData)

            if (this.head == null) {
                this.head = node
            } else {
                this.tail.next = node
            }

            this.tail = node
        }
    }

    def printSinglyLinkedList(head: SinglyLinkedListNode, sep: String) = {
        var node = head

        while (node != null) {
            print(node.data)

            node = node.next

            if (node != null) {
                print(sep)
            }
        }
    }
    
       // Complete the reversePrint function below.

    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     data: Int
     *     next: SinglyLinkedListNode
     * }
     *
     */
    def reversePrint(llist: SinglyLinkedListNode) {
        import scala.collection.mutable.Stack
        
        val storedList = Stack[Int]();
        var temp = llist
        while( temp != null ){
            storedList.push( temp.data )
            temp = temp.next
        }
        
        while( ! storedList.isEmpty ){
           println( storedList.pop() ) 
        }

    }
    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.out.println( fileLocation )
        System.setIn(new FileInputStream( fileLocation + "HackerRankLLPrintReverse.txt"));
        
        val tests = stdin.readLine.trim.toInt
        System.out.println( tests )
        for (testsItr <- 1 to tests) {
            val llist = new SinglyLinkedList()

            val llistCount = stdin.readLine.trim.toInt
            System.out.println( llistCount )
            for (_ <- 0 until llistCount) {
                val llistItem = stdin.readLine.trim.toInt
                llist.insertNode(llistItem)
            }
            System.out.println( "\n" + "Print in Order" )
            printSinglyLinkedList(llist.head, "\n" )
            System.out.println( "\n" + "Print in Reverse Order" )
            reversePrint(llist.head)
        }
    }    
}