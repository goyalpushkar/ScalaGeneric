package com.cswg.practice

/*
 * Reverse a linked list
Youre given the pointer to the head node of a linked list. Change the next pointers of the nodes so that
their order is reversed. The head pointer given may be null meaning that the initial list is empty.
Input Format
You have to complete the Node* Reverse(Node* head) method which takes one argument - the head of
the linked list. You should NOT read any input from stdin/console.
Output Format
Change the next pointers of the nodes that their order is reversed and return the head of the reversed
linked list. Do NOT print anything to stdout/console.
Sample Input
NULL
2 --> 3 --> NULL
Sample Output
NULL
3 --> 2 --> NULL
Explanation
1. Empty list remains empty
2. List is reversed from 2,3 to 3,2
Video lesson
This challenge is part of a tutorial track byMyCodeSchool and is accompanied by a video lesson.
 */
import java.io._

object HackerRankReverseLinkedList {
  
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

    def printSinglyLinkedList (head: SinglyLinkedListNode, sep: String) = {  //, printWriter: PrintWriter
        var node = head

        while (node != null) {
            print(node.data)
            //printWriter
            node = node.next

            if (node != null) {
                print(sep)
                //printWriter
            }
        }
    }

    // Complete the reverse function below.

    /*
     * For your reference:
     *
     * SinglyLinkedListNode {
     *     data: Int
     *     next: SinglyLinkedListNode
     * }
     *
     */
    def reverse(llist: SinglyLinkedListNode): SinglyLinkedListNode = {
        import scala.collection.mutable.Stack
        
        val storedList = Stack[Int]();
        val newList = new SinglyLinkedList()
        var temp = llist
        while( temp != null ){
            storedList.push( temp.data )
            temp = temp.next
        }
        
        while( ! storedList.isEmpty ){
           newList.insertNode(storedList.pop())
        }
        
        return newList.head

    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.out.println( fileLocation )
        System.setIn(new FileInputStream( fileLocation + "HackerRankReverseLinkedList.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

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

            val llist1 = reverse(llist.head)

            printSinglyLinkedList(llist1, " ")  //, printWriter
            System.out.println( "\n" )
            //printWriter.println()
        }

        //printWriter.close()
    }
}