package com.cswg.practice

/*
 * Inserting a Node Into
a Sorted Doubly
Linked List
Given a reference to the head of a doubly-linked list and an integer, , create a new
DoublyLinkedListNode object having data value and insert it into a sorted linked list.
Complete the DoublyLinkedListNode SortedInsert(DoublyLinkedListNode head, int data) method in the
editor below. It has two parameters:
1. : A reference to the head of a doubly-linked list of Node objects.
2. : An integer denoting the value of the field for the Node you must insert into the list.
The method must insert a new Node into the sorted (in ascending order) doubly-linked list whose data
value is without breaking any of the list's double links or causing it to become unsorted.
Note: Recall that an empty list (i.e., where ) and a list with one element are sorted lists.
Input Format
The first line contains an integer , the number of test cases.
Each of the test case is in the following format:
The first line contains an integer , the number of elements in the linked list.
Each of the next lines contains an integer, the data for each node of the linked list.
The last line contains an integer which needs to be inserted into the sorted doubly-linked list.
Constraints
Output Format
Do not print anything to stdout. Your method must return a reference to the of the same list
that was passed to it as a parameter.
The ouput is handled by the code in the editor and is as follows:
For each test case, print the elements of the sorted doubly-linked list separated by spaces on a new line.
Sample Input
1
4
1
3
4
10
5
Sample Output
1 3 4 5 10
Explanation
The initial doubly linked list is: .
The doubly linked list after insertion is:
 */
object HackerRankInsertSortedDoublyList {
  

    class DoublyLinkedListNode( var data: Int, var next: DoublyLinkedListNode = null , var prev: DoublyLinkedListNode = null )
    
    class DoublyLinkedList( var head: DoublyLinkedListNode = null, var tail: DoublyLinkedListNode = null){
        def insertNode ( data: Int ) = {
            val newNode = new DoublyLinkedListNode( data )
            if ( head == null )
              this.head = newNode
            else{
              this.tail.next = newNode
              newNode.prev = this.tail 
            }
            this.tail = newNode
        }
        
    }
    
    def printDoublyLinkedList(head: DoublyLinkedListNode, sep: String) = {
        var node = head

        while (node != null) {
            print(node.data)
            node = node.next
            if (node != null) {
                print(sep)
            }
        }
    }
    // Complete the sortedInsert function below.

    /*
     * For your reference:
     *
     * DoublyLinkedListNode {
     *     data: Int
     *     next: DoublyLinkedListNode
     *     prev: DoublyLinkedListNode
     * }
     *
     */
    def sortedInsert(llist: DoublyLinkedListNode, data: Int): DoublyLinkedListNode = {

        import scala.util.control.Breaks._
        var temp = llist
        val newNode = new DoublyLinkedListNode(data)
        
        breakable{
            while( temp != null ) {
              //println( temp.data + "  " + ( if ( temp.prev != null ) temp.prev.data else null ) + "\n" )
              if ( temp.data < data ) {
                 if ( temp.next == null ){  //if it is the last node
                    newNode.next = temp.next
                    newNode.prev = temp
                    temp.next = newNode
                    break
                 }else{
                   temp = temp.next
                 }
                   
              }else{
                 newNode.next = temp
                 if ( temp.prev != null ){
                    newNode.prev =  temp.prev
                    temp.prev.next = newNode
                 }else{
                    newNode.prev =  null
                    return newNode
                 }
                 break
              }
            }
        }
        
        return llist
    }

    def main(args: Array[String]) {
      
        import java.io.FileInputStream
        val stdin = scala.io.StdIn
        System.out.println( fileLocation )
        System.setIn(new FileInputStream( fileLocation + "HackerRankInsertSortedDLL.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val t = stdin.readLine.trim.toInt
        println( t )
        for (tItr <- 1 to t) {
            val llist = new DoublyLinkedList()

            val llistCount = stdin.readLine.trim.toInt
            println( llistCount )
            for (_ <- 0 until llistCount) {
                val llistItem = stdin.readLine.trim.toInt
                llist.insertNode(llistItem)
            }

            val data = stdin.readLine.trim.toInt
            println(data)
            val llist1 = sortedInsert(llist.head, data)

            printDoublyLinkedList(llist1, " ")
            println()
        }

        //printWriter.close()
    }
}