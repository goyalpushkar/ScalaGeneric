package com.cswg.practice

class PracticeLinkedList {
  
}

object PracticeLinkedList {
  val linkedList = collection.mutable.LinkedList[Int](1,2,3,4,5)
  //linkedList.elem
  //linkedList.next
  
  //Write code to remove duplicates from an unsorted linked list
  def removeDuplicateList( valueList: collection.mutable.LinkedList[String] ):  collection.mutable.LinkedList[String] = {
        valueList.foreach{ x => println( "List - " + x.toString()) }
        
        //Way1
        var finalList = valueList
        var newList1 = valueList.next
         while ( !newList1.isEmpty ) {
              var secondList = valueList
              
              while ( secondList != newList1 ) {
                  if ( secondList.elem == newList1.elem ) {
                      //finalList.elem = secondList.next.elem
                      finalList.next = newList1.next
                      newList1 = newList1.next
                      //util.control.Breaks.break()
                     //secondList.next
                  }/*else{
                    finalList = secondList
                  }*/
                  secondList = secondList.next
              }
           
              if ( secondList == newList1 ){
                finalList = newList1
                newList1 = newList1.next
                //finalList.next
              }
         }
        
        //Way 2
        val duplicateValues: collection.mutable.Set[String] = collection.mutable.Set[String]()
        var newList: collection.mutable.LinkedList[String] = collection.mutable.LinkedList[String]()
        var nextList = valueList
        while( !nextList.isEmpty ) {
            if ( !duplicateValues.contains(nextList.elem) ) {
                duplicateValues.+=( nextList.elem )
                newList = newList.:+( nextList.elem )
            }
            nextList = nextList.next
        }
        
        newList.foreach{ println }
        
        return newList
        
  }
  //val valueList =  collection.mutable.LinkedList("1","2","3","4","5","6","7","8","2","4","1")
 //val removed = removeDuplicateList( valueList);
  
  //Implement an algorithm to find the nth to last element of a singly linked list.
  def findList( valToFind: Int, valueList: collection.mutable.LinkedList[String] ): collection.mutable.LinkedList[String] = {
        valueList.foreach{ x => "List - " + x.toString() }
        
        //Question 1 nth to Last
        /*var returnList = valueList
        for ( i <- 0 to valToFind - 1 ) {
            returnList = returnList.next
        }
        return returnList
        */
        
        //Question 2 nth from last to Last
        var list1 = valueList
        var list2 = valueList
        for ( i <- 1 to valToFind ){
           list1 = list1.next
        }
        
        while( !list1.isEmpty ){
          println( " list1 - " + list1.elem + " :List2 - " + list2.elem )
           list1 = list1.next
           list2 = list2.next
        }
        
        return list2
          
  }
  //val newList = findList( 5, valueList);
  
  //Implement an algorithm to delete a node in the middle of a single linked list, given only access to that node.
  /*EXAMPLE
    Input: the node ï¿½cï¿½ from the linked list a->b->c->d->e
    Result: nothing is returned, but the new linked list looks like a->b->d->e\*/
  def deleteInMiddle( valueList: collection.mutable.LinkedList[String] ): String = {
      if ( valueList == null || valueList.next == null ) {
         return "No"
      }
      
      val nextNode = valueList.next
      valueList.elem = nextNode.elem
      valueList.next = nextNode.next
      
      nextNode.foreach { println }
      
      return "Yes"
  }
  // val deleted = deleteInMiddle( valueList)
  
  //You have two numbers represented by a linked list, where each node contains a single digit. The digits are stored in reverse order, such that the 1ï¿½s digit is at the head of the list. Write a function that adds the two numbers and returns the sum as a linked list.
    /*EXAMPLE
    Input: (3 -> 1 -> 5) + (5 -> 9 -> 2)
    Output: 8 -> 0 -> 8*/
   def sumLinkedList( list1: collection.mutable.LinkedList[Int], list2: collection.mutable.LinkedList[Int], carry: Int ) : collection.mutable.LinkedList[Int] = {
       var summedValue = collection.mutable.LinkedList[Int]()
       //var carr = 0 
       //var value1 = 0
       
       var newList1 = list1
       var newList2 = list2
      // var carrier: Int = 0
      // var value1: Int = 0
       
      /* def sumList( value1: Int, value2: Int, carry: Int ): ( Int, Int ) = {
          var sumValue = value1 + value2 + carry
          var carryForward = carry
          if ( sumValue >= 10 ) {
          println("Greater than 10")
           sumValue = sumValue % 10
           carryForward =  1
          }
          
           return ( sumValue, carryForward )
       }
       
       while( !newList1.isEmpty || !newList2.isEmpty ) {
            var ( value1, carrier: Int ) = sumList( if ( newList1 == null ) 0 else newList1.elem, if ( newList2 == null ) 0 else newList2.elem, carrier )   
            println( "Returned - " + value1 + " " + carrier )           
            summedValue = summedValue.:+( value1 )
            //carr = carrier
                       
           newList1 = newList1.next
           newList2 = newList2.next
           
           //When lists are finished save carrier in the list if it is greater than 0
           if ( newList1.isEmpty  && newList2.isEmpty ) {
              if ( carrier >  0 ) 
                summedValue = summedValue.:+( carrier )
            }
       }*/
       
       return summedValue
   }
  //val list1 = collection.mutable.LinkedList( 5, 3, 2, 9 )
  //val list2 = collection.mutable.LinkedList( 4, 8, 8 )
  // val summ = sumLinkedList( list1, list2, 0 );
   
  /*
   * Given a circular linked list, implement an algorithm which returns node at the beginning of the loop.
      DEFINITION
      Circular linked list: A (corrupt) linked list in which a nodeï¿½s next pointer points to an earlier node, so as to make a loop in the linked list.
      EXAMPLE
      input: A -> B -> C -> D -> E -> C [the same C as earlier]
      output: C
   */
   def circularList( valueList: collection.mutable.LinkedList[String] ) : String = {
       var valueListNew = valueList
       
       //Way 1
       /*val values: collection.mutable.Map[Int, Int] = collection.mutable.Map[Int, Int]()
       while( valueListNew != null ) {
            if ( !values.contains( valueListNew.elem ) ) {
              values.put( valueListNew.elem, 1 ) 
            }else{
              return values.getOrElse( valueListNew.elem, 0)
            }
       }*/
       
       //Way2
       var newList = valueList
       while ( !newList.isEmpty ) {
            var secondList = valueList
            
            while ( secondList != newList ) {
                if ( secondList.elem == newList.elem ) {
                   return secondList.elem
                   //secondList.next
                }
                secondList = secondList.next
              
            }
         
            newList = newList.next
       }
       
       return "0"
       
   }
   //val circ = circularList( valueList);
   
   //
   //Insert an element in sortedLinkedList

   class ListNode(var _x: Int = 0) {
    var next: ListNode = null
    var x: Int = _x
  }

    def reverseList(head: ListNode): ListNode = {
        var newHead: ListNode = head
        var prev: ListNode = null;
        var current: ListNode = head;
        var next: ListNode = null;
        while (current != null) {
            next = current.next;
            current.next = prev;
            prev = current;
            current = next;
        }
        newHead = prev;
        return newHead;
    }
    /* 
var listOfNodes = new ListNode( 4)
listOfNodes.next = new ListNode( 85 )
listOfNodes.next.next = new ListNode( 51 )
listOfNodes.next.next.next = new ListNode( 32 )
listOfNodes.next.next.next.next = new ListNode( 67 )
*/
    // val newHead = reverseList( listOfNodes )
    
  class node( value: Int ) {
    var next: node = null
    var prev: node = null
    var x: Int = value
  }
  
  class MyLinkedList(value: Int) {

    /** Initialize your data structure here. */
    var head: node = null
    
    def MyLinkedListInit(value: Int) = {
       println( "Head Value - " + ( if ( head != null ) head.x else null ) + " :head.next - " + ( if ( head != null ) head.next.x else null ) 
              +  " :head.prev - " + ( if ( head != null ) head.prev.x else null )
              )
        val newNode = new node( value )
        if ( head == null ){
           newNode.next = null
           newNode.prev = null 
           head = newNode
        }else{
          addAtTail(value)
        }
    }

    //Call constructor method
    MyLinkedListInit(value) 
    
    /** Get the value of the index-th node in the linked list. If the index is invalid, return -1. */
    def get(index: Int): Int = {
      import util.control.Breaks._
      
      var next = head
      var broken = 0
      breakable{
          for ( i <- 1 to index - 1){
            if ( next.next != null )
                next = next.next
              else{
                broken = 1
                break()
            }
          }
      }
      
      if ( broken == 0 )
        return next.x
      else 
        return -1
    }

    /** Add a node of value val before the first element of the linked list. After the insertion, the new node will be the first node of the linked list. */
    def addAtHead(value: Int) {
        println( "Head Value - " + ( if ( head != null ) head.x else null ) + " :head.next - " + ( if ( head != null ) head.next else null ) 
              +  " :head.prev - " + ( if ( head != null ) head.prev else null )
              )
        val newNode = new node( value )
        if ( head == null ){
           newNode.next = null
           newNode.prev = null 
           head = newNode
        }else{
           newNode.next = head
           newNode.prev = null
           head.prev = newNode
           head = newNode
        }
          
    }

    /** Append a node of value val to the last element of the linked list. */
    def addAtTail(value: Int) {
        println( "Head Value - " + ( if ( head != null ) head.x else null ) + " :head.next - " + ( if ( head != null ) head.next else null ) 
              +  " :head.prev - " + ( if ( head != null ) head.prev else null )
              )
        val newNode = new node( value )
        var next = head
        while( next.next != null ){
           next = next.next
        }
       if ( next.next == null ) {
         newNode.prev = next
         newNode.next = next.next
         next.next = newNode
       }
    }

    /** Add a node of value val before the index-th node in the linked list. If index equals to the length of linked list, the node will be appended to the end of linked list. If index is greater than the length, the node will not be inserted. */
    def addAtIndex(index: Int, value: Int) {
        import util.control.Breaks._
        
        println( "Head Value - " + ( if ( head != null ) head.x else null ) + " :head.next - " + ( if ( head != null ) head.next else null ) 
              +  " :head.prev - " + ( if ( head != null ) head.prev else null )
              )
        
        val newNode = new node( value )
        var next = head
        var broken = 0
        breakable{
          for( i <- 1 to index - 1  ) {
            if ( next.next != null ) 
               next = next.next  
            else{
              broken = 1
              break()
            }
          }
        }
        if ( broken == 0 ){
          newNode.prev = next
          newNode.next = next.next
          next.next = newNode
          if ( next.next != null )
            next.next.prev = newNode
        }
    }

    /** Delete the index-th node in the linked list, if the index is valid. */
    def deleteAtIndex(index: Int) {
      import util.control.Breaks._
      
        var next = head
        var broken = 0
        breakable{
          for( i <- 1 to index - 1  ) {
            if ( next.next != null ) 
               next = next.next  
            else{
              broken = 1
              break()
            }
          }
        }
        if ( broken == 0 ){
          next.prev.next = null
          next.prev = null
          //next.next.prev = null
          //next.next = null
        }
    }
    
    def printList = {
        var next  = head
        while ( next != null ){
             println ( next.x )
             next =  next.next
        }
    }

  }
  val newLinkedList = new MyLinkedList(5)
  newLinkedList.addAtHead(7)
  newLinkedList.get(2) 
  newLinkedList.get(5)
  newLinkedList.addAtTail(89)
  newLinkedList.addAtIndex(3, 11)
  newLinkedList.deleteAtIndex(2)
  newLinkedList.printList
  
}