package com.cswg.practice

class PracticeQueueStack {
  
}

object PracticeQueueStack {
 
  val queue = collection.mutable.Queue[Int](1,2,3,4,5)
  //queue.front 
  //queue.tail 
  //queue.extractFirst( linkedList.front, (_==4) )
  
  //Describe how you could use a single array to implement three stacks.
  def threeStacks(stackSize: Int) = {
      val stackM = collection.mutable.Stack(1, 2, 3)
      val stackIm = collection.immutable.Stack(1, 2, 3)

      val array = Array[String]()
      val arrayStack = Array(0, 0, 0)
      
      def push( arrayNum: Int, value: String )= {
          val index = arrayNum * stackSize + arrayStack.apply( arrayNum )  
          arrayStack.update( arrayNum, arrayStack.apply( arrayNum ) + 1 )
          array.update( index, value )
      }
      
      def pop( arrayNum: Int ): String = {
          val index = arrayNum * stackSize + arrayStack.apply( arrayNum ) - 1
          arrayStack.update( arrayNum, arrayStack.apply( arrayNum ) - 1 )
           val returnval = array.apply( index )
          array.update( index, null )
          return returnval
      }
      
      def peek( arrayNum: Int ): String = {
          val index = arrayNum * stackSize + arrayStack.apply( arrayNum ) - 1
          return array.apply( index )
      }
      
      def isEmpty( arrayNum: Int ): Boolean = {
           if ( arrayStack.apply( arrayNum ) == 0 )
             return true
           else return false
      }
  }
  
  /*
   * How would you design a stack which, in addition to push and pop, also has a function min which returns the minimum element? Push, pop and min should all operate in O(1) time.
   */
  //Way 1
   class stackWithMin() {
      case class stackNode (  value: Int,  min: Int ) 
      
      val arrayStack = collection.immutable.Stack[stackNode]()
      
      def push ( value: Int ) = {
          var min = 0
          
          if ( arrayStack.head.min < value ) 
             min = arrayStack.head.min
          else 
             min = value
          //arrayStack.foreach{ x => if ( x.min < value ) min = x.min else min = value }
          val newNode = stackNode.apply( value, min) 
          arrayStack.push( newNode )
          
      }
      
      def pop (): stackNode = {
          val returnValue = arrayStack.top
          //There is an issue if popped value is minimum value then we are keeping wrong min value
          arrayStack.pop
          return returnValue
      }
      
      def min (): Int = {
          return arrayStack.head.min
      }
      
   }
   
   //Way 2
   class stackWithMin2() {
      var minValue = collection.mutable.ArrayBuffer[Int]()  // ArrayBuffer has constant append, update and head
      val arrayStack = collection.immutable.Stack[Int]()
      
      def push ( value: Int ) = {
         if ( minValue.isEmpty ){
            minValue.++=( Seq(value) )
         }else{
          if ( value < minValue.head ) 
              minValue.update(0, value )
           else
              minValue.update(1, value )
         }
          arrayStack.push( value )
          
      }
      
      def pop (): Int = {
          val returnValue = arrayStack.head
          if ( returnValue == minValue.head ) 
             minValue.remove(0)
          arrayStack.pop
          return returnValue
      }
      
      def min (): Int = {
          return minValue.head
      }
      
   }
  
  
  /*
   * Imagine a (literal) stack of plates. If the stack gets too high, it might topple. Therefore, in real life, we would likely start a new stack when the previous stack exceeds some threshold. Implement a data structure SetOfStacks that mimics this. 
   * SetOfStacks should be composed of several stacks, and should create a new stack once the previous one exceeds capacity. 
   * SetOfStacks.push() and SetOfStacks.pop() should behave identically to a single stack (that is, pop() should return the same values as it would if there were just a single stack).
      FOLLOW UP
      Implement a function popAt(int index) which performs a pop operation on a specific sub-stack.
   */
  class setOfStacks (threshold: Int) {
       
      val setOfStacks = collection.mutable.Stack[collection.mutable.Stack[String]]()
    
      def push( value: String ) = {
        /*println( "value - " + value + "\n"
              +  "setOfStacks - "  + setOfStacks.foreach ( x =>  x.foreach (println ) )
               )*/
               
        if ( setOfStacks.isEmpty ) {
            //println("Empty")
            val stack = collection.mutable.Stack[String]()
            stack.push(value)
            setOfStacks.push(stack)
        }else{
            //println("Not Empty - " + setOfStacks.apply(0))
            if ( setOfStacks.apply(0).size == threshold ){
               val stack = collection.mutable.Stack[String]()
               stack.push(value)
               setOfStacks.push(stack)
            }else{
               val stack = setOfStacks.pop()
               stack.push(value)
               setOfStacks.push( stack )
            }
        }
        
      }
      
      def pop(): String = {
         println( "setOfStacks - "  + setOfStacks.foreach ( println )
               )
         return setOfStacks.apply( 0 ).pop()
      }
      
      def popAt( index: Int ): String = {
        println( "index - " + index + "\n"
              +  "setOfStacks - "  + setOfStacks.foreach ( println )
               )
         if ( setOfStacks.apply( index ) == null )
            return "No stack exists with this index"
         else{
            if ( setOfStacks.apply( index ).isEmpty ){
                 // setOfStacks..pop()
                 setOfStacks.update(index, null)
                 return "No Element in the stack"
            }else{
                return setOfStacks.apply( index ).pop()
            }
         }
      }
  }
      
  //val setOfS = new setOfStacks(2)
  //setOfS.push("1") setOfS.push("2") setOfS.push("3") setOfS.push("4") setOfS.pop() setOfS.popAt(1)
  
  /*
   * In the classic problem of the Towers of Hanoi, you have 3 rods and N disks of different sizes which can slide onto any tower. 
   * The puzzle starts with disks sorted in ascending order of size from top to bottom (e.g., each disk sits on top of an even larger one). You have the following constraints:
(A) Only one disk can be moved at a time.
(B) A disk is slid off the top of one rod onto the next rod.
(C) A disk can only be placed on top of a larger disk.
Write a program to move the disks from the first rod to the last using Stacks
   */
   def moveDisks(noOfDisks: Int) = {
       
      val stackCounts = Array[Int](noOfDisks,0,0)
      var stack1 = collection.mutable.Stack[Int]()
      var stack2 = collection.mutable.Stack[Int]()
      var stack3 = collection.mutable.Stack[Int]()
      
      for( i <- 1 to noOfDisks ){
         stack1.push( i )
      }
      
      stack1.foreach( println )
      
      def checkDiscStack( stackA: collection.mutable.Stack[Int], stackB: collection.mutable.Stack[Int], stackC: collection.mutable.Stack[Int] )
          : ( collection.mutable.Stack[Int], collection.mutable.Stack[Int], collection.mutable.Stack[Int] ) = {
           if ( stackA.size == noOfDisks ) 
              return ( stackA, stackB, stackC )
          else if ( stackB.size == noOfDisks )
              return ( stackB, stackA, stackC )
          else if (stackC.size == noOfDisks ) 
              return ( stackC, stackA, stackB )
          else 
              return ( stackA, stackB, stackC )
      }
      
      val ( filledStack, emptyStack1, emptyStack2 ) = checkDiscStack( stack1, stack2, stack3 )
      //checkDiscStack( stack1, stack2, stack3 )
      /*println( "Returned Stacks after checkDisc" )
      stack1.foreach( println )
      println( "Empty1" ) 
      stack2.foreach( println )
      println( "Empty2" ) 
      stack3.foreach( println )*/
      def moveLast(source: collection.mutable.Stack[Int], destination: collection.mutable.Stack[Int]) = {
        if ( source.size > 0 )   
          destination.push( source.pop() )
      }
      
      def recursiveMove( stackA: collection.mutable.Stack[Int], stackB: collection.mutable.Stack[Int], stackC: collection.mutable.Stack[Int] )
      //, towerSource: collection.mutable.Stack[Int]
        //: ( collection.mutable.Stack[Int], collection.mutable.Stack[Int], collection.mutable.Stack[Int] )
         //: collection.mutable.Stack[Int] 
      = {
          stackC.push( stackA.pop() )
          stackB.push( stackA.pop() )
          stackB.push( stackC.pop() )
          
          //moveLast( towerSource, if ( stackB.isEmpty ) stackB else stackC )
          
           println( "call recursive ")
           println( "stackA" )
           stackA.foreach( println )
           println( "stackB" ) 
           stackB.foreach( println )
           println( "stackC" ) 
           stackC.foreach( println )
                      
          /*if ( (stackB.size != noOfDisks) && ( stackC.size != noOfDisks ) )
             recursiveMove( stackB, stackC, stackA, stackA )
          else 
            if ( stackB.size == noOfDisks )
               return stackB
            else return stackC*/
          
          //return ( stackC, stackA, stackB  )
      }
         
       var i = 1
       while ( emptyStack2.size != noOfDisks ) {
           if ( i == 1 ) {
               recursiveMove( filledStack, emptyStack1, emptyStack2  ) //filledStack
           }else if ( i == 2 ) {
               recursiveMove( emptyStack1, emptyStack2, filledStack  ) //filledStack
            }else if ( i == 3  ){
               recursiveMove( emptyStack2, filledStack, emptyStack1  ) //filledStack
               i = 0
            }
           if ( emptyStack2.size != noOfDisks ) {
               moveLast( (if ( !filledStack.isEmpty  ) filledStack else if ( !emptyStack2.isEmpty ) emptyStack2 else emptyStack1 ) 
                       , (if ( emptyStack2.isEmpty ) emptyStack2 else if ( emptyStack1.isEmpty ) emptyStack1 else filledStack ) 
                       )
           }
                   
           println( "after call recursive ")
           println( "filledStack" )
           filledStack.foreach( println )
           println( "emptyStack1" ) 
           emptyStack1.foreach( println )
           println( "emptyStack2" ) 
           emptyStack2.foreach( println )
           i = i.+(1)
       }
       println( "emptyStack2 " )
       emptyStack2.foreach { println }
       
       //val replcedStack = recursiveMove( filledStack, emptyStack1, emptyStack2, filledStack  )
       //replcedStack.foreach{ println }
      /*while ( (emptyStack1.size != noOfDisks) && ( emptyStack2.size != noOfDisks ) ){
           println( "call recursive ")
           filledStack.foreach( println )
           println( "Empty1" ) 
           emptyStack1.foreach( println )
           println( "Empty2" ) 
           emptyStack2.foreach( println )
           val ( stack1: collection.mutable.Stack[Int], stack2: collection.mutable.Stack[Int], stack3: collection.mutable.Stack[Int] ) = recursiveMove(stack1, stack2, stack3)
      }*/
      
      //filledStack.foreach( println )
      //emptyStack1.foreach( println )
      //emptyStack2.foreach( println )
  
   }
   //val movedD = moveDisks(3)
   
  /*
   * Implement a MyQueue class which implements a queue using two stacks.
   */
   class MyQueue() {
         val stack1 = collection.mutable.Stack[String]()
         val stack2 = collection.mutable.Stack[String]()
         
         def push( value: String ) = {
             stack1.push( value ) 
         }
         
         def pop(): String = {
             if ( !stack2.isEmpty ) 
               return stack2.pop()
             
             while ( !stack1.isEmpty ){
                stack2.push( stack1.pop() )
             }
             val returnVal = stack2.pop()
             /*while( !stack2.isEmpty  ){
                 stack1.push( stack2.pop )
             }*/
            return returnVal
         }
         
         def apply(): String = {
             if ( !stack2.isEmpty ) 
               return stack2.apply(0)
               
             while ( !stack1.isEmpty ){
                stack2.push( stack1.pop() )
             }
             val returnVal = stack2.apply(0)
            /* while( !stack2.isEmpty  ){
                 stack1.push( stack2.pop )
             }*/
             return returnVal
         }
         
   }
   //val myQueue = new MyQueue()
   //myQueue.push("1") myQueue.pop()
   
  /*
   * Write a program to sort a stack in ascending order. You should not make any assumptions about how the stack is implemented. 
   * The following are the only functions that should be used to write this program: push | pop | peek | isEmpty.
   */
   def sortStack ( origStack: collection.mutable.Stack[Int] ): collection.mutable.Stack[Int] = {
       val newStack = collection.mutable.Stack[Int]()
       import util.control.Breaks._
       
       while ( ! (origStack.isEmpty) ){
           val currValue = origStack.pop()
           var n = 0
           if ( newStack.isEmpty ){
                newStack.push( currValue)
           }else{
               breakable{
                while( !newStack.isEmpty ){
                  if ( currValue > newStack.apply(0) ){
                     origStack.push( newStack.pop() )
                     n = n.+(1)
                     //newStack.push( currValue)
                  }else{
                    //newStack.push( currValue)
                    util.control.Breaks.break()
                  }
                }
               }
                newStack.push( currValue)
                for ( i <- 1 to n ) {
                   newStack.push( origStack.pop() )
                }
           }
       }
       
       return newStack
   }
   //val origStack = collection.mutable.Stack(3,5,8,4,3,6)
   //val newStack = sortStack( origStack )
   
}