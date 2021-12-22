package com.cswg.practice

class PracticeTreesGraphs {
  
}

object PracticeTreesGraphs {
  
    val treeImple = collection.mutable.TreeSet[String]()
    treeImple.+=("1")
    //treeImple.
    
    //implement tree
   /* sealed abstract class BinaryTree[+A] {
        def isValid: Boolean
        def isEmpty: Boolean
        def add( item: A ): BinaryTree[A]
    }
    
    case object EmptyTree extends BinaryTree[Nothing] {
         def isValid = true
         def isEmpty = true
         //def add[A]( item: A ): BinaryTree[A] = NonEmptyTree(scala.Ordering.Implicits)( item, EmptyTree, EmptyTree )
    }
    
    case class NonEmptyTree[A] (
          val data: A
         ,val leftNode: BinaryTree[A]
         ,val rightNode: BinaryTree[A] 
         ) (implicit ord: Ordering[A]) extends BinaryTree[A] {
        def isEmpty = false
        def isValid: Boolean = {
            import ord._
            def isValidWith( f: A => Boolean, t: BinaryTree[A] ): Boolean = t match {
              case EmptyTree => true
              case NonEmptyTree( that, _, _) => f(that) && t.isValid
            }
            
            isValidWith( data > _, rightNode ) && isValidWith( data < _, leftNode )  
        }
      
    }*/
    
    //Implement Complete Tree
    class node(dataValue : String) {
       var data: String = dataValue
       var left: node = null;
       var right: node = null;
       var parent: node = null
       
       /*def node(dataValue: String) = {
           data = dataValue;
           
       }*/
       
       def setData(dataValue: String) = {
           data = dataValue
       }
       
       def setLeft( nodeValue: node ) = {
           left = nodeValue
       }
       
       def setRight( nodeValue: node ) = {
           right = nodeValue
       }
       
       def getLeft( ): node = {
           return left
       }
       
       def getRight( ):node = {
           return right
       }
       
       def getData(): String = {
           return data
       }
       
        def getOut(): Seq[node] = {
            val newNode = new node("4")
            return Seq(newNode)
         }
    }
    
    class tree {
          var root: node = null
          
          def isEmpty(): Boolean = {
              return root == null
          }
          
          def insert( data: String ): Unit = {
              root = insertData( root, data ) 
          }
          
          private def insertData( rootNode: node, dataValue: String ): node = {
              if ( rootNode == null ) {
                 val nodeValue = new node(dataValue)                 
                 return nodeValue
              }else{
                 if ( rootNode.getRight() == null ) {
                   rootNode.right = insertData( rootNode.right, dataValue)
                   rootNode.right.parent = rootNode
                 }else{ //if ( rootNode.getLeft() == null ){
                   rootNode.left = insertData( rootNode.left, dataValue)
                   rootNode.left.parent = rootNode
                 }/*else{
                   rootNode
                 }*/
                 
                 
                 /*
                  * Binary Seach Tree
                  */
                 /*if ( dataValue > rootNode.data ) 
                    rootNode.right = insertData( rootNode.right, dataValue  )
                 else if ( dataValue < rootNode.data ) 
                    rootNode.left = insertData( rootNode.left, dataValue  )
                 else return rootNode
                   */ 
                 return rootNode
              }
          }
          
          def count(rootValue: node): Int = {
              return countNodes(rootValue) 
          }
          
          private def countNodes( rootValue: node ): Int = {
              var count = 1
              if ( rootValue == null ) 
                return 0
              else{
                count = count + countNodes( rootValue.getLeft() )
                count = count + countNodes( rootValue.getRight() )
                
                return count
              }
                
          }
          
          def search( nodeValue: node, value: String ): Boolean = {
              if ( nodeValue.getData == value )
                return true
              else{
                 if ( nodeValue.getLeft() != null) {
                    if ( search( nodeValue.getLeft(), value ) )
                       return true
                 }
                 
                 if ( nodeValue.getRight() != null ) {
                    if ( search( nodeValue.getRight(), value ) )
                       return true
                 }
              }
              
              return false
          }
    
          //inOrder, PreOrder and PostOrder are depth-first search
          def inOrder(): Unit = {
             inOrder( root )
          }
          
          private def inOrder( nodeValue: node ): Unit = {
              if ( nodeValue != null ) {
                 inOrder( nodeValue.getLeft() )
                 println ( "Node Value - " + nodeValue.data ) 
                 inOrder( nodeValue.getRight() )
              }
          }
          
          def preOrder(): Unit = {
             preOrder( root )
          }
          
          private def preOrder( nodeValue: node ): Unit = {
              if ( nodeValue != null ) {
                println ( "Node Value - " + nodeValue.data )  
                preOrder( nodeValue.getLeft() )
                preOrder( nodeValue.getRight() )
              }
          }
          
          def postOrder(): Unit = {
             postOrder( root )
          }
          
          private def postOrder( nodeValue: node ): Unit = {
              if ( nodeValue != null ) {
                postOrder( nodeValue.getLeft() )
                postOrder( nodeValue.getRight() )
                println ( "Node Value - " + nodeValue.data )  
              }
          }
          
          //breadth-first Search
          def traverseSearch(): Unit = {
              traverseSearch(root)
          }
          
          private def traverseSearch( nodeValue: node): Unit = {
              if ( nodeValue == null ) 
                 return 
              
              var nodes = collection.mutable.Queue[node]()
              nodes.++=( Seq(nodeValue) )
              
              while( !nodes.isEmpty ) {
                  //val node = nodes.head;
                 val node = nodes.dequeue();
                 
                 println( "Node Value - " + node.data )
                 
                 if ( node.getLeft() != null ){
                   nodes.++=( Seq(node.left) )
                 }
                 
                 if ( node.getRight() != null ){
                   nodes.++=( Seq(node.right) )
                 }
                 
                 //nodes = nodes.drop(1)
              }
              
              
          }
    }
    val treeCustom = new tree( ) ;
    treeCustom.insert("1")
    treeCustom.insert("4")
    
    /*
     * Implement a function to check if a tree is balanced For the purposes of this question,
     *  a balanced tree is defined to be a tree such that no two leaf nodes differ in distance from the root by more than one
     */
    def balancedCheck( root: node): Boolean = {
       
         def maxDepth(nodeValue: node): Int= {
             if ( nodeValue == null ) 
                return 0
             else return 1 + math.max( maxDepth( nodeValue.left ), maxDepth( nodeValue.right ) )
      
         }
         
         def minDepth(nodeValue: node): Int= {
             if ( nodeValue == null ) 
                return 0
             else return 1 + math.min( minDepth( nodeValue.left ), minDepth( nodeValue.right ) )
      
         }
         
         if ( maxDepth( root ) - minDepth( root ) > 1)
            return false
         else return true
    }
    //val value = balancedCheck(treeCustom.root)
    
  /*
   * Given a directed graph, design an algorithm to find out whether there is a route be- tween two nodes
   */
    class Graph ( ) {
         def getOut(): Seq[node] = {  
            val newNode = new node("5")
            println("")
            
            return Seq(newNode)
         }
    }
    
  def findRoute( sourceNode: Graph, destNode: Graph ): Boolean = {
       
      //val listOuts: collection.mutable.LinkedList[node] = sourceNode.getOut()
      //var listOuts = collection.mutable.LinkedList[node]()
      //listOuts = listOuts.++( sourceNode.getOut() )
      /*for ( node <- sourceNode.getOut() ){
         listOuts = listOuts.:+( node )
      }*/
     var listOuts = collection.mutable.ArrayBuffer[node]()
     listOuts.++=( sourceNode.getOut() ) 
      
      while( !listOuts.isEmpty ){
         if ( listOuts.contains( destNode ) ) {
            return true
         }else{
           listOuts = listOuts.++( listOuts.apply(0).getOut() )
         }
         listOuts.drop(1)
      }
         
      /*if ( listOuts.contains( destNode ) ) 
         return true
      else{
        for ( i <- 0 to listOuts.size ) {
          findRoute( listOuts.apply(i), destNode ) 
        }
      }*/
      
      return false
  }
  
  //Way2
  def findRoute2( sourceNode: node, destNode: node ): Boolean = {
       
      def findRouteIn( sourceNode: node, destNode: node, list: collection.mutable.ArrayBuffer[node] ): Boolean = {
          list.++=( sourceNode.getOut() )
          while ( !list.isEmpty ){
              if ( list.contains( destNode ) )
                  return true
              else {
                val newSource = list.apply(0)
                list.drop(1)
                findRouteIn( newSource, destNode, list )
              }
          }
          
          return false
      }
      val list = collection.mutable.ArrayBuffer[node]()
      findRouteIn( sourceNode, destNode, list )
      return false
  }
  /*
   * Given a sorted (increasing order) array, write an algorithm to create a binary tree with minimal height
   */
   def createBinaryTree( array: Array[Int] ): node = {
       
       val newTree = new tree()
       def createTree( array: Array[Int], start:Int, end: Int ): node = {
           //var newNode: node = null
           if ( end < start )  
              return null
              
           val midd = ( start + end ) / 2
           
           /*if ( start == 0 && end == array.size -1 ) {
              newTree.insert( array.apply(midd).toString ) 
           }else{
              newNode = new node( array.apply(midd).toString )
           }*/
           
           val newNode = new node( array.apply(midd).toString )
           newNode.left = createTree( array, start, midd - 1)
           newNode.right = createTree( array, midd + 1 , end )
           
           return newNode
       }
       
       return createTree( array, 0 , array.size - 1 ) 
       
       //return newTree
   }
   //val array = Array[Int]( 1,2,3,4,5,6,7,8,9 )
   //val newTree = createBinaryTree( array )
   //val tree = new tree()
   //tree.root = newTree
   
   /*def createBinaryTree( array: Array[String] ) = {
       import util.control.Breaks._
       
       def checkLeftRight( nodeValue: node ): node = {
            if ( nodeValue.right == null )
                return nodeValue.right
            else if ( nodeValue.right == null )
                return nodeValue.left
            else nodeValue
        }
       
        val level = 2
        breakable{
          for ( i <- 1 to level ) {
              for ( j <- 1 to 2 ){
                if ( j == 1 )
                  node = node.right
                else
                   node = node.left
                val newNode = checkLeftRight( node ) 
                if ( newNode == null ) {
                   break()
                }
                
              }
              node = node.right
          }
        }
        insert( newNode, dataValue )
        
        def insert( nodeValue: node, dataValue: String): node = {
            if ( nodeValue == null )
               return new node( dataValue)
            else{
               var returnNode: node = null
               if ( nodeValue.right == null ){
                  nodeValue.right = insert( nodeValue.right, dataValue )
                  returnNode = nodeValue 
               }else if ( nodeValue.left == null ){
                  nodeValue.left = insert( nodeValue.left, dataValue )
                  returnNode = nodeValue
               }else{
                   if ( siblingMap.isEmpty ) {
                       //nodeValue.right.right = insert( nodeValue.right.right, dataValue )
                       if ( nodeValue.right.right == null ){
                          nodeValue.right.right = insert( nodeValue.right.right, dataValue )
                          returnNode = nodeValue.right 
                      }else if ( nodeValue.right.left == null ){
                          nodeValue.right.left = insert( nodeValue.right.left, dataValue )
                          returnNode = nodeValue.right
                      }else{
                          siblingMap.put( nodeValue.right, nodeValue.left )
                          returnNode = nodeValue.right
                      }
                   }else{
                      val sibling = siblingMap.getOrElse(nodeValue, defaultNode)
                      if ( sibling.right == null ){
                          sibling.right = insert( sibling.right, dataValue )
                          returnNode = nodeValue
                      }else if ( sibling.left == null ){
                          sibling.left = insert( sibling.left, dataValue )
                          returnNode = nodeValue
                      }else{
                          siblingMap.clear()
                          returnNode = nodeValue.right
                      }
                   }
               }
                 
               return returnNode
            }
              
       } 
       var siblingMap = collection.mutable.HashMap[node, node]()
       var defaultNode: node = null
       for( i <- 0 to array.size - 1 ){
          defaultNode = insert( defaultNode, array.apply(i) )
       }
   }  */
  
  
  /*
   * Given a binary search tree, design an algorithm which creates a linked list of all the nodes at each depth (i e , if you have a tree with depth D, you’ll have D linked lists)
   */
   def createLinkedList(nodeValue: node): collection.mutable.ArrayBuffer[collection.mutable.LinkedList[node]]
      = {
        import collection.mutable._
        import util.control.Breaks._
        val arrayLinked = collection.mutable.ArrayBuffer[collection.mutable.LinkedList[node]]()
        var arrayLinkedLoop = collection.mutable.ArrayBuffer[collection.mutable.LinkedList[node]]()
        
        def createdLinkedArray(nodeValue: node): collection.mutable.LinkedList[node] = {
            var linkedList = collection.mutable.LinkedList[node]()
            if ( nodeValue != null ) {
               if ( nodeValue.left  != null ) 
                 linkedList = linkedList.:+( nodeValue.left )
              
               if ( nodeValue.right  != null ) 
                 linkedList = linkedList.:+( nodeValue.right )
            }
             
            return linkedList
        }
        
        arrayLinked.++=( Seq( LinkedList(nodeValue) ) )
        arrayLinkedLoop.++=( Seq( LinkedList(nodeValue) ) ) 
        arrayLinked.foreach( println )
        //for ( i <- 0 to arrayLinked.size -1 ){
        var i = 0
        //breakable{
          while ( !arrayLinkedLoop.isEmpty ){
              //arrayLinkedLoop.head
             //println( "array element - " + i ) 
             var linkedListnextLevel = collection.mutable.LinkedList[node]()
             for( j <- 0 to  arrayLinked.apply(i).size - 1 ) {
                val childValues = createdLinkedArray( arrayLinked.apply(i).apply(j) )
                //childValues.foreach( x => println( "Returned Child - " + x.data ) )
                childValues.foreach { x => linkedListnextLevel = linkedListnextLevel.:+(x) }
             }
             linkedListnextLevel.foreach { x => println( "Child - " + x.data ) }
             if ( !linkedListnextLevel.isEmpty ) 
               arrayLinked.++= ( Seq(linkedListnextLevel)  )
               arrayLinkedLoop.++=( Seq( LinkedList(nodeValue) ) ) 
             /*else
                 break()*/
             arrayLinkedLoop = arrayLinkedLoop.drop(i) 
             //= arrayLinkedLoop.remove(i)
             //arrayLinkedLoop.foreach{ x => x.foreach{ y => println( "Loop values - " + y.data ) } }
             //println( arrayLinkedLoop.isEmpty )
             i = i.+(1)
          }
        //}
        
        return arrayLinked
   }
   //val arrayLinked = createLinkedList(tree.root) 
   
   //Way2
   def createLinkedListWith(nodeValue: node): collection.mutable.ArrayBuffer[collection.mutable.LinkedList[node]]
      = {
        import collection.mutable._
        import util.control.Breaks._
        val arrayLinked = collection.mutable.ArrayBuffer[collection.mutable.LinkedList[node]]()
        
        def createdLinkedArray(nodeValue: node): collection.mutable.LinkedList[node] = {
            var linkedList = collection.mutable.LinkedList[node]()
            if ( nodeValue != null ) {
               if ( nodeValue.left  != null ) 
                 linkedList = linkedList.:+( nodeValue.left )
              
               if ( nodeValue.right  != null ) 
                 linkedList = linkedList.:+( nodeValue.right )
            }
             
            return linkedList
        }
        
        arrayLinked.++=( Seq( LinkedList(nodeValue) ) ) 
        arrayLinked.foreach( println )
        var i = 0
        breakable{
          while ( !arrayLinked.isEmpty ){
              //arrayLinkedLoop.head
             println( "array element - " + i ) 
             var linkedListnextLevel = collection.mutable.LinkedList[node]()
             for( j <- 0 to  arrayLinked.apply(i).size - 1 ) {
               if ( arrayLinked.apply(i).apply(j) != null ) {
                 if ( arrayLinked.apply(i).apply(j).left  != null ) 
                   linkedListnextLevel = linkedListnextLevel.:+( arrayLinked.apply(i).apply(j).left )
                
                 if ( arrayLinked.apply(i).apply(j).right  != null ) 
                   linkedListnextLevel = linkedListnextLevel.:+( arrayLinked.apply(i).apply(j).right )
              }
             }
             linkedListnextLevel.foreach { x => println( "Child - " + x.data ) }
             if ( !linkedListnextLevel.isEmpty ) 
               arrayLinked.++= ( Seq(linkedListnextLevel)  )
             else
                 break()
             //= arrayLinkedLoop.remove(i)
             //arrayLinkedLoop.foreach{ x => x.foreach{ y => println( "Loop values - " + y.data ) } }
             //println( arrayLinkedLoop.isEmpty )
             i = i.+(1)
          }
       }
        
        return arrayLinked
   }
  //val arrayLinked = createLinkedListWith(tree.root) 
   
  /*
   * Write an algorithm to find the ‘next’ node (i e , in-order successor) of a given node in a binary search tree where each node has a link to its parent
   */
  def findNextNode( nodeValue: node ) = {
       import util.control.Breaks._
      def leftMostChild( nodeVal: node ): node  = {
          if ( nodeVal != null ) {
             if ( nodeVal.left != null )
               return leftMostChild( nodeVal.left )
              else
                return nodeVal
          }else
            return null
      }
      
      def findNext( nodeVal: node, sameYN: String ): node = {
          if ( nodeVal != null ) {
            if ( nodeVal.right != null || nodeVal.parent == null ) { //
               return leftMostChild( nodeVal.right )
            }else {
              /*if ( nodeVal.parent == null )
                return nodeVal
              else if ( nodeVal == nodeVal.parent.left ) 
               return nodeVal.parent
              else if ( nodeVal == nodeVal.parent.right ) 
                return findNext( nodeVal.parent, "N" )
              else
                return null*/
              var parent: node = null //nodeVal.parent
              var nodeValue = nodeVal
              breakable{
                 while ( ( parent = nodeValue.parent ) != null ) {
                     if ( parent.left == nodeValue )
                        break
                        
                     /*if ( parent.parent != null ) 
                        parent = parent.parent
                     else break
                     */
                     nodeValue = parent   
                 }
              }
              
              return parent
            }
            
          }
          else
            return null
      }
      
      findNext( nodeValue, "Y" )
      
  }
  /*
   * Design an algorithm and write code to find the first common ancestor of two nodes in a binary tree Avoid storing additional nodes in a data structure NOTE: This is not necessarily a binary search tree
   */
   def commonParent( node1: node, node2: node ) = {
       
       while ( node1.data != null) {
         
       }
   }
  /*
   * You have two very large binary trees: T1, with millions of nodes, and T2, with hun- dreds of nodes Create an algorithm to decide if T2 is a subtree of T1
   */
  
  /*
   * You are given a binary tree in which each node contains a value Design an algorithm to print all paths which sum up to that value Note that it can be any path in the tree - it does not have to start at the root
   */
  
}