package com.cswg.practice

class PracticeGeeksforGeeks {
  
}

object PracticeGeeksforGeeks {
   
  //Array
  val values = Array[Int](8,7,3,5,8,45,65,71,3) 
  val stringValues = Array[String]("apple","mango","banana")
  //Add Element
  values.:+(11)
  //Prepend Element
  values.+:(29)
  //Get value from specific position
  values.apply(3)
  //Remove element at specific position
  values.zipWithIndex.filter( _._2 != 4 ).map( _._1 )
  //Drop number of elements 
  values.drop(1)
  //Update Value
  values.update( 3, 99)
  //Other functions
  values.count { x => x == 5 }
  values.head
  values.tail
  //Sorting functions
  values.sorted
  values.sortBy{ x => x.toInt }
  values.sortWith( _ > _)
  scala.util.Sorting.quickSort(stringValues)
  
  //ArrayBuffer
  val valuesBuffer: collection.mutable.ArrayBuffer[Int] = collection.mutable.ArrayBuffer[Int](8,7,3,5,6,45,65,71,3)
  //Add elements
  valuesBuffer.+=(34)
  //Prepend elements
  valuesBuffer.+=:(29)
  //Get value from specific position
  valuesBuffer.apply(3)
  //Remove Element
  valuesBuffer.remove(4)
  valuesBuffer.remove(3, 2)
  //Sorting functions
  valuesBuffer.sorted
  
  valuesBuffer.-=(3)
  valuesBuffer.contains(elem)
  
  //List
  val valueList = List[Int](8,7,3,5,8,45,65,71,3)
  //Add Element - Prepended
  valueList.+:(14)
  //Add Element in the same List
  valueList.::(15)
  valueList.:+(17)
  //Remove element at specific position
  valueList.zipWithIndex.filter( _._2 != 6).map(_._1)
  //Get value from specific position
  valueList.apply(3)
  //Update Value
  //Not allowed
  //Drop number of elements 
  valueList.drop(1)
  //ReversesList
  valueList.reverse
  //Other functions
  valueList.count { x => x == 5 }
  valueList.head
  valueList.tail
  //Sorting functions
  valueList.sorted
  valueList.sortBy{ x => x.toInt }
  valueList.sortWith( _ > _)
  
  //ListBuffer
  val valuesListBuffer: collection.mutable.ListBuffer[Int] = collection.mutable.ListBuffer[Int](8,7,3,5,6,45,65,71,3)
  //Add elements
  valuesListBuffer.+=(13)
  //Prepend elements
  valuesListBuffer.+=:(29)
  //Get value from specific position
  valuesListBuffer.apply(3)
  //Remove Element
  valuesListBuffer.remove(4)
  valuesListBuffer.remove(3, 2)
  //Drop number of elements 
  valuesListBuffer.drop(1)
  //Update Value
  valuesListBuffer.update(2, 34)
  //Sorting functions
  valuesListBuffer.sorted
  valuesListBuffer.sortBy{ x => x.toInt }
  valuesListBuffer.sortWith( _ > _)
  
  //Map
  //If you want Map to remember insertion order use LinkedHashMap or ListMap
  val linkedMap = scala.collection.mutable.LinkedHashMap[String, String]("al" -> "albert", "go" -> "goniana", "ba" -> "banana" )
  //If you want to return elements in keys sorting order use Sorted Map
  val sortedMap = scala.collection.SortedMap[String, String]("al" -> "albert", "go" -> "goniana", "ba" -> "banana" )
  val hashSet = scala.collection.mutable.HashSet[Int]( 1,2,3,4,5,2,3)
  val sortedSet = scala.collection.mutable.SortedSet[Int]( 1,2,3,4,5,2,3)
  linkedMap.values.exists { _ == "albert" }
  var JValueSearchParams: scala.collection.mutable.Map[ String, String ] = scala.collection.mutable.HashMap[ String, String ]()
  
  //Add Element
  linkedMap.+=("po" -> "Pomegranite")
  //Get value from specific position
  linkedMap.get("po")
  //Remove element at specific position
  linkedMap.-=("an")
  linkedMap.remove("ac")
  //Update Value
  linkedMap.update("an", "ana")
  //sorting
  scala.collection.mutable.ListMap( linkedMap.toSeq.sortBy( _._1.toLowerCase() ): _* )
  
  //Stack
  val stack = collection.mutable.Stack(4,5,73,12,54,86,45)
  //Add Element
  stack.push(1)
  //Prepend Element
  stack.+:(29)
  //Get value from specific position
  stack.apply(3)
  stack.head
  val elem = stack.elems
  //Remove element at specific position
  stack.pop()
  stack.zipWithIndex.filter( _._2 != 4 ).map( _._1 )
  //Drop number of elements 
  stack.drop(1)
  //Update Value
  stack.update( 3, 99)
  //Sorting
  stack.sorted
  
  //Queue 
  val queue = collection.mutable.Queue[Int](1,2,3,4,5)
  //Add Element
  queue.+=(12)
  //Prepend Element
  queue.+=:(23)
  //Get value from specific position
  queue.apply(3)
  queue.head
  queue.front
  queue.tail
  queue.last
  queue.distinct
  //Remove element at specific position
  queue.dequeueFirst { x => x==343 }
  queue.dequeue()
  queue.dequeueAll { x => x == 343 }
  //Drop number of elements 
  queue.drop(1)
  //Update Value
  queue.update( 3, 99)
  
  //LinkedList
  val linkedList = collection.mutable.LinkedList[Int](1,2,3,4,5,9,35,22,23)
  //Add Element
  linkedList.:+(12)
  //Prepend Element
  linkedList.+:(19)
  //Get value from specific position
  linkedList.apply(3)
  linkedList.head
  linkedList.tail
  //Remove element at specific position
  linkedList.filter { x => x.toInt == 2}
  linkedList.zipWithIndex.filter( _._2 == 2 ).map( _._1 )
  //Drop number of elements 
  linkedList.drop(1)
  //Update Value
  linkedList.update( 3, 99)
  //Sorting
  linkedList.sorted
  
  //partition GroupBY span
  val listed = List( 15, 10, 5, 8, 20, 12)
  listed.partition { x => x > 10 }
  listed.groupBy { x => x > 10 }
  listed.span { x => x > 10 }
  listed.splitAt {2 }

  //enumeration
  object cardType extends Enumeration{
      type cardTypeValue = Value
      val SPADE, CLUB, DIAMOND, HEART = Value
  }
  
  cardType.apply(1)
  
  // Strings
  val s = "Hello World"
  s.replace(" ", ", ")
  
  val source = "123 Main Street, Keene, NH, US, 03431"
  val reg = "[0-9]+".r
  reg.findFirstIn(source)
  reg.findAllIn(source).foreach{ println }
  reg.findAllMatchIn(source).foreach{ println }
  reg.findFirstMatchIn(source)
  reg.findPrefixOf(source)
  
  scala.collection.immutable.StringOps
  
  /// K largest elements from a big file or array.
  def kLargestElements( array: Array[Int], noOfElements: Int ) ={
    
     import util.control.Breaks._
     var newArray = collection.mutable.ArrayBuffer[Int]()
     
     def sortkLargestElements( array: Array[Int] ) = {
         import collection.mutable._
         var arrayIndex = 0;
         var valueChanged = "N"
         var moved = "N"
         var givenArray = array
         //this works
         //val valuesBuffer = collection.mutable.ArrayBuffer[Int]( array: _*)
         //This does not work
         //var givenArray:collection.mutable.ArrayBuffer[Int] = array.asInstanceOf[collection.mutable.ArrayBuffer[Int]]
         //givenArray.foreach {  x => println (x) }
         //for ( i <- 0 to givenArray.size -1 ) {
         while( !givenArray.isEmpty ) {
            val temp = givenArray.apply(0)
            valueChanged = "N"
            println( "temp - " + temp + " newArray.size - " + newArray.size )
            givenArray = givenArray.drop(1)
            if ( newArray.isEmpty ){
                newArray.+=( temp )                
            }else{
              breakable{
                //for ( j <- 0 to newArray.size - 1 ){
                while( !newArray.isEmpty ){
                 if ( temp >= newArray.apply( newArray.size - 1) ){
                    println( "Greater value found - " + temp   //+ " :  " + valueChanged
                           + " :newArray.size " + newArray.size
                           + " : givenArray.size  " +  givenArray.size 
                          )
                            
                    if ( valueChanged.equalsIgnoreCase( "N" ) )
                      arrayIndex = givenArray.size
                    
                    valueChanged = "Y"
                    givenArray = givenArray.:+( newArray.apply(newArray.size - 1) ) 
                    newArray.remove( newArray.size - 1 )                 
                    moved = "Y"              
                    //newArray.update( newArray.size - 1, temp )
                 }else{
                   break
                 }
                }
              }
              
              newArray.+=( temp ) 
              println( "arrayIndex - " + arrayIndex  + " :givenArray.size - " + givenArray.size )  //+ " - moved " + moved
              if ( moved == "Y" ){
                for ( f <- givenArray.size - 1 to arrayIndex by -1 ) {
                  newArray.+=( givenArray( f ) )
                  givenArray = givenArray.zipWithIndex.filter( _._2 != f ).map( _._1 )
                  givenArray.foreach { x => println ( x ) }
                }
              }
              
           }
            
         }
     }
     
     for ( i <- 0 to array.size -1 ) {
       println( "array - " + array.apply(i) )
     }
     sortkLargestElements( array );
     println( "created array ")
     for ( i <- 0 to noOfElements - 1 ) {
        println( newArray(i) )
     }
     
  }
  //kLargestElements( values, 3)
  
  def kLargestElement( array: Array[Int], noOfElements: Int ) ={
    
     var newArray = collection.mutable.ArrayBuffer[Int]()
     
     for ( i <- 0 to noOfElements - 1 ) {
         
        println(  array.sortWith( _ > _ ).apply(i) )
     }

  }
  //val values = Array[Int](8,7,3,5,8,45,65,71,3)  
  // kLargestElement( values, 3)
  
  def findLargest( array: Array[Int] ): Int = {
    
     
     import scala.annotation.tailrec
     import collection.mutable._
     
     val newArray = collection.mutable.ArrayBuffer[Int]()
     
     @tailrec
     def findLargestElem( array: Array[Int], maxNumber: Int): Int = {
         if ( array.isEmpty ) 
            return maxNumber
         else
            if ( array.head > maxNumber )
               findLargestElem( array.tail, array.head )
            else
               findLargestElem( array.tail, maxNumber )            
     }
     
     return findLargestElem( array, 0 );
     
  }
  //val large = findLargest(values)
  
  //findTriplets where a = b + c
  def triplets( array: Array[Int] ) ={
    
     import util.control.Breaks._
     var newArray = collection.mutable.ArrayBuffer[Int]()
     
     def sortkLargestElements( array: Array[Int] ) = {
         import collection.mutable._
         var arrayIndex = 0;
         var valueChanged = "N"
         var moved = "N"
         var givenArray = array
         //this works
         //val valuesBuffer = collection.mutable.ArrayBuffer[Int]( array: _*)
         while( !givenArray.isEmpty ) {
            val temp = givenArray.apply(0)
            valueChanged = "N"
            //println( "temp - " + temp + " newArray.size - " + newArray.size )
            givenArray = givenArray.drop(1)
            if ( newArray.isEmpty ){
                newArray.+=( temp )                
            }else{
              breakable{
                while( !newArray.isEmpty ){
                 if ( temp >= newArray.apply( newArray.size - 1) ){
                    /*println( "Greater value found - " + temp   //+ " :  " + valueChanged
                           + " :newArray.size " + newArray.size
                           + " : givenArray.size  " +  givenArray.size 
                          )
                            */
                    if ( valueChanged.equalsIgnoreCase( "N" ) )
                      arrayIndex = givenArray.size
                    
                    valueChanged = "Y"
                    givenArray = givenArray.:+( newArray.apply(newArray.size - 1) ) 
                    newArray.remove( newArray.size - 1 )                 
                    moved = "Y"              
                 }else{
                   break
                 }
                }
              }
              
              newArray.+=( temp ) 
              //println( "arrayIndex - " + arrayIndex  + " :givenArray.size - " + givenArray.size )  //+ " - moved " + moved
              if ( moved == "Y" ){
                for ( f <- givenArray.size - 1 to arrayIndex by -1 ) {
                  newArray.+=( givenArray( f ) )
                  givenArray = givenArray.zipWithIndex.filter( _._2 != f ).map( _._1 )
                  givenArray.foreach { x => println ( x ) }
                }
              }
              
           }
            
         }
     }
     println("Passed Array")
     array.foreach { x => println( x ) }

     sortkLargestElements( array );
     
     println( "created array ")
     newArray.foreach { x => println( x ) }

     
     val triplets = collection.mutable.LinkedHashMap[Int, (Int, Int, Int)]()
     var count = 0
     
     def findTriplets() = { //:Boolean
         for ( actElem <- 0 to newArray.size - 3  ){
            var firstElement = actElem + 1
            var secondElement = newArray.size - 1            
            println( "Check for - " + newArray.apply(actElem) )
             while ( secondElement > firstElement ) {
                println( "Matched with  - " + newArray.apply(firstElement) + " & " + newArray.apply(secondElement) )
                if ( newArray.apply(actElem) == newArray.apply(firstElement) + newArray.apply(secondElement) ) {
                  println( "Matched - " + count )
                  count = count.+(1)
                  triplets.put( count, (newArray.apply(actElem), newArray.apply(firstElement), newArray.apply(secondElement) ) )
                  secondElement = secondElement.-(1)
                  //firstElement = firstElement.+(1)
                  //return true
                }else{
                   if ( newArray.apply(actElem) > newArray.apply(firstElement) + newArray.apply(secondElement) ) {
                       secondElement = secondElement.-(1)
                   }else{
                       firstElement = firstElement.+(1)
                   }
                }
             }
         }
         
         //return false
     }
     
     val exists = findTriplets()
     println( "exists - " + exists ) 
     triplets.foreach(f => println( "Triplet No " + f._1 + " - "  + f._2._1 + " = " + f._2._2 + " + " + f._2._3 ) )
  } 
  //val values = Array[Int](5,8,10,12,9,6,14,1,4)  
  //triplets(values)
  
  //pivot index is index where sum of left side and right side is equal
  def findPivotIndex( array: Array[Int] ): Int = {
      import scala.annotation.tailrec
      
       @tailrec
       def checkSum( index: Integer ): Int = { //leftArray: Array[Int], rightArray: Array[Int]
           var sumLeft = 0
           var sumRight = 0
           
           if ( index == array.size ) {
              return -1
           } 
           for ( i <- 0 to index - 1 ) {
                sumLeft = sumLeft + array.apply(i)
           }
           
           for ( i <- index + 1 to array.size - 1 ) {
                sumRight = sumRight + array.apply(i)
           }
           
           if ( sumLeft == sumRight ) {
              return index
           }else{
             checkSum( index + 1 )           
           }
           
       }
       
     if ( array.isEmpty ) {
        return -1
     }else{
       checkSum( 0 )
     }
      
  }
  val array = Array[Int](1, 7, 3, 6, 5, 6) 
  val array1 = Array[Int](-1,-1,-1,0,1,1) 
  val array2 = Array[Int](-4,1,1,1,1,-1) 
  val pivotIndex = findPivotIndex( array )
  
 //find largest number atleast twice the size of all other numbers
  def findLargestTwiceSize( nums: Array[Int] ): Int = {
      import scala.annotation.tailrec
      
      @tailrec
      def maxTwice( nums: Array[Int], largest: Int, largestIndex: Int, runningIndex: Int ): Int = { // , twice: String

          if ( nums.isEmpty ){
            //if ( twice == "Y" )
              return largestIndex
            //else return -1
          }
          
          if ( largest >= nums.apply(0) ) {
             maxTwice( nums.tail, largest, largestIndex , runningIndex + 1)
             /*if ( largest >= 2 * nums.apply(0) )
               maxTwice( nums.tail, largest, largestIndex , runningIndex + 1)  // ,  if ( twice != "N" ) "Y" else "N" 
             else
                maxTwice( nums.tail, largest, largestIndex, runningIndex + 1  )  //, "N"*/
          }else{
             maxTwice( nums.tail, nums.apply(0), runningIndex + 1, runningIndex + 1  ) 
            /*if ( nums.apply(0) >= largest * 2 )
               maxTwice( nums.tail, nums.apply(0), runningIndex + 1, runningIndex + 1  )  //, "Y"
           else 
               maxTwice( nums.tail, nums.apply(0), runningIndex + 1, runningIndex + 1 )  //, "N"*/
          }
      }
       
      val largestIndex = maxTwice( nums.tail, nums.apply(0), 0, 0  )
      println ( " largestIndex - " + largestIndex)
      
      for ( number <- nums.zipWithIndex.filter { x => x._2 != largestIndex }.map(_._1 ) ){
         if ( nums(largestIndex ) < number * 2 ) 
           return -1
      }
      
      return largestIndex
  }
  val nums = Array[Int](1, 2, 3, 4)
  val nums1 = Array[Int](3, 6, 1, 0)
  val nums2 = Array[Int](0,0,2,1)
  val nums3 = Array[Int](0,1,0,2)
  val nums4 = Array[Int](0,1,1,2)
  findLargestTwiceSize( nums )
  
  
  /*
   * Given a non-empty array of digits representing a non-negative integer, plus one to the integer.
The digits are stored such that the most significant digit is at the head of the list, and each element in the array contain a single digit.
   */
  def plusOne(digits: Array[Int]): Array[Int] = {
      import scala.annotation.tailrec  
    
      val newArray = scala.collection.mutable.ArrayBuffer[Int]()
      
      @tailrec
      def addOne( index: Int, carry: Int ): Unit = {
          
           if ( index < 0 ) 
             return
             
           if ( ( digits(index) + carry ) >= 10 ) {
               if ( index != 0 ){
                  newArray.+=( ( digits(index) + carry ) % 10 )                  
               }else{
                  newArray.+=( ( digits(index) + carry ) % 10 )
                  newArray.+=( 1 )
               }
               addOne( index - 1, 1 )
           }else{
              newArray.+=( digits(index) + carry )
              addOne( index - 1, 0 )
           }
      }
      
      addOne( digits.length - 1, 1 )
      //newArray.reverse.foreach ( println ( _ )  )
      
      return Array[Int]( newArray.reverse: _* )
  }
  
  //Way 2
  def plusOne2(digits: Array[Int]): Array[Int] = {
      import scala.annotation.tailrec  
          
      for ( i <- digits.size - 1 to 0 by -1 ){
          if ( digits(i) < 9 ){
              digits.update(i, digits.apply(i)  + 1 )
              return digits
          }
          
          digits.update(i, 0)
             
      }
      
      var newArray = Array[Int](1)
      for ( number <- digits ){
         newArray = newArray.:+( number )
      }
      
      return newArray
  }
  val digits = Array[Int]( 4,3,2,1)
  val digits2 = Array[Int](5,8,9,9 )
  val digits3 = Array[Int](9,9,9,9)
  val digits4 = Array[Int](9,8,2,9)
  plusOne( digits )
  plusOne2( digits )
}