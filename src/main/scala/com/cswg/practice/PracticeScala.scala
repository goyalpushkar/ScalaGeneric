package com.cswg.practice

class PracticeScala {
  
}

object PracticeScala {
  
   def main(args: Array[String]){
      
   }

   scala.math.Ordering
   val values = "PushkarGoyal!@^&";
   values.sorted   //Sort first capital letters and then small letters
   values.sortBy { x => x.toLower }  
   values.sortWith( _ > _ )
   
   
   val array = Array[Int](1,2,5,3,6,2,72,4)
   array.sortBy { x => x }
   array.sortWith( _ <  _)
   array.sorted 
   
   val words = "The quick brown fox jumped over the lazy dog".split(' ')
    // this works because scala.Ordering will implicitly provide an Ordering[Tuple2[Int, Char]]
   words.sortBy(x => (x.length, x.head) )
   words.sortBy(x => ( x.length) )
   words.sortBy(x => x.head )
   words.sortBy(x => x.head.toLower )
    
  //Implement an algorithm to determine if a string has all unique characters. What if you can not use additional data structures?
  def uniqueChars(values: String): String = {
    import util.control.Breaks._
     var check = true
     var index = 0
     breakable{
     values.foreach { x => 
                           /*println( "index - " + index + " :x - " + x + " :Index - " + values.indexOf(x, index) 
                               + " Sub String - " + values.substring( values.indexOf(x, index) + 1, values.size )
                                 )*/
                            println(" x- " + x + " :index - " + index )
                           if ( !( values.substring( values.indexOf(x, index) + 1, values.size ).isEmpty() ) ) {
                              if ( values.substring( values.indexOf(x, index) + 1, values.size ).contains(x) ) 
                                println( "Inside If" )
                                 check = false 
                                //break()
                           }else 
                           { if ( check != false ) 
                              check = true 
                           } 
                           index = index + 1
                    }
     }
     if ( check == false ) 
        return "Not Unique" 
     else return "Unique"
  };
  ////val uniq = uniqueChars("PushkarGoyal");
  
  def unique(values: String) = {
      if ( values.toLowerCase.distinct.size != values.size ) {
           println( "NotUnique")
       }else{
           println( "Unique" )
       }
  }
  //val uniq = unique("PushkarGoyal");
  
  //Write code to reverse a C-Style String (C-String means that Ã¢â‚¬Å“abcdÃ¢â‚¬ï¿½ is represented as five characters, including the null character )
  def reverseCStyleString( value: String ): String = {
      //Way 1
      //return value.reverse

      //Way 2
      /*val newString: StringBuffer = new StringBuffer()
      for ( i <- value.size - 2 to 0 by -1 ){
         println( "i - " + i + " - " + value.apply( i ) )
         //if ( value.apply(value.size - i)  != null || value.apply(value.size - i)  != " " || value.apply(value.size - i)  != "" ) 
         newString.append( value.apply(i) )
        //else println("null value")
      }
      //value.foreach{ x => if ( x != null ) newString.(x) }
      return newString.toString()
      */
    
      //Way 3
      /*var values = value.toArray
      for ( i <- 0 to ( values.size - 2 ) / 2 ){
          val tmp = values.apply(i)
          //println( " i - " + i + " apply(i) = " + values.apply( i ) + " values.size - 2 - i - " + ( values.size - 2 - i ) + " apply(size -2 -i) = " + values.apply( values.size - 2 - i ) )
          values.update(i, values.apply( values.size - 2 - i ) )
          values.update( (values.size - 2 - i) , tmp )
          //values.foreach(println)
      }*/
    
      var start = 0
      var end = value.size - 1
      var values: String = value
      while ( end >= start ){
          val temp = value.apply(end)
          values = values.updated(end, value(start) )
          values = values.updated(start, temp )
          start = start.+(1)
          end = end.-(1)
      }
      return values  //.mkString
  };
  //val reverse = reverseCStyleString("Pushkar Goyal ");
	
  /*void reverse(char *str) {
      char * end = str;
      char tmp;
       if (str) {
           while (*end) {
           ++end;
           }
           --end;
          while (str < end) {
           tmp = *str;
           *str++ = *end;
           *end-- = tmp;
           }
       }
   }*/
  
  val word = "Pushkar Goyal";

  //Design an algorithm and write code to remove the duplicate characters in a string without using any additional buffer NOTE: One or two additional variables are fine An extra copy of the array is no
  def removeDuplicates( value: String ): String = {
      var values = value
      for ( i <- values.size - 1 to 0 by -1 ){
         //println( "i - " + i + " :Substring - " + values.substring( 0, i ) + " :Value Check - " + values.apply(i))
         if ( values.substring( 0, i ).toLowerCase().contains( values.apply(i).toLower ) ) 
            values = values.substring(0, i) + values.substring( i + 1, values.size)
         //else values = values
         //println( "New Value - " + values )
      }
      
      return values
  }
  //val removedDup = removeDuplicates("Pushkar Goyal s");
  
  //Write a method to decide if two strings are anagrams or not.
  def anagramTest( string1: String, string2: String ): String = {
      var check = "Yes"  
      
      if ( string1 != null && string2 != null ) {
        if ( string1.size == string2.size ) {
            check = "Yes"
        }else{
            return "No"
        }
      }else if ( string1 == null && string2 == null ) {
         return "Yes"
      }else{
        return "No"
      }
      val charCount: collection.mutable.Map[String, Int] = collection.mutable.Map()
      
      def checkvalueExists( value: String, mapValue: collection.mutable.Map[String, Int] ): collection.mutable.Map[String, Int] = {
          if ( mapValue.contains( value ) ) {
             mapValue.put(value, mapValue.getOrElse(value, 0) + 1 )
          }else {
             mapValue.put( value, 1 )
          }
          
          return mapValue
      }
      
      def removeCount( value: String, mapValue: collection.mutable.Map[String, Int] ): collection.mutable.Map[String, Int] = {
          
        if ( mapValue.contains( value ) ){
          mapValue.put( value, mapValue.getOrElse( value, 0 ) - 1 )
        }else{
           mapValue.put( value, 1 )
        }
        
        return mapValue
      }
      
      string1.foreach{ x => checkvalueExists( x.toString(), charCount )  }
      charCount.foreach{ f => println( "key - " + f._1 + " :Value - " + f._2 ) }
      
      string2.foreach{ x => removeCount( x.toString(), charCount ) }
      println( "Final " )
      charCount.foreach{ f => println( "key - " + f._1 + " :Value - " + f._2 ) 
                              if ( f._2 > 0 ) 
                                 check = "No"
                              else{
                                 if ( check != "No" ) 
                                   check = "Yes"
                              }                                 
                       }

      return check
  }
  //val anagramYN = anagramTest( "PUSHKAR", "RSHUAKP" )
  
  //Write a method to replace all spaces in a string with Ã¢â‚¬Ëœ%20Ã¢â‚¬â„¢
  def replaceSpaces( value: String): String = {
      var values = value
      //return value.replaceAll(" " , "%20")
      //return value.map{ x => if ( x.toString().equalsIgnoreCase(" ") ) "%20" else x }.toString()
      //value.foreach{ x => if ( x.equals(" ") ) "%20" else x }
      val noOSpaces = value.count { x => x.toString().trim().length() == 0 }
      println("noOSpaces - " + noOSpaces ); 
      for ( i <- 0 to ( values.size + (2 * noOSpaces) ) - 1 ) {
        //println( "i - " + i + " :Substring - " + values.substring( 0, i ) + " " + values.substring( i + 1, values.size ) + " :Value Check - " + values.apply(i))
         if ( values.apply(i).toString().trim().length() == 0 ) 
           values = values.substring(0, i) + "%20" + values.substring( i + 1, values.size )
      }
      return values
  };
  //val replaced = replaceSpaces("Push kar Goy al ");
  
  //Given an image represented by an NxN matrix, where each pixel in the image is 4 bytes, write a method to rotate the image by 90 degrees. Can you do this in place?
  def rotateImage( size: Int, matrix: Array[Array[Int]] ) = {  //Array[Array[Int]]
      for { i <- 0 to size - 1 
            j <- 0 to size -1 
         }{ println ( s" Before change - ($i)($j) - " + matrix(i)(j) )  }  
      var start = 0
      var end = size - 1
      for{ i <- 0 to size / 2  - 1
           j <- end to start by -1  
         } {
            if ( start == size - i ){
               start = i
               end = end - i
            }
            val temp = matrix(start)(i)
            matrix(start)(i) = matrix(i)(j)
            matrix(i)(j) = matrix(j)(end)
            matrix(j)(end) = matrix(end)(start)
            matrix(end)(start) = temp
            start = start + 1
      }
      //return matrix
     for { i <- 0 to size - 1 
        j <- 0 to size -1 
     }{ println ( s" After change - ($i)($j) - " + matrix(i)(j) )  }  
  }
   // Populate data
  /* val matrix: Array.ofDim[Int](4,5)
   * for{ i <- 0 to 5
       j <- 0 to 5
  }{
      matrix(i)(j) = i + j + 1  
  }
  for{ i <- 0 to 5
       j <- 0 to 5
  }{
      { println ( s" After change - ($i)($j) - " + matrix(i)(j) )  }
  }
  rotateImage( 6, matrix )
  */
  
  //Write an algorithm such that if an element in an MxN matrix is 0, its entire row and column is set to 0.
  def setRowColumnZero(rows: Int, columns: Int, matrix: Array[Array[Int]] ) = {
      //var matrix = Array.ofDim[Int](rows, columns)
      
      var zeroColumns = collection.mutable.ArrayBuffer[Int]()
      var zeroExists = false
      
      for { i <- 0 to rows - 1 
            j <- 0 to columns -1 
         }{ println ( s" Before change - ($i)($j) - " + matrix(i)(j) )  }  
         
      for { i <- 0 to rows - 1 
            j <- 0 to columns -1 
         }{
            //if value is  0 store column number
           // and mark to make row as 0
            if ( matrix(i)(j) == 0 ){
              zeroColumns.+=(j)
              zeroExists = true
            }
            //If column number exists in previous 0 column list mark value as 0
            else if ( zeroColumns.contains(j) ){
              matrix(i)(j) = 0
            }
            
            //if column has 0 make whole row 0
            if ( zeroExists ) {
              for( j <- 0 to columns -1 ){
                matrix(i)(j) = 0
              }
            }
              
          }
      
      //println( "Zero Columns " )
      //zeroColumns.foreach( println )
      //loop to make previuos rows as 0 if vaue exists in next rows
      for { i <- 0 to rows - 1 
            j <- 0 to columns -1 
         }{
            
            if ( zeroColumns.contains(j) ){
              matrix(i)(j) = 0
            }
         }
      
      for { i <- 0 to rows - 1 
            j <- 0 to columns -1 
         }{ println ( s" After change - ($i)($j) - " + matrix(i)(j) )  }       
    }
  
  // Populate data
  /* val matrix: Array.ofDim[Int](4,5)
   * for{ i <- 0 to 4 
       j <- 0 to 3
  }{
      if ( i == 1 && j == 2 ){
        matrix(i)(j) = 0
      }else if ( i == 2 && j == 1 ){
        matrix(i)(j) = 0
      }else if ( i == 3 && j == 3 ){
        matrix(i)(j) = 0
      }else{
        matrix(i)(j) = i + j + 1
      }   
  }*/
  //setRowColumnZero( 5, 4, matrix );
  
  //Assume you have a method isSubstring which checks if one word is a substring of another. Given two strings, s1 and s2, write code to check if s2 is a rotation of s1 using only one call to isSubstring (i.e., â€œwaterbottleâ€� is a rotation of â€œerbottlewatâ€�).
  def rotation( string1: String, string2: String ): String = {
    
      val newString = string1 + string1
      /*if ( isSubstring( string2, newString ) ) {
        return "Yes"
      }
      else return "No"
      */
      return "No"
  }
  
  def checkValues() = {
      val array = Array.ofDim[String](3,4 )
      array.size
  }
 }