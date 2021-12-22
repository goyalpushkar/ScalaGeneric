package com.cswg.practice

class PracticeRecursive {
  
}

object PracticeRecursive {
  
   //Write a method to generate the factorial of a number
    def factorial( number: Int  ): Int = {
      if ( number <= 1 )
         return 1
      else 
        return number * factorial( number -1 )       
    }
    
    def factorialrec( number: Int  ): Int = {
      import scala.annotation.tailrec
      
      @tailrec
       def factorialTail( number: Int, acc: Int ): Int = {
      if ( number <= 1 )
         return acc
      else 
         factorialTail( number - 1, number * acc )
      }
      
      factorialTail( number, 1 )
    }
    //val fac = factorialrec(5)
    
    //Write a method to generate the nth Fibonacci number
    def fibonacciSeries( start: Int, next: Int, number: Int  ): Int = {
        import scala.annotation.tailrec
        
        @tailrec
        def fibonacci( start: Int, next: Int, number: Int ): Int = {
            if ( next == number )
               return number
            else if ( next > number ) 
               return 0
            else 
              return  fibonacci(next, start + next, number) 
        }
          
       val acc = 0
       fibonacci(start, next, number)
    }
    //fibonacciSeries( 0, 1, 28 )
    
    def fibonacciSeriesRec( number: Int  ): Int = {
        import scala.annotation.tailrec
        
        @tailrec
        def fibonacci( start: Int, next: Int, number: Int ): Int = {
            if ( next == number )
               return number
            else if ( next > number ) 
               return 0
            else 
              return  fibonacci(next, start + next, number) 
        }
          
       val acc = 0
       fibonacci(0, 1, number)
    }
   
    /*
     * Imagine a robot sitting on the upper left hand corner of an NxN grid The robot can only move in two directions: right and down How many possible paths are there for the robot?
FOLLOW UP
Imagine certain squares are â€œoff limitsâ€�, such that the robot can not step on them Design an algorithm to get all possible paths for the robot
     */
    def robotPaths ( gridSize: Int ) = {
        
      var pathN = 0
      //Right Path
      for { i <- gridSize - 1 to 1 
            j <- i to gridSize - 1 
          } {
          pathN = pathN.+(1)
          println( " Path - " + pathN + " : "
                + " Move Right = " + i
                + " Move Down = " +  j
                + " Move Right = " + () 
                + " Move Down = " + () 
                )
          
          
      }
      
      //Down Path
      for ( i <- 1 to gridSize - 1 ) {
          pathN = pathN.+(1)
          println( " Path - " + pathN + " : "
                + " Move Down = " + i
                + " Move Right = " +  ( gridSize - 1 ) 
                + " Move Down = " + ( gridSize - i - 1 )
                )
          
          
      }
      
      println( "Total Paths - " + pathN ) 
    }
    //robotPaths(5)
    
    def roboPath( x: Int, y: Int ) = {
        val z = new java.awt.Point( x, y )
        //is_free(x,y)
    }
    
    //Write a method that returns all subsets of a set
    
    
    //Write a method to compute all permutations of a string
    def getPermutations( string: String ) = {
      
        def insertChar( word: String, char: Char, position: Int ): String = {
           
            val newWord = word.substring(0, position ) + char + word.substring(position)
            println( " word - " + word + " :Char "  + char + " :position - " + position + "\n"
                  + " newWord - " + newWord )
            return newWord
        }
        
        
        //, permutations: collection.mutable.ArrayBuffer[String]
        def getPerms( string: String ): collection.mutable.ArrayBuffer[String] = {
            val permutations = collection.mutable.ArrayBuffer[String]() 
            
            if( string == null ) 
               return null
            else if ( string.length() == 0) {
              permutations.++=( Seq("") )
              return permutations
            }
               
            val firstChar = string.apply(0)
            val subString = string.substring(1)
            println( " firstChar - " + firstChar + " :subString - " + subString )
            val words = getPerms( subString )
            words.foreach (x => println( "Got words - " + x ) )
            for ( word <- words ) {
               for ( j <- 0 to word.size  ){
                  permutations.++=( Seq( insertChar( word, firstChar, j ) ) )
               }
            }
            return permutations
        }
        
        val allPermutations = getPerms( string )
        allPermutations.foreach { x => println ( x )  }
    }
    //getPermutations("abc")
    
    /*
     * Implement an algorithm to print all valid (e g , properly opened and closed) combi- nations of n-pairs of parentheses
      EXAMPLE:
      input: 3 (e g , 3 pairs of parentheses) output: ()()(), ()(()), (())(), ((()))
     */
    def parentheses( numberofPairs: Int ) = {
      
        def printParantheses( numberofPairs: Int  ): collection.mutable.ArrayBuffer[String] = {
            val totalParanth = collection.mutable.ArrayBuffer[String]()  
            
            if ( numberofPairs == 0 ){
               totalParanth.++=( Seq("") ) 
               return totalParanth
            }
              
            
            for ( i <- numberofPairs to 0 by -1) {
               val newParanth = printParantheses( i )
               for ( j <- 1 to i ) {
                 totalParanth.++=( newParanth )
               }
            }
            
            return totalParanth
        }
        
        val allParanth = printParantheses( numberofPairs ) 
        allParanth.foreach( println )
    }
    
    /*
     * Implement the â€œpaint fillâ€� function that one might see on many image editing pro- grams That is, given a screen (represented by a 2 dimensional array of Colors), a point, and a new color, fill in the surrounding area until you hit a border of that col- orâ€™
     */
    
    /*
     * Given an infinite number of quarters (25 cents), dimes (10 cents), nickels (5 cents) and pennies (1 cent), write code to calculate the number of ways of representing n cents
     */
    
    /*
     * Write an algorithm to print all ways of arranging eight queens on a chess board so that none of them share the same row, column or diagonal
     */
}