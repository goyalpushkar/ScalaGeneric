package com.cswg.practice

import java.io._;
import scala.util.matching.Regex

object IsPalindrome {
  
     def isPalindrome( providedString: String ): Boolean = {
        
         var left = 0
         var right = providedString.size - 1
         while( left < right ) {
             while ( !providedString.charAt(left).isLetter  ) 
                 left.+=(1)
                 
             while ( !providedString.charAt(right).isLetter ) 
                 right.-=(1)
                 
             if ( providedString.charAt(left).toLower == providedString.charAt(right).toLower ){
                 left.+=(1)
                 right.-=(1)
             }else{
                return false;
             }
               
         }
         return true;
     }
     
     def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream( fileLocation + "IsPalindrome.txt"));
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val a = stdin.readLine
        println( a )
        val res = isPalindrome(a)

        System.out.println( res )
        //printWriter.println(res)
        //printWriter.close()
    }
}