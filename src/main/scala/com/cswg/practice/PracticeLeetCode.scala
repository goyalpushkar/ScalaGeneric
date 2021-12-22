package com.cswg.practice

class PracticeLeetCode {
  
}

object PracticeLeetCode {
    
    def numJewelsInStones(J: String, S: String): Int = {
  
      var count = 0
      S.foreach{ x => if ( J.contains(x) ) count = count.+(1) }
      
      return count
    }
    //val Jewels = "aA"
    //val Stones = "aAAbbbb"
    //val Jewels2 = "z"
    //val Stones2 = "Z"    
    //numJewelsInStones( Jewels, Stones )
    
    //Given a string, find the length of the longest substring without repeating characters.
    def lengthOfLongestSubstring(s: String): Int = {
        
         val hashMap = collection.mutable.HashMap[String, Int]()
         for ( i <- 0 to s.size - 1 ){
           
           for ( j <- i + 1 to s.size - 1 ){
             
             if ( s.substring(j, j + i + 1 ).contains( s.substring( 0, i + 1 ) ) ){
               if ( !hashMap.contains ( s.substring( 0, i + 1 ) ) )
                 hashMap.put( s.substring( 0, i ), 1 )
               else
                 hashMap.put( s.substring( 0, i ), hashMap.getOrElse( s.substring( 0, i + 1 ) + 1, 0 ) )
             }
           }
         }
        
         hashMap.foreach(f => println ( f._1 + " - " + f._2 ) )
        return 1
    }
    
    def lengthOfLongestSubstring2(s: String): Int = {
        
        var i = 0
        var j = 0
        var max = 0;
        val set = collection.mutable.HashSet[Char]();
    
        while (j < s.length()) {
            println( "s(j) - " + s(j) )
            if (!set.contains(s(j))) {
                set.add( s(j));
                j = j.+(1)
                max = Math.max(max, set.size);
            } else {
                set.remove(s(i));
                i = i.+(1)
            }
        }
    
        return max;
    }
    val s = "abcabcbb"
    val s2 = "bbbbb"
    val s3 = "asasasasasbabababa"
    lengthOfLongestSubstring2(s)
    
    //Power function
    def myPow(x: Double, n: Int): Double = {
        import scala.annotation.tailrec
        
        @tailrec
        def power( x: Double, n: Int ): Double = {
           println( " x - " + x + " n - " + n )
            n match
            {
                case 1 => return x
                case _ =>  power( x * x, n -1 )
            }
           /*if ( n == 1) 
             return x
           else 
              power( x * x , n -1 )*/
        }
        
        return power(x, n)
    }
    myPow(5, 4)
}