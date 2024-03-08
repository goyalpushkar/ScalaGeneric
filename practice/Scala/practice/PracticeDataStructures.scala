package com.cswg.practice

class PracticeDataStructures {
  
}

object PracticeDataStructures {
  
  // It is given a set of strings S. For example S = {ocean, beer, money, happiness}. T
  //The task is to write a program, which prints all subsets of S.
    def listAllSubsets( array: Array[String] ) = {
        import collection.mutable._
        
        val subsetQueue = collection.mutable.Queue[ collection.mutable.HashSet[String] ]()
        val subset = collection.mutable.HashSet[String]()
        subsetQueue.enqueue(subset)
        
        while(subsetQueue.size > 0 ) {
            val element = subsetQueue.dequeue()
            
            println( "{ ")
            element.foreach( x => println( "Element - " + x ) )
            println( " } ")
            
            /*var start = -1
            
            if ( element.size > 0 ) 
               start = element.apply( element.size - 1 )
               
            println ( " Start - " + start )
            *
            */
            for( word <- array ) {
              if ( !element.contains( word) ) {
                 var newList = HashSet[String]()
                 newList.++=( element )
                 newList.+=( word )
                 subsetQueue.enqueue( newList )
              }
            }
            
        }       
        
    } 
    
    def listAllSubsets2( array: Array[String] ) = {
        import collection.mutable._
        
        val subsetQueue = collection.mutable.Queue[ List[Int] ]()
        val subset = List[Int]()
        subsetQueue.enqueue(subset)
        
        while( subsetQueue.size > 0 ) {
            val element = subsetQueue.dequeue()
            
            println( "{ ")
            element.foreach( x => println( "Element - " + array(x) ) )
            println( " } ")
            
            var start = -1
            
            if ( element.size > 0 ) 
               start = element.apply( element.size - 1 )
               
            println ( " Start - " + start )
  
            for( i <- start + 1 to array.size-1  ) {
                 var newList = List[Int]()
                 newList = newList.++( element )
                 newList = newList.:+(i)
                 subsetQueue.enqueue( newList )
            }
            
        }       
        
    } 
    val array = Array[String]("ocean", "bear", "money", "happiness" )
    listAllSubsets( array )
    listAllSubsets2( array )
    
    
}