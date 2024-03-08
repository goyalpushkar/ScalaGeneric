package com.cswg.practice

/*
 * Triple sum
Given arrays of different sizes, find the number of distinct triplets where is an element
of , written as , , and , satisfying the criteria: .
For example, given and , we find four distinct triplets:
.
Function Description
Complete the triplets function in the editor below. It must return the number of distinct triplets that can be
formed from the given arrays.
triplets has the following parameter(s):
a, b, c: three arrays of integers .
Input Format
The first line contains integers , the sizes of the three arrays.
The next lines contain space-separated integers numbering respectively.
Constraints
Output Format
Print an integer representing the number of distinct triplets.
Sample Input 0
3 2 3
1 3 5
2 3
1 2 3
Sample Output 0
8
Explanation 0
The special triplets are .
Sample Input 1
3 3 3
1 4 5
2 3 3
1 2 3
Sample Output 1
5
Explanation 1
The special triplets are
Sample Input 2
4 3 4
1 3 5 7
5 7 9
7 9 11 13
Sample Output 2
12
Explanation 2
The special triplets are
.
 */

import java.io._;

object HackerRankTripleSum {
 
     // Complete the triplets function below.
    def triplets(a: Array[Int], b: Array[Int], c: Array[Int]): Long = {
   
        //val sortedA = a.sortWith(_ < _)
        //val sortedC = c.sortWith(_ < _)
        var totalTriplets: Long = 0
        
        for( elem <- b.distinct ){
            val aless = a.count( _ <= elem )
            val cless = c.count( _ <= elem )
            
            totalTriplets.+=( aless * cless )
            
            System.out.println( elem + " -> " + totalTriplets )
        }
        
        return totalTriplets
    }
    
   //Same efficient as above
   def tripletsLP(a: Array[Int], b: Array[Int], c: Array[Int]): Long = {
       
        import scala.util.control.Breaks._
        val sortedA = a.sortWith(_ < _)
        val sortedC = c.sortWith(_ < _)
        var totalTriplets: Long = 0
        
        for( elem <- b.distinct ){
            var aless = 0
            var cless = 0
            breakable{
                for( aElem <- sortedA ){
                    if ( aElem <= elem )
                       aless.+=(1)
                    else
                       break
                }
            }
            
            breakable{
                for( cElem <- sortedC ){
                    if ( cElem <= elem )
                       cless.+=(1)
                    else
                       break
                }
            }
            totalTriplets.+=( aless * cless )
            
            System.out.println( elem + " -> " + totalTriplets )
        }
        
        return totalTriplets
    }    

   //Same efficient as above
   def tripletsP(a: Array[Int], b: Array[Int], c: Array[Int]): Long = {
       
        //import scala.collection.mutable.HashMap
        import scala.util.control.Breaks._
        val sortedA = a.distinct.sortWith(_ < _)
        val sortedB = b.distinct.sortWith(_ < _)
        val sortedC = c.distinct.sortWith(_ < _)
        var totalTriplets: Long = 0
        //val triplets = new HashMap[Int, Long]()
        
        var aIndex: Long = 0;
        var bIndex: Long = 0;
        var cIndex: Long = 0;
        while ( bIndex < sortedB.size ){
           
             while ( aIndex < sortedA.size && sortedA.apply(aIndex.toInt) <= sortedB.apply(bIndex.toInt) ){
                  aIndex.+=(1)
             }
             
             while ( cIndex < sortedC.size && sortedC.apply(cIndex.toInt) <= sortedB.apply(bIndex.toInt) ){
                  cIndex.+=(1)
             }
           
             totalTriplets.+=( aIndex * cIndex )
               
             System.out.println( sortedB.apply(bIndex.toInt)  + " -> " + totalTriplets )
             
             bIndex.+=(1)
        }

        
        return totalTriplets
    }    
   
    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.out.println( fileLocation )
        System.setIn(new FileInputStream( fileLocation + "HackerRankTripleSum.txt"));
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val lenaLenbLenc = stdin.readLine.split(" ")

        val lena = lenaLenbLenc(0).trim.toInt
        val lenb = lenaLenbLenc(1).trim.toInt
        val lenc = lenaLenbLenc(2).trim.toInt
        System.out.println( lena  + " " + lenb + " " + lenc )
        
        val arra = stdin.readLine.split(" ").map(_.trim.toInt)
        val arrb = stdin.readLine.split(" ").map(_.trim.toInt)
        val arrc = stdin.readLine.split(" ").map(_.trim.toInt)
        
        val ans = tripletsP(arra, arrb, arrc)
        println( ans )
        //printWriter.println(ans)
        //printWriter.close()
    }
}