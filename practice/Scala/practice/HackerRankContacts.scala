package com.cswg.practice

import java.io.PrintWriter
import java.io.FileInputStream

/*
 * Contacts
We're going to make our own Contacts application! The application must perform two types of operations:
1. add name , where is a string denoting a contact name. This must store as a new
contact in the application.
2. find partial , where is a string denoting a partial name to search the application for. It must
count the number of contacts starting with and print the count on a new line.
Given sequential add and find operations, perform each operation in order.
Input Format
The first line contains a single integer, , denoting the number of operations to perform.
Each line of the subsequent lines contains an operation in one of the two forms defined above.
Constraints
It is guaranteed that and contain lowercase English letters only.
The input doesn't have any duplicate for the operation.
Output Format
For each find partial operation, print the number of contact names starting with on a new line.
Sample Input
4
add hack
add hackerrank
find hac
find hak
Sample Output
2
0
Explanation
We perform the following sequence of operations:
1. Add a contact named hack .
2. Add a contact named hackerrank .
3. Find and print the number of contact names beginning with hac . There are currently two contact
names in the application and both of them start with hac , so we print on a new line.
4. Find and print the number of contact names beginning with hak . There are currently two contact
names in the application but neither of them start with hak , so we print on a new line.
 */
class HackerRankContacts {
  
}

object HackerRankContacts {
   
    import scala.collection.mutable.HashMap
    import scala.collection.mutable.ArrayBuffer
    val subContacts: HashMap[String, Int] = HashMap[String, Int]()
    var contacts = Array[String]()
    
    def addSubContact( value: String ) = {
        subContacts.+=( value -> ( subContacts.getOrElse( value, 0) + 1 )  )
    }
    
    def findSubContact( value: String ): Int = {
        return subContacts.getOrElse( value, 0 )
    }
    
    def addName( name: String ) = {
        //println( name )
        //contacts = contacts.:+( name )
        for( i <- 1 to name.length() ) {
           addSubContact( name.substring(0, i) )
        }
        //subContacts.foreach(f => print( f._1 + " " + f._2 + "\t" ) )
    }
    
    def findPartial( partial: String ): Int = {
        //println( partial )
        return findSubContact( partial )
    }
    /*
     * Complete the contacts function below.
     */
    def contacts(queries: Array[Array[String]]): Array[Int] = {
        /*
         * Write your code here.
         */
        var returnArray = ArrayBuffer[Int]()
        queries.foreach(f => if ( "add".equalsIgnoreCase( f.apply(0) ) ){
                                addName(f.apply(1))
                            }else if ( "find".equalsIgnoreCase( f.apply(0) ) ){
                                                   val count = findPartial(f.apply(1))
                                                   returnArray.+=( count )
                                                   //returnArray = returnArray.:+(count)
                            }
                       )
                       
         return returnArray.toArray

    }
    

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankContacts.txt"));
        ///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankCountingInversions.txt
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        
        val queriesRows = stdin.readLine.trim.toInt
        val queries = Array.ofDim[String](queriesRows, 2)

        for (queriesRowItr <- 0 until queriesRows) {
            queries(queriesRowItr) = stdin.readLine.split(" ")
        }

        val result = contacts(queries)
        println(result.mkString("\n"))
        
        //printWriter.println(result.mkString("\n"))
        //printWriter.close()
    }    
}