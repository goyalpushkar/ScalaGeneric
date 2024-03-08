package com.cswg.practice


/*
 * Mr Ski is a famous Cricket Bat manufacturer in Meerut. He has been in the business for a long time now. He has been developing the special bats with the changing times to meet up the expectations of the players. He was researching for a while to develop new kind of bats as the cricket is moving at a fast pace and he has to maintain his reputation and quality of bats. 


He has developed special kind of bats and the word has spread all over the city. The bats have a weight which provides the balance to the 
cricketer and are ready to use. The bats can be directly put to action without knocking. There are N such bats with each bat having a 
weight Wi and a minimum price Mi. Mr Ski knows the worth of his bats and has decided to sell only one bat per person.




There are C Cricketers who have contacted Mr. Ski and each of them has different requirements. A cricketer Cj will only be interested in a 
bat with a weight greater than Gj and he can spend maximum money of Pj. 

Example:

Consider the 4 bats made by Mr Ski.

Suppose the cricketer wants a bat with a weight greater than 5 and the maximum money he can spend is 100. With the given requirements, the cricketer would be interested in Bat 3 and Bat 4.

Mr Ski wants to know the maximum number of bats he would be able to sell. He is busy in showing bats to the cricketers and needs your help. Can you tell him the maximum number of bats he would be able to sell?


Input Format
The first line of input consists of two space-separated integers, the number of bats (N) and the number of Cricketers (C)

The next C lines each consists of two space-separated integers, the requirement of the weight of the bat (Gj) and maximum price the cricketer can spend (Pj)

The next N lines each consists of two space-separated integers, the weight of the bat (Wi) and minimum price of the bat (Mi)

Constraints
1<= N <=1000

1<= C <=1000

1<= Wi <=10^9

1<= Mi <=10^9

1<= Gj <=10^9

1<= Pj <=10^9


Output Format
Print the required output in a separate line.

Sample TestCase 1
Input
4 4
5 100
7 80
10 90
6 150
8 100
10 150
9 60
7 80
Output
3
 * 
 * 
 * Explanation

The bats made by Mr Ski with their weights and minimum price are:


A cricketer will be interested in a bat if the weight of a bat is greater than Gj and its price is less than or equal to P.

Following this, respective cricketers would be interested in the following bats:

Cricketer 1: Weight of Bat should be greater than 5.

Maximum money he can spend = 100


Bats he would be interested in are Bat 1, Bat 3 and Bat 4.



Cricketer 2: Weight of Bat should be greater than 7.

Maximum money he can spend = 80


He would only be interested in Bat 3.



Cricketer 3: Weight of Bat should be greater than 10.

Maximum money he can spend = 90


There is no bat meeting his requirements. Hecne, he would not be interested in any of the bat.



Cricketer 4: Weight of Bat should be greater than 6.

Maximum money he can spend = 150


All the bats meet up his requirements and thus he would be interested in all the 4 bats, Bat 1, Bat 2, Bat 3 and Bat 4


Mr. Ski would sell only one bat per cricketer. So, the maximum number of bats which Mr. Ski can sell is 3.

 */

import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._

class TechgigCricketBatM {
  
}

object TechgigCricketBatM {

    def main(args: Array[String]) {

        // Write code here
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("C:/opt/cassandra/default/dataFiles/CricketBatManu.txt"));
        ///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/CricketBatManu.txt
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val nk = stdin.readLine.split(" ")
        val nB = nk(0).trim.toInt
        val nC = nk(1).trim.toInt

        println( nB + "->" + nC )
        
        for ( i <- 1 to nC ){
            val weight = stdin.readLine.split(" ").apply(0).trim().toInt
            val price = stdin.readLine.split(" ").apply(1).trim().toInt
        }
        
        for ( i <- 1 to nB ){
            val minWeight = stdin.readLine.split(" ").apply(0).trim().toInt
            val minPrice = stdin.readLine.split(" ").apply(1).trim().toInt
        }
        println(result)
        //printWriter.println(result)
        //printWriter.close()
    }

}