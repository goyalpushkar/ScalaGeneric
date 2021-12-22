package com.cswg.practice

import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._

class HackerRankNewYearChaos {
  
}

/*
It's New Year's Day and everyone's in line for the Wonderland rollercoaster ride! There are a number of people queued up, and each person wears a sticker indicating their initial position in the queue. Initial positions increment by  from  at the front of the line to  at the back.

Any person in the queue can bribe the person directly in front of them to swap positions. If two people swap positions, they still wear the same sticker denoting their original places in line. One person can bribe at most two others. For example, if n=8 and Person5 bribes Person4, the queue will look like this: .
1,2,3,5,4,6,7,8

Fascinated by this chaotic queue, you decide you must know the minimum number of bribes that took place to get the queue into its current state!

Function Description

Complete the function minimumBribes in the editor below. It must print an integer representing the minimum number of bribes necessary, or Too chaotic if the line configuration is not possible.

minimumBribes has the following parameter(s):

q: an array of integers
Input Format

The first line contains an integer , the number of test cases.

Each of the next  pairs of lines are as follows: 
- The first line contains an integer , the number of people in the queue 
- The second line has n space-separated integers describing the final state of the queue.

Constraints
1 <= t <= 10
1 <= n <= 10^5

Subtasks

For 60% score 1 <= n <= 10^3
For 100% score  1 <= n <= 10^5

Output Format

Print an integer denoting the minimum number of bribes needed to get the queue into its final state. Print Too chaotic if the state is invalid, i.e. it requires a person to have bribed more than  people.

Sample Input
2
5
2 1 5 3 4
5
2 5 1 3 4
Sample Output
3
Too chaotic

Explanation

Test Case 1

The initial state:
pic1(1).png
After person  moves one position ahead by bribing person :
pic2.png
Now person  moves another position ahead by bribing person :
pic3.png
And person  moves one position ahead by bribing person :
pic5.png
So the final state is  after three bribing operations.

Test Case 2
No person can bribe more than two people, so its not possible to achieve the input state.
 */
object HackerRankNewYearChaos {

    // Complete the minimumBribes function below.
    // Anyone who bribed P cannot get to higher than one position in front of P's original position,
    def minimumBribes(q: Array[Int]) {

        def precedingHighers( value: Int, startIndex:Int, tillIndex: Int ): Int = {
            var higerNumbers = 0
            for ( i <- startIndex to tillIndex ){
                if ( q.apply(i) > value ) {
                    higerNumbers = higerNumbers.+(1)
                }
            }
            println( "higerNumbers - " + higerNumbers )
            return higerNumbers
        }
        
        var tooChaotic = 0
        var minimumBribes = 0
        import scala.util.control.Breaks._
        breakable{
          for ( i <- 0 to q.size - 1 ){
              val actualIndex = ( q.apply(i) - 1 ) - i
              if ( actualIndex > 2 ) {
                 tooChaotic = 1;
                 break
              }else{
                val higherNumbers = precedingHighers( q.apply(i), Math.max(0, q.apply(i) - 2 ), i ) 
                 minimumBribes = minimumBribes + higherNumbers
              }
              /*else if ( actualIndex < 0 ) {
                 val higherNumbers = precedingHighers( q.apply(i), i ) 
                 minimumBribes = minimumBribes + ( higherNumbers + actualIndex ) 
              }else{
                minimumBribes = minimumBribes + actualIndex
              }*/
              println( " minimumBribes - " + minimumBribes + "\t" + " :tooChaotic - " + tooChaotic )
          }
        }
        
        if ( tooChaotic.equals(1) ) {
            println ( "Too chaotic" ) 
        }else{
            println ( minimumBribes )
        }

    }

    def main(args: Array[String]) {
        //val stdin = scala.io.StdIn
        //val t = stdin.readLine.trim.toInt
        val t = args(0).trim.toInt
        println( "No of test cases - " + t )
        var param = 1
        for (tItr <- 1 to t) {
            //val n = stdin.readLine.trim.toInt
            //val q = stdin.readLine.split(" ").map(_.trim.toInt)
            val n = args(param).trim.toInt
            val q = args(param + 1).split(" ").map(_.trim.toInt)
            minimumBribes(q)
            param = param.+(2)
        }
    }  
}