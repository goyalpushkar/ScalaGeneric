package com.cswg.practice

/*
 *  Recursion: Davis'
Staircase
Davis has a number of staircases in his house and he likes to climb each staircase , , or steps at a time. Being a very precocious child, he wonders how many ways there are to reach the top of the staircase.
Given the respective heights for each of the staircases in his house, find and print the number of ways he can climb each staircase, module on a new line.
For example, there is staircase in the house that is steps high. Davis can step on the following sequences of steps:
                   11111 1112 1121 1211 2111 122 221 212 113 131 311
23 32
There are
possible ways he can take these
steps.
                   Function Description
Complete the stepPerms function in the editor below. It should recursively calculate and return the integer number of ways Davis can climb the staircase, modulo 10000000007.
stepPerms has the following parameter(s):
n: an integer, the number of stairs in the staircase
Input Format
The first line contains a single integer, , the number of staircases in his house. Each of the following lines contains a single integer, , the height of staircase .
     Constraints
Subtasks
Output Format
for of the maximum score.
                       For each staircase, return the number of ways Davis can climb it as an integer.
Sample Input
3 1 3 7
 
  Sample Output
 1 4 44
Explanation
Let's calculate the number of ways of climbing the first two of the Davis'
staircases:
   1. The first staircase only has step, so there is only one way for him to climb it (i.e., by jumping step). Thus, we print on a new line.
     2. The second staircase has 1.
2. 3. 4.
Thus, we print on a new line.
steps and he can climb it in any of the four following ways:
                
 */

import java.io._

object HackerRankDavisStaircase {
  
    def stepPermsRec( n: Int, d: Int ): Int = {

        val quotient = n / d
        val remainder = n % d

        if ( remainder == 0 ) 
           return 1
        else if ( remainder == 1 )
           return quotient + 1
        else if ( remainder == 2 )
           return ( ( 2 * quotient ) + 3 )
        else 
           return 0
    }

    // Complete the stepPerms function below.
    def stepPerms(n: Int): Int = {
        return stepPermsRec( n, 1) + stepPermsRec( n, 2) + stepPermsRec( n, 3)
    }


    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        System.setIn(new FileInputStream( fileLocation + "HackerRankMaxMin.txt"));
        val s = stdin.readLine.trim.toInt

        for (sItr <- 1 to s) {
            val n = stdin.readLine.trim.toInt

            val res = stepPerms(n)
            println(res)
            //printWriter.println(res)
        }

        //printWriter.close()
    }  
}