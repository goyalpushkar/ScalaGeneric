package com.cswg.practice

/*
 *                                                                                    Luck Balance
Lena is preparing for an important coding competition that is preceded by a number of sequential preliminary contests. She believes in "saving luck", and wants to check her theory. Each contest is described by two integers, and :
is the amount of luck associated with a contest. If Lena wins the contest, her luck balance will decrease by ; if she loses it, her luck balance will increase by .
denotes the contest's importance rating. It's equal to if the contest is important, and it's equal to if it's unimportant.
If Lena loses no more than important contests, what is the maximum amount of luck she can have after competing in all the preliminary contests? This value may be negative.
For example, and:
Contest L[i]T[i] 151
211
340
If Lena loses all of the contests, her will be
contests, and there are only important contests. She can lose all three contests to maximize her luck at
. If , she has to win at least of the important contests. She would choose to win the lowest value important contest worth . Her final luck will be .
Input Format
The first line contains two space-separated integers and , the number of preliminary contests and the maximum number of important contests Lena can lose.
Each of the next lines contains two space-separated integers, and , the contest's luck balance and its importance rating.
Constraints
Output Format
Print a single integer denoting the maximum amount of luck Lena can have after all the contests.
Sample Input
63 51 21 11 81 10 0 50
                                    . Since she is allowed to lose
important
                                     Sample Output

  29
Explanation
There are contests. Of these contests,
them. Lena maximizes her luck if she wins the other five contests for a total luck balance of
are important and she cannot lose more than of
important contest (where ) and loses all of the .
                               
 */

import java.io._

object HackerRankLuckBalance {
     // Complete the luckBalance function below.
    def luckBalance(k: Int, contests: Array[Array[Int]]): Int = {
        var totalLuck = 0
        var importantContests = 0 
        val newContests = contests.sortWith( _.apply(0) > _.apply(0) )
          //contests.sortBy { x => ( x.apply(1) ) }
        //newContests.foreach { x => println( x.apply(0) + " " + x.apply(1) ) }
        for ( i <- 0 until contests.size ) {
             if ( newContests.apply(i).apply(1) == 1 ){
                if ( importantContests < k ){
                   totalLuck = totalLuck + newContests.apply(i).apply(0)
                }else{
                  totalLuck = totalLuck - newContests.apply(i).apply(0)
                }
             }else{
               totalLuck = totalLuck + newContests.apply(i).apply(0)
             }
          
        }
        return totalLuck
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
         System.setIn(new FileInputStream( fileLocation + "HackerRankLuckBalance.txt"));
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val nk = stdin.readLine.split(" ")
        val n = nk(0).trim.toInt
        val k = nk(1).trim.toInt
        val contests = Array.ofDim[Int](n, 2)

        for (i <- 0 until n) {
            contests(i) = stdin.readLine.split(" ").map(_.trim.toInt)
        }

        val result = luckBalance(k, contests)
        println( result )
        //printWriter.println(result)
        //printWriter.close()
    }
}