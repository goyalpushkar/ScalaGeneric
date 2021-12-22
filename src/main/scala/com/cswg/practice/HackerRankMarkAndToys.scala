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

class HackerRankMarkAndToys {
  
}

/*
 * Mark and Jane are very happy after having their first child. Their son loves toys, so Mark wants to buy some. There are a number of different toys lying in front of him, tagged with their prices. Mark has only a certain amount to spend, and he wants to maximize the number of toys he buys with this money.

Given a list of prices and an amount to spend, what is the maximum number of toys Mark can buy? For example, if  and Mark has  to spend, he can buy items  for , or  for  units of currency. He would choose the first group of  items.

Function Description

Complete the function maximumToys in the editor below. It should return an integer representing the maximum number of toys Mark can purchase.

maximumToys has the following parameter(s):

prices: an array of integers representing toy prices
k: an integer, Mark's budget
Input Format

The first line contains two integers,  and , the number of priced toys and the amount Mark has to spend. 
The next line contains  space-separated integers 

Constraints
1 <= n <= 10^5
1 <= k <= 10^9
1 <= price(i) <= 10^9
 
A toy can't be bought multiple times.

Output Format

An integer that denotes the maximum number of toys Mark can buy for his son.

Sample Input

7 50
1 12 5 111 200 1000 10
Sample Output

4
Explanation

He can buy only  toys at most. These toys have the following prices: .
 */
object HackerRankMarkAndToys{
    
    // Complete the maximumToys function below.
    def maximumToys(prices: Array[Int], k: Int): Int = {

        import scala.util.control.Breaks._
        val newArray = prices.sortWith( _ < _)
        var sumPrice = 0
        var maxToys = 0
        
        breakable{ 
              newArray.foreach { x => sumPrice = sumPrice.+(x)
                                      if ( sumPrice < k ) 
                                         maxToys = maxToys.+(1)
                                      else
                                         break
                               }
        }
        return maxToys
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn
        System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankMarkAndToys.txt"));
        
        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val nk = stdin.readLine.split(" ")
        val n = nk(0).trim.toInt
        val k = nk(1).trim.toInt

        println( n + "->" + k )
        val prices = stdin.readLine.split(" ").map(_.trim.toInt)
        val result = maximumToys(prices, k)
        println(result)
        //printWriter.println(result)
        //printWriter.close()
    }  
}