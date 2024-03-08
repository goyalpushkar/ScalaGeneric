package com.cswg.practice

/*
 *  Greedy Florist
A group of friends want to buy a bouquet of flowers. The florist wants to maximize his number of new customers and the money he makes. To do this, he decides he'll multiply the price of each flower by the number of that customer's previously purchased flowers plus . The first flower will be original price,
, the next will be and so on.
Given the size of the group of friends, the number of flowers they want to purchase and the original prices of the flowers, determine the minimum cost to purchase all of the flowers.
For example, if there are friends that want to buy flowers that cost each will
buy one of the flowers priced flower in the list, , will now cost
.
Function Description
at the original price. Having each purchased flower, the first . The total cost will be
Complete the getMinimumCost function in the editor below. It should return the minimum cost to purchase all of the flowers.
getMinimumCost has the following parameter(s):
c: an array of integers representing the original price of each flower k: an integer, the number of friends
Input Format
The first line contains two space-separated integers and , the number of flowers and the number of friends.
The second line contains space-separated positive integers , the original price of each flower.
Constraints
Output Format
Print the minimum cost to buy all
Sample Input 0
33 256
Sample Output 0
13
Explanation 0
flowers.
There are flowers with costs
and people in the group. If each person buys one

 flower, the total cost of prices paid is
dollars. Thus, we print
as our answer.
          Sample Input 1
32 256
Sample Output 1
15
Explanation 1
    and people in the group. We can minimize the total 1. The first person purchases flowers in order of decreasing price; this means they buy the more
dollars and the less expensive flower ( dollars.
There are flowers with costs purchase cost like so:
                expensive flower ( ) first at price ) second at price
                dollars.
2. The second person buys the most expensive flower at price
                          We then print the sum of these purchases, which is
, as our answer.
        Sample Input 2
53 13579
Sample Output 2
29
Explanation 2
The friends buy flowers for
, and , and
for a cost of
.
                               
 */
class HackerRankGreedyFlorist {
   // Complete the getMinimumCost function below.
    def getMinimumCost(k: Int, c: Array[Int]): Int = {

        val newC = c.sortWith( _ > _ )
        var sum = 0 
        var noOfVisits = 0
        var friendNum = 0
        for ( index <- 0 until newC.size ) {
            if ( friendNum < k ){
                sum = sum + ( newC.apply(index) * ( noOfVisits + 1 ) )
            }else{
                noOfVisits.+=(1)
                friendNum = 0
                sum = sum + ( newC.apply(index) * ( noOfVisits + 1 ) )
            }
            friendNum.+=(1)
        }

        return sum
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        //val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))
        val nk = stdin.readLine.split(" ")

        val n = nk(0).trim.toInt

        val k = nk(1).trim.toInt

        val c = stdin.readLine.split(" ").map(_.trim.toInt)
        val minimumCost = getMinimumCost(k, c)
        println( minimumCost)
        //printWriter.println(minimumCost)
        //printWriter.close()
    }  
}