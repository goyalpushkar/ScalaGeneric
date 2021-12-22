package com.cswg.practice

/*
 *  Recursion: Fibonacci
Numbers
The Fibonacci Sequence
The Fibonacci sequence appears in nature all around us, in the arrangement of seeds in a sunflower and the spiral of a nautilus for example.
The Fibonacci sequence begins with and as its first and second terms. After these first two elements, each subsequent element is equal to the sum of the previous two elements.
Programmatically:
Given , return the number in the sequence.
As an example, . The Fibonacci sequence to is . With zero-based indexing, .
Function Description
Complete the recursive function in the editor below. It must return the Fibonacci sequence.
fibonacci has the following parameter(s):
n: the integer index of the sequence to return
Input Format
The input line contains a single integer, .
Constraints
Output Format
Locked stub code in the editor prints the integer value returned by the
Sample Input
3
Sample Output
2
Explanation
element in the
The Fibonacci sequence begins as follows:
function.

 ...
We want to know the value of . In the sequence above, evaluates to .
 * 
 */
object HackerRankFibonnaci {
  
    def fibonacci(x:Int):Int = {

        if ( x == 0 )
           return 0
          
        if ( x == 1 )
           return 1
           
        return fibonacci(x-1) + fibonacci(x-2)
          
	    	// Write your code here. You may add a helper function as well, if necessary.
    }

    def main(args: Array[String]) {
         /** This will handle the input and output**/
         println(fibonacci(readInt()))

    }
}