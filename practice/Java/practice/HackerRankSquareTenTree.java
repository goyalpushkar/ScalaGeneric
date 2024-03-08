package com.cswg.practice;

/*
 *  Square-Ten Tree
The square-ten tree decomposition of an array is defined as follows:
The lowest ( ) level of the square-ten tree consists of single array elements in their natural order.
The length the length
level (starting from ) of the square-ten tree consists of subsequent array subsegments of
in their natural order. Thus, the level contains subsegments of length ,
level contains subsegments of length , the level contains subsegments of , etc.
In other words, every level (for every ) of square-ten tree consists of array subsegments indexed as:
Level consists of array subsegments indexed as .
The image below depicts the bottom-left corner (i.e., the first array elements) of the table representing a square-ten tree. The levels are numbered from bottom to top:
Task
Given the borders of array subsegment , find its decomposition into a minimal number of nodes of a
square-ten tree. In other words, you must find a subsegment sequence as for every , , , where every square-ten tree levels and is minimal amongst all such variants.
Input Format
The first line contains a single integer denoting . The second line contains a single integer denoting .
Constraints
The numbers in input do not contain leading zeroes.
Output Format
As soon as array indices are too large, you should find a sequence of
, meaning that subsegment belongs to the level of the square-ten tree.
Print this sequence in the following compressed format:
On the first line, print the value of (i.e., the compressed sequence block count).
For each of the subsequent lines, print space-separated integers, and ( , ), meaning that the number appears consequently times in sequence . Blocks should be listed in the order they appear in the sequence. In other words, should be equal to ,
should be equal to , etc.
Thus must be true and must be true for every . All numbers should be printed without leading zeroes.
such belongs to any of the
square-ten tree level numbers,

 Sample Input 0
 1 10
Sample Output 0
1 11
Explanation 0
Segment belongs to level of the square-ten tree.
        
 */

import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;


public class HackerRankSquareTenTree {

	public static void main(String[] args) {
        /* Enter your code here. Read input from STDIN. Print output to STDOUT. Your class should be named Solution. */
    }
}
