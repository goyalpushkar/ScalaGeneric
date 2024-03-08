package com.cswg.practice;

import java.util.*;
import java.io.*;


/*
 *  Tree: Height of a
Binary Tree
The height of a binary tree is the number of edges between the tree's root and its furthest leaf. For example, the following binary tree is of height :
Function Description
Complete the getHeight or height function in the editor. It must return the height of a binary tree as an integer.
getHeight or height has the following parameter(s): root: a reference to the root of a binary tree.
Note -The Height of binary tree with single node is taken as zero.
Input Format
The first line contains an integer , the number of nodes in the tree.
Next line contains space separated integer where th integer denotes node[i].data.
Note: Node values are inserted into a binary search tree before a reference to the tree's root node is passed to your function. In a binary search tree, all nodes on the left branch of a node are less than the node value. All values on the right branch are greater than the node value.
Constraints
Output Format
Your function should return a single integer denoting the height of the binary tree.
Sample Input
                                
  Sample Output
3
Explanation
The longest root-to-leaf path is shown below:
There are nodes in this path that are connected by edges, meaning our binary tree's .
            
 */
class NodeTH {
    NodeTH left;
    NodeTH right;
    int data;
    
    NodeTH(int data) {
        this.data = data;
        left = null;
        right = null;
    }
}

public class HackerRankTreeHeight {
	/*
    class NodeTH 
    	int data;
    	NodeTH left;
    	NodeTH right;
	*/
	
	public static int height(NodeTH root) {
      	// Write your code here.
		if ( root == null )
			return 0;
		else return 1 + Math.max( height(root.left), height( root.right ) );
    }

	public static NodeTH insert(NodeTH root, int data) {
        if(root == null) {
            return new NodeTH(data);
        } else {
            NodeTH cur;
            if(data <= root.data) {
                cur = insert(root.left, data);
                root.left = cur;
            } else {
                cur = insert(root.right, data);
                root.right = cur;
            }
            return root;
        }
    }

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankTreeHeight.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int t = scan.nextInt();
        NodeTH root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        scan.close();
        int height = height(root);
        System.out.println(height);
    }	
}


