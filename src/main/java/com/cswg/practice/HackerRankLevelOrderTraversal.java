package com.cswg.practice;

/*
 *  Tree: Level Order
Traversal
You are given a pointer to the root of a binary tree. You need to print the level order traversal of this tree. In level order traversal, we visit the NodeLTs level by level from left to right. You only have to complete the function. For example:
1 \
2 \
5 /\
36 \
4
For the above tree, the level order traversal is 1 -> 2 -> 5 -> 3 -> 6 -> 4.
Input Format
You are given a function,
void levelOrder(node * root) { }
Constraints
1 Nodes in the tree 500
Output Format
Print the values in a single line separated by a space.
Sample Input
1 \
2 \
5 /\
36 \
4
Sample Output
125364
Explanation
We need to print the nodes level by level. We process each level from left to right. Level Order Traversal: 1 -> 2 -> 5 -> 3 -> 6 -> 4.
        
 */

import java.util.*;
import java.io.*;

class NodeLT {
    NodeLT left;
    NodeLT right;
    int data;
    
    NodeLT(int data) {
        this.data = data;
        left = null;
        right = null;
    }
}

public class HackerRankLevelOrderTraversal {

	/* 
    
    class NodeLT 
    	int data;
    	NodeLT left;
    	NodeLT right;
	*/
	
	public static void levelOrder(NodeLT root) {
		Queue<NodeLT> queueTraversal = new LinkedList<NodeLT>();
		queueTraversal.add(root);
		
		while( !(queueTraversal.isEmpty()) ){
			NodeLT curr = queueTraversal.poll();
			System.out.print( curr.data + " " );
			if ( curr.left != null )
				queueTraversal.add(curr.left);
			if ( curr.right != null )
				queueTraversal.add(curr.right);
		}
		
    }

	public static NodeLT insert(NodeLT root, int data) {
        if(root == null) {
            return new NodeLT(data);
        } else {
            NodeLT cur;
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
    	try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankLevelOrderTraversal.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scan = new Scanner(System.in);
        int t = scan.nextInt();
        NodeLT root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        scan.close();
        levelOrder(root);
    }	
}

