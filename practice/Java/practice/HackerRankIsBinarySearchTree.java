package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Stack;

/*
 * Is This a Binary
Search Tree?
For the purposes of this challenge, we define a binary tree to be a binary search tree with the following
ordering requirements:
The value of every node in a node's left subtree is less than the data value of that node.
The value of every node in a node's right subtree is greater than the data value of that node.
Given the root node of a binary tree, can you determine if it's also a binary search tree?
Complete the function in your editor below, which has parameter: a pointer to the root of a binary tree.
It must return a boolean denoting whether or not the binary tree is a binary search tree. You may have to
write one or more helper functions to complete this challenge.
Input Format
You are not responsible for reading any input from stdin. Hidden code stubs will assemble a binary tree
and pass its root node to your function as an argument.
Constraints
Output Format
You are not responsible for printing any output to stdout. Your function must return true if the tree is a
binary search tree; otherwise, it must return false. Hidden code stubs will print this result as a Yes or No
answer on a new line.
Sample Input
Sample Output
No
 */

class NodeBST {
    NodeBST left;
    NodeBST right;
    int data;
    
    NodeBST(int data) {
        this.data = data;
        left = null;
        right = null;
    }
}

public class HackerRankIsBinarySearchTree {

	/*public static boolean check( NodeBST root ) {
		if ( root == null ) 
	            return true;
	    else if( root.left != null )
			if ( root.left.data < root.data ) {
				if ( root.right != null ) {
					if ( root.right.data > root.data )
						return true;
					else return false;
				}else {
					return true;
				}
			}
			else return false;
		else if ( root.right != null )
			if ( root.right.data > root.data )
				return true;
			else return false;
	    else
	    	return true;
	}*/
	
	public static int numOfNodes( NodeBST root) {
		if ( root == null )
			return 0;
		else return 1 + numOfNodes( root.left ) + numOfNodes( root.right );
	}
	
	//With array it doesnot work
	/*public static void inOrder( NodeBST root, int[] nodeArr ) {
		
	    int i = 0;
		if ( root == null )
			return;
		
		inOrder( root.left, nodeArr );
        nodeArr[i++] = root.data;
        inOrder( root.right, nodeArr );
	}*/
	
	public static void inOrder( NodeBST root, ArrayList<Integer> nodeArr ) {
			
		if ( root == null )
			return;
		
		inOrder( root.left, nodeArr );
		nodeArr.add( root.data);
        inOrder( root.right, nodeArr );

	}
	
    public static boolean checkBST(NodeBST root) {
    	int noOfNodes = numOfNodes(root);
    	
    	//int[] nodeDataArrInt = new int[noOfNodes];
        //inOrder(root, nodeDataArrInt);
    	
    	ArrayList<Integer> nodeDataArr = new ArrayList<Integer>();
        inOrder(root, nodeDataArr);
        
        for( int index = 0; index < noOfNodes - 1 ; index++ ) {
            if ( nodeDataArr.get(index) >= nodeDataArr.get(index + 1) )
                return false;
        }
        
        return true;
    }
    
	public static NodeBST insert(NodeBST root, int data) {
        if(root == null) {
            return new NodeBST(data);
        } else {
            NodeBST cur;
            if(data < root.data) {
                cur = insert(root.left, data);
                root.left = cur;
            } else {
                cur = insert(root.right, data);
                root.right = cur;
            }
            return root;
        }
    }

	public static void inOrderR( NodeBST root ) {
		 if ( root == null )
			 return;
		 
		 inOrderR( root.left );
		 System.out.print( root.data + " " );  //+ " " 
		 inOrderR( root.right );
	}
	
    public static void main(String[] args) {
    	try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankIsBST.txt"));
					// "C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankIsBST.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scan = new Scanner(System.in);
        int t = scan.nextInt();
        NodeBST root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        scan.close();
        System.out.println("In Order");
        inOrderR(root);
        System.out.println( "\n" + "Check BST");
        Boolean bstYN = checkBST(root);
        System.out.println("bstYN - " + bstYN);
    }
}
