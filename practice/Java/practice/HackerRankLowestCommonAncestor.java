package com.cswg.practice;

/*
 *  Binary Search Tree : Lowest Common Ancestor
You are given pointer to the root of the binary search tree and two values and the lowest common ancestor (LCA) of and in the binary search tree.
In the diagram above, the lowest common ancestor of the nodes and is the node lowest node which has nodes and as descendants.
Function Description
. You need to return
            . Node is the
      Complete the function lca in the editor below. It should return a pointer to the lowest common ancestor node of the two values given.
lca has the following parameters:
- root: a pointer to the root node of a binary search tree - v1: a node.data value
- v2: a node.data value
Input Format
The first line contains an integer, , the number of nodes in the tree.
The second line contains space-separated integers representing values. The third line contains two space-separated integers, and .
To use the test data, you will have to create the binary search tree yourself. Here on the platform, the tree will be created for you.
Constraints
The tree will contain nodes with data equal to and .
Output Format
Return the a pointer to the node that is the lowest common ancestor of
                                                  and .
    Sample Input

  and .
Sample Output
[reference to node 4]
Explanation
LCA of and is , the root in this case. Return a pointer to the node.
           
 */
import java.util.*;
import java.io.*;

class NodeLCA {
    NodeLCA left;
    NodeLCA right;
    int data;
    
    NodeLCA(int data) {
        this.data = data;
        left = null;
        right = null;
    }
}

public class HackerRankLowestCommonAncestor {
	/*
    class NodeLCA 
    	int data;
    	NodeLCA left;
    	NodeLCA right;
	*/
	public static NodeLCA lca(NodeLCA root, int v1, int v2) {
      	// Write your code here.
		
		if ( root == null )
			return root;
		
		System.out.println("root - " + root.data + " :v1 - " + v1 + " :V2 - " + v2);
		if ( root.data > v1 && root.data > v2 )
		   return lca( root.left, v1, v2);
	    
		if ( root.data < v1 && root.data < v2 )
		   return lca( root.right, v1, v2);
				
		return root;
    }

	public static void inOrder( NodeLCA root ) {
		 if ( root == null )
			 return;
		 
		 inOrder( root.left );
		 System.out.print( root.data + " " );
		 inOrder( root.right );
	}
	
	public static NodeLCA insert(NodeLCA root, int data) {
        if(root == null) {
            return new NodeLCA(data);
        } else {
            NodeLCA cur;
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
			System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankLowestCommonAncestor.txt"));
			///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankLowestCommonAncestor.txt
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scan = new Scanner(System.in);
        int t = scan.nextInt();
        NodeLCA root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        int v1 = scan.nextInt();
      	int v2 = scan.nextInt();
        scan.close();
        System.out.println("In Order");
        inOrder( root);
        System.out.println("\n"+ "LCA");
        NodeLCA ans = lca(root,v1,v2);
        System.out.println(ans.data);
    }	
}
