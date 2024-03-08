package com.cswg.practice;

/*
 *  Tree : Top View
You are given a pointer to the root of a binary tree. Print the top view of the binary tree.
Top view means when you look the tree from the top the nodes you will see will be called the top view of the tree. See the example below.
You only have to complete the function.
For example :
1 \
2 \
5 /\
36 \
4
Top View : 1 -> 2 -> 5 -> 6
Input Format
You are given a function,
void topView(node * root) { }
Constraints
1 Nodes in the tree 500
Output Format
Print the values on a single line separated by space.
Sample Input
1 \
2 \
5 /\
36 \
4
Sample Output
1256
Explanation
1 \
2 \
5 /\
36 \
         4

 From the top only nodes 1,2,5,6 will be visible.

 */
import java.util.*;
import java.io.*;

class NodeTV {
    NodeTV left;
    NodeTV right;
    int data;
    
    NodeTV(int data) {
        this.data = data;
        left = null;
        right = null;
    }
}

public class HackerRankTopViewTree {
	
	static class nodeDistance{
		NodeTV node;
		int distance;
		
		nodeDistance(NodeTV n, int d){
			this.node = n;
			this.distance = d;
		}
		
	}
	
	public static void preOrder(NodeTV root) {
    	if ( root == null )
    		return;
    	
    	System.out.print( root.data + " " );
    	preOrder( root.left);
    	preOrder( root.right);
    }
	
	public static void topView(NodeTV root) {
	      Queue<nodeDistance> nodeQueue = new LinkedList<nodeDistance>();
	      Map<Integer, NodeTV> topMap = new TreeMap<Integer, NodeTV>();
	      
	      if ( root != null ){
	    	  nodeQueue.add( new nodeDistance(root, 0) );
	      }
	      
	      while( !nodeQueue.isEmpty() ){
	    	  nodeDistance currNodeDistance = nodeQueue.poll();
	    	  NodeTV currNode = null;
	    	  int currDistance = 0;
	    	  if ( currNodeDistance != null){
	    		  currNode = currNodeDistance.node;
	    		  currDistance = currNodeDistance.distance;		  
	    	  }
	    	  
	    	  if ( ! topMap.containsKey(currDistance) ){
	    		  //System.out.print( "currDistance - " + currDistance + " -> " + currNode.data + "\n");
	    		  topMap.put( currDistance, currNode);
	    	  }
	    	  
	    	  if ( currNode.left != null )
	    		  nodeQueue.add( new nodeDistance(currNode.left, currDistance - 1 ) );
	    	  
	    	  if ( currNode.right != null )
	    		  nodeQueue.add( new nodeDistance(currNode.right, currDistance + 1 ) );
	    	  	    	  
	      }
	      
	      for( Integer key: topMap.keySet() ){
	    	  System.out.print( topMap.get(key).data + " " ); 
	      }
    }

	public static NodeTV insert(NodeTV root, int data) {
        if(root == null) {
            return new NodeTV(data);
        } else {
            NodeTV cur;
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
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankTopView.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scan = new Scanner(System.in);
        int t = scan.nextInt();
        NodeTV root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        scan.close();
        preOrder(root);
        System.out.println("\n");
        topView(root);
    }	
}
