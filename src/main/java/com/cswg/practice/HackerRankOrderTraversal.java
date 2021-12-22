package com.cswg.practice;

import java.util.*;
import java.io.*;

class Node {
    Node left;
    Node right;
    int data;
    
    Node(int data) {
        this.data = data;
        left = null;
        right = null;
    }
}

public class HackerRankOrderTraversal {

	/* you only have to complete the function given below.  
	Node is defined as  

	class Node {
	    int data;
	    Node left;
	    Node right;
	}

	*/

    public static void preOrder(Node root) {
    	if ( root == null )
    		return;
    	
    	System.out.print( root.data + " " );
    	preOrder( root.left);
    	preOrder( root.right);
    }
    
    public static void preOrderWOR(Node root) {
    	Stack<Node> stackNodes = new Stack<Node>();
    	if ( root == null )
    		return;
    	else
    		stackNodes.push( root );
    	
    	while( !stackNodes.isEmpty() ){
    		Node curr = stackNodes.pop();
    		System.out.print( curr.data + " " );
    		
    		if ( curr.right != null )
    			stackNodes.push( curr.right );
    		
    		if ( curr.left != null )
    			stackNodes.push( curr.left );
    	}
    }
    
    public static void postOrder(Node root) {
    	if ( root == null )
    		return;
    	
    	postOrder( root.left);
    	postOrder( root.right);    	
    	System.out.print( root.data + " " );

    }
    
    public static void postOrderWOR(Node root) {
    	Stack<Node> stackNodes = new Stack<Node>();
    	Node curr = root;
    	Node lastVisitedNode = null;
    	while( !stackNodes.isEmpty() || curr != null ){
    		if ( curr != null ){
    			stackNodes.push( curr );
    		    curr = curr.left;
    		}else{
    		    Node topNode = stackNodes.peek();
    		    if ( topNode.right != null && lastVisitedNode != topNode.right )
    		    	curr = topNode.right;
    		    else{
    		    	System.out.print( topNode.data + " ");
    		    	lastVisitedNode = stackNodes.pop();
    		    }
    		}
    	}
    }
    
    public static void inOrder(Node root) {
    	if ( root == null )
    		return;
    	
    	inOrder( root.left);
    	System.out.print( root.data + " " );
    	inOrder( root.right);    	
    	
    }

    public static void inOrderWOR(Node root) {
    	Stack<Node> stackNodes = new Stack<Node>();
    	Node curr = root;
    	while( !stackNodes.isEmpty() || curr != null ){
    		if ( curr != null ){
    			stackNodes.push( curr );
    		    curr = curr.left;
    		}else{
    			curr = stackNodes.pop();
    			System.out.print( curr.data + " ");
    			curr = curr.right;
    		}
    	}
    }
    
    public static void levelOrder(Node root) {
		Queue<Node> queueTraversal = new LinkedList<Node>();
		queueTraversal.add(root);
		
		while( !(queueTraversal.isEmpty()) ){
			Node curr = queueTraversal.poll();
			System.out.print( curr.data + " " );
			if ( curr.left != null )
				queueTraversal.add(curr.left);
			if ( curr.right != null )
				queueTraversal.add(curr.right);
		}
		
    }
    
    public static class nodeDistance{
    	Node node;
    	int distance;
    	
    	nodeDistance(Node n, int d){
    		this.node = n;
    		this.distance = d;
    	}
    	
    }
    
    public static void verticalOrder(Node root) {
		Queue<nodeDistance> queueTraversal = new LinkedList<nodeDistance>();
		Map<Integer, List<Node>> verticalDist = new HashMap<Integer, List<Node>>();
		queueTraversal.add( new nodeDistance(root, 0) );
		
		while( !(queueTraversal.isEmpty()) ){
			Node curr = null;
			int currDistance = 0;
			nodeDistance currDist = queueTraversal.poll();
			if ( currDist != null ){
		  	    curr = currDist.node;
		        currDistance = currDist.distance;
			}
			//System.out.println( " :currDistance - " + currDistance);
			
			List<Node> newList;
			if ( verticalDist.containsKey(currDistance) ){
			    newList = verticalDist.get(currDistance);
			}else{
			    newList = new ArrayList<Node>();
			}
			newList.add(curr);
			verticalDist.put(currDistance, newList);
	
			if ( curr.left != null )
				queueTraversal.add( new nodeDistance(curr.left, currDistance - 1) );
			if ( curr.right != null )
				queueTraversal.add( new nodeDistance(curr.right, currDistance + 1) );
		}
		
		
		for( Integer elem: verticalDist.keySet() ){
			System.out.print( "Distance - " + elem );
			for( Node distNodes: verticalDist.get(elem) ){
				System.out.print( " -> " + distNodes.data + " " );
			}
			System.out.print( "\n");
		}
		
    }
    
	public static Node insert(Node root, int data) {
        if(root == null) {
            return new Node(data);
        } else {
            Node cur;
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
    	System.out.println("inside main");
        try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankOrderTraversal.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scan = new Scanner(System.in);
        int t = scan.nextInt();
        System.out.println("t -" + t);
        Node root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        scan.close();
        System.out.println("preOrder");
        preOrder(root);
        System.out.println("\n"+ "preOrderWOR");
        preOrderWOR(root);
        System.out.println("\n"+ "postOrder");
        postOrder(root);
        System.out.println("\n"+ "postOrderWOR");
        postOrderWOR(root);
        System.out.println("\n"+ "inOrder");
        inOrder(root);
        System.out.println("\n"+ "inOrderWOR");
        inOrderWOR(root);
        System.out.println("\n"+ "levelOrder");
        levelOrder(root);
        System.out.println("\n"+ "verticalOrder");
        verticalOrder(root);
    }	
}
