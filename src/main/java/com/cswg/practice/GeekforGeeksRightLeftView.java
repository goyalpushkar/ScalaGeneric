package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

class NodeLRV { 
	  
    int data; 
    NodeLRV left, right; 
  
    NodeLRV(int item) { 
        data = item; 
        left = right = null; 
    } 
}

public class GeekforGeeksRightLeftView {

	public static void rightView( NodeLRV root ) {
		Queue<NodeLRV> queueNodes = new LinkedList<NodeLRV>();
		
		//int level = 1;
		if( root != null ){
			queueNodes.add( root );
	   	    queueNodes.add(null);
		}
		
		while( !queueNodes.isEmpty() ){
			NodeLRV currNode = queueNodes.poll();
			
			if ( currNode == null ){
				if( queueNodes.peek() == null ){
					return;
				}else{
					queueNodes.add(null);
				}
			}else{
				if( queueNodes.peek() == null ){
					System.out.print(currNode.data + " " );
				}
				
				if ( currNode.left != null )
					queueNodes.add(currNode.left);
				
				if ( currNode.right != null )
					queueNodes.add(currNode.right);
			}
		}
	}
	
	static int maxLevel = 0;
	public static void rightViewRec( NodeLRV root, int level ) {
		if ( root == null ){
			return;
		}else{			
				
			if ( level > maxLevel ){
				System.out.print( root.data + " " );
				maxLevel = level;
			}
			if ( root.right != null )
			   rightViewRec( root.right, level + 1);
			if ( root.left != null )
			   rightViewRec( root.left, level + 1);
	  }
		
	}
	
    public static void leftView( NodeLRV root ) {
		Queue<NodeLRV> queueNodes = new LinkedList<NodeLRV>();
		
		//int level = 1;
		if( root != null ){
			queueNodes.add( root );
	   	    queueNodes.add(null);
		}
		
		while( !queueNodes.isEmpty() ){
			NodeLRV currNode = queueNodes.poll();
			
			if ( currNode == null ){
				if( queueNodes.peek() == null ){
					return;
				}else{
					queueNodes.add(null);
				}
			}else{
				//System.out.println( "currNode - " + currNode );
				if( queueNodes.peek() == null ){
					System.out.print(currNode.data + " " );
				}
				
				if ( currNode.right != null )
					queueNodes.add(currNode.right);
				
				if ( currNode.left != null )
					queueNodes.add(currNode.left);
			}
		}
	}

    public static void leftViewRec( NodeLRV root, int level ) {
		if ( root == null ){
			return;
		}else{			
				
			if ( level > maxLevel ){
				System.out.print( root.data + " " );
				maxLevel = level;
			}
			
			if ( root.left != null )
			   leftViewRec( root.left, level + 1);
			if ( root.right != null )
			   leftViewRec( root.right, level + 1);
	  }
		
	}
    
	public static NodeLRV insert(NodeLRV root, int data) {
        if(root == null) {
            return new NodeLRV(data);
        } else {
            NodeLRV cur;
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
	
	// Driver program to test the above functions 
	public static void main(String[] args) {
    	try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankTopView.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scan = new Scanner(System.in);
        int t = scan.nextInt();
        NodeLRV root = null;
        while(t-- > 0) {
            int data = scan.nextInt();
            root = insert(root, data);
        }
        scan.close();
        System.out.println("Right View");
        rightView(root);
        System.out.println("\n");
        System.out.println("Left View");
        leftView(root);
        System.out.println("\n");
        System.out.println("Right View Recursive");
        maxLevel = 0;
        rightViewRec(root, 1);
        System.out.println("\n");
        System.out.println("Left View Recursive");
        maxLevel = 0;
        leftViewRec(root, 1);
    }	
	
}
