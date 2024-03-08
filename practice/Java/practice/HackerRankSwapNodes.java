package com.cswg.practice;

import java.io.*;
import java.math.*;
import java.text.*;
import java.util.*;
import java.util.regex.*;

//import org.spark_project.guava.primitives.Ints;

/*
 *  Swap Nodes [Algo]
A binary tree is a tree which is characterized by any one of the following properties: It can be an empty (null).
It contains a root node and two subtrees, left subtree and right subtree. These subtrees are also binary tree.
Inorder traversal is performed as 1. Traverse the left subtree.
2. Visit root (print it).
3. Traverse the right subtree.
(For an Inorder traversal, start from the root and keep visiting the left subtree recursively until you reach the leaf,then you print the node at which you are and then you visit the right subtree.)
We define depth of a node as follow:
Root node is at depth 1.
If the depth of parent node is d , then the depth of current node wll be d+1 .
Swapping: Swapping subtrees of a node means that if initially node has left subtree L and right subtree R , then after swapping left subtree will be R and right subtree L .
              Eg. In the following tree, we swap children of node
Depth 1 1 [1]
/\ /\ 23->32 [2]
\\ \\
45 54 [3]
1 .
   Inordertraversaloflefttreeis 24135 andofrighttreeis 35124.
Swap operation: Given a tree and a integer, K , we have to swap the subtrees of all the nodes who are
atdepth h,where h∈[K,2K,3K,...].
You are given a tree of N nodes where nodes are indexed from [1..N] and it is rooted at 1 . You have to perform T swap operations on it, and after each swap operation print the inorder traversal of the current state of the tree.
Input Format
First line of input contains N , number of nodes in tree. Then N lines follow. Here each of ith line (1 <= i <= N) contains two integers, a b , where a is the index of left child, and b is the index of right child of ith node. -1 is used to represent null node.
Next line contain an integer, T . Then again T lines follows. Each of these line contains an integer K .
Output Format
For each K , perform swap operation as mentioned above and print the inorder traversal of the current state of tree.
Constraints
1 <= N <= 1024
1 <= T <= 100
1 <= K <= N
Either a = -1 or 2 <= a <= N
                 
 Either b = -1 or 2 <= b <= N
Index of (non-null) child will always be greater than that of parent.
Sample Input #00
3 23 -1 -1 -1 -1 2
1
1
Sample Output #00
312 213
Sample Input #01
5 23 -1 4 -1 5 -1 -1 -1 -1 1
2
Sample Output #01
42153
Sample Input #02
11 23
4 -1
5 -1
6 -1 78
-1 9 -1 -1 10 11 -1 -1 -1 -1 -1 -1 2
2 4
Sample Output #02
2 9 6 4 1 3 7 5 11 8 10 2 6 9 4 1 3 7 5 10 8 11
Explanation
** [s] represents swap operation is done at this depth.
Test Case #00: As node 2 and 3 has no child, swapping will not have any effect on it. We only have to
swap the child nodes of root node.
        1 [s] 1 [s] 1

  /\ ->/\ ->/\ 23[s] 32[s] 23
Test Case #01: Swapping child nodes of node 2 and 3 we get
11 /\ /\
2 3 [s] -> 2 3 \\ //
45 45
Test Case #02: Here we perform swap operations at the nodes whose depth is either 2 and 4 and then at nodes whose depth is 4.
111 /\/\/\
/\/\/\
2 3 [s] 2 3 2 3
//\\\\ //\\\\
4 5 -> 4 5 -> 4 5 //\ //\ //\
//\ //\ //\
6 7 8 [s] 6 7 8 [s] 6 7 8
\/\//\\/\ \/\//\\/\
9101191110 91011
  
 */

class NodeSN{
	 NodeSN left;
	 NodeSN right;
	 int depth;
	 int data;
	 
	 NodeSN(int data, int depth){
		 this.left = null;
		 this.right = null;
		 this.depth = depth;
		 this.data = data;
	 }
}


public class HackerRankSwapNodes {

	 /*public static NodeSN insertNode (NodeSN root, int data ){
	        if ( root == null ){
	        	return new NodeSN(data);
	        }else{
	        	NodeSN curr;
	            if ( data <= root.data ){
	            	curr = insertNode( root.left, data);
	            	root.left = curr;
	            }else{
	            	curr = insertNode( root.right, data);
	            	root.right = curr;
	            }
	            
	            return root;
	        }
	 }*/
			
	/*public static void insertNode( NodeSN root, int data ){
		Queue<NodeSN> nodeTraverse = new PriorityQueue<NodeSN>();
		nodeTraverse.add(root);
		nodeTraverse.add(null);
		int level = 1;
		while ( !nodeTraverse.isEmpty() ) {
			NodeSN curr = nodeTraverse.poll();	
			
			if ( curr != null ){
				if ( curr.left == null && curr.data != -1 ){
					//if ( data != -1){
						NodeSN newNode = new NodeSN(data);
					    curr.left = newNode;
					    curr.depth = level + 1;
					    return;
					//}
				}else if ( curr.right == null && curr.data != -1 ){
					//if ( data != -1){
						NodeSN newNode = new NodeSN(data);
						curr.right = newNode;
						curr.depth = level + 1;
						return;
					//}
				}else{
					if ( curr.left != null )
					  nodeTraverse.add(curr.left);
					if ( curr.right != null )
					  nodeTraverse.add(curr.right);
					
					if ( nodeTraverse.peek() == null ){
						level++;
						nodeTraverse.add(null);
					}

				}
			}
		}
    }*/
	 
	public static void swap( NodeSN node) {
		
		NodeSN temp = node.left;
		node.left = node.right;
		node.right = temp;
		temp = null;
	}
	
	public static Queue<NodeSN> levelOrderTraversal(NodeSN root, int level) {
		Queue<NodeSN> nodeTraverse = new LinkedList<NodeSN>();
		if ( root != null)
			nodeTraverse.add(root);

		int depth = 1;
		while( ! nodeTraverse.isEmpty() ){
			 
			depth = nodeTraverse.peek().depth;			
			if ( depth == level )
				return nodeTraverse;
			
			NodeSN curr = nodeTraverse.poll();
			if ( curr != null ){
				if( curr.left != null )
					nodeTraverse.add( curr.left );
				
				if ( curr.right != null )
					nodeTraverse.add( curr.right );
			}
		}
		
		return nodeTraverse;
		
	}
	 public static int treeHeight( NodeSN root ) {
		 if ( root == null ){
			return 0;
		 }else{
			 return 1 + Math.max( treeHeight(root.left), treeHeight(root.right) );
		 }
			
	 }
	
	 //static int index = 0;
	 //Both inOrderTraversal are working
	 //ArrayList<Integer>
	 public static void inOrderTraversal(ArrayList<Integer> returnArray, NodeSN root ) {
         
		 if ( root == null )
			 return;  //returnArray
					 
		 inOrderTraversal(returnArray, root.left);
		 //System.out.println( "Root Data - " + root.data + " :index - " + index);
		 returnArray.add( root.data );
		 //returnArray[index++] = root.data;
	     //System.out.println( "Return Array - " + returnArray[index] );
		 inOrderTraversal(returnArray, root.right);
		 
		 //return returnArray;
	 }
	 
     /*public static void inOrderTraversal(Queue<Integer> returnQueue, NodeSN root ) {
         
		 if ( root == null )
			 return;
					 
		 inOrderTraversal(returnQueue, root.left);
		 //System.out.println( "Root Data - " + root.data );
		 returnQueue.add(root.data);
		 inOrderTraversal(returnQueue, root.right);
	 }*/

	 public static Queue<NodeSN> nodeTraverse = new LinkedList<NodeSN>();
	 
	 public static void insertNodeAt( NodeSN root, int leftData, int rightData, int depth ){
		if ( root.left == null ){
			if( leftData != -1 ){
				NodeSN newNode = new NodeSN(leftData, depth + 1);
			    root.left = newNode;
			    nodeTraverse.add(newNode);
			}
		}

		if ( root.right == null ) {
			if( rightData != -1 ){
				NodeSN newNode = new NodeSN(rightData, depth + 1);
			    root.right = newNode;
			    nodeTraverse.add(newNode);
			}
		}
        //return root;	
	 }
	 /*
     * Complete the swapNodes function below.
     */
    static int[][] swapNodes(int[][] indexes, int[] queries) {
        /*
         * Write your code here.
         */
    	 NodeSN treeRoot = null;
    	 int level = 1;
    	 if ( indexes.length > 0 ){
    		 treeRoot = new NodeSN(1, 1);
    		 nodeTraverse.add(treeRoot);
    		 nodeTraverse.add(null);
    	 }
    	 for ( int treeNodes = 0; treeNodes < indexes.length ; treeNodes++ ){
    		 NodeSN currNode =  nodeTraverse.poll();
    		 
    		 if ( currNode == null ){
    			 nodeTraverse.add(null);
    			 level++;
    			 treeNodes--;
    		 }else{
    			 //System.out.println( "currNode - " + currNode.data + " -> " + indexes[treeNodes][0] + " " +  indexes[treeNodes][1]);
    			 insertNodeAt(currNode, indexes[treeNodes][0], indexes[treeNodes][1], level);
    			 
    		 }
    	 }
    	 
    	 int treeHeightVal =  treeHeight(treeRoot);  //indexes.length + 1; 
    	 //System.out.println( "treeHeightVal - " + treeHeightVal + " Array Length - " + Math.pow(2, treeHeightVal - 1 ) );
    	 
    	 System.out.println("Generated Tree");
    	 //Queue<Integer> returnQueue = new LinkedList<Integer>();
    	 ArrayList<Integer> returnQueue = new ArrayList<Integer> ();
    	 inOrderTraversal( returnQueue, treeRoot );
    	 //while( !returnQueue.isEmpty() ){
    	 for( int index = 0; index < returnQueue.size(); index++ ){
    		 System.out.println( returnQueue.get(index) );
    	 }
    	 System.out.println("\n");
    	 
    	 int[][] returnFinal = new int[queries.length][indexes.length];  //treeHeightVals
    	 for ( int queryIndex = 0; queryIndex < queries.length; queryIndex++ ){
    		 
    		//call swap function
    		 for( int noOfOper = 1; noOfOper < treeHeightVal; noOfOper++ ){
    			 int operationLevel = Math.round( noOfOper * queries[queryIndex] );
    			 //System.out.println( "operationLevel - " + operationLevel );
    			 if ( operationLevel <= treeHeightVal ){
    				 Queue<NodeSN> nodesAtQuery = levelOrderTraversal(treeRoot, operationLevel );
    			     for( NodeSN nodeAtDepth: nodesAtQuery){
    			    	 //System.out.println( nodeAtDepth.data );
    			    	 swap( nodeAtDepth );
    			     }
    				 
    			 }
    		 }

	    	 ArrayList<Integer> returnArrayList = new ArrayList<Integer>();
	    	 int[] returnArray = new int[indexes.length];
	    	 inOrderTraversal( returnArrayList, treeRoot);
	    	 for( int index=0; index < returnArrayList.size(); index++)
	    		 returnArray[index] = returnArrayList.get(index);
	    	 
	    	 returnFinal[queryIndex] = returnArray;
	    	 //Ints.toArray( returnArrayList );
    	 }

    	 return returnFinal;
    }

    //private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getenv("OUTPUT_PATH")));
        try {
			System.setIn(new FileInputStream("/Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankSwapNodes.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        Scanner scanner = new Scanner(System.in);
        int n = Integer.parseInt(scanner.nextLine().trim());

        int[][] indexes = new int[n][2];

        for (int indexesRowItr = 0; indexesRowItr < n; indexesRowItr++) {
            String[] indexesRowItems = scanner.nextLine().split(" ");

            for (int indexesColumnItr = 0; indexesColumnItr < 2; indexesColumnItr++) {
                int indexesItem = Integer.parseInt(indexesRowItems[indexesColumnItr].trim());
                indexes[indexesRowItr][indexesColumnItr] = indexesItem;
            }
        }

        int queriesCount = Integer.parseInt(scanner.nextLine().trim());

        int[] queries = new int[queriesCount];

        for (int queriesItr = 0; queriesItr < queriesCount; queriesItr++) {
            int queriesItem = Integer.parseInt(scanner.nextLine().trim());
            queries[queriesItr] = queriesItem;
        }

        int[][] result = swapNodes(indexes, queries);

        for (int resultRowItr = 0; resultRowItr < result.length; resultRowItr++) {
            for (int resultColumnItr = 0; resultColumnItr < result[resultRowItr].length; resultColumnItr++) {
                //bufferedWriter.write(String.valueOf(result[resultRowItr][resultColumnItr]));
                System.out.println(String.valueOf(result[resultRowItr][resultColumnItr]));
                if (resultColumnItr != result[resultRowItr].length - 1) {
                    //bufferedWriter.write(" ");
                    System.out.println( " ");
                }
            }

            if (resultRowItr != result.length - 1) {
                //bufferedWriter.write("\n");
                System.out.println("\n");
            }
        }

        //bufferedWriter.newLine();
        //bufferedWriter.close();
        
        scanner.close();
    }
}
