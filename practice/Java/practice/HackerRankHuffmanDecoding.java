package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/*
 *  Tree: Huffman
Decoding
Huffman coding assigns variable length codewords to fixed length input characters based on their frequencies. More frequent characters are assigned shorter codewords and less frequent characters are assigned longer codewords. All edges along the path to a character contain a code digit. If they are on the left side of the tree, they will be a 0 (zero). If on the right, they'll be a 1 (one). Only the leaves will contain a letter and its frequency count. All other nodes will contain a null instead of a character, and the count of the frequency of all of it and its descendant characters.
For instance, consider the string ABRACADABRA. There are a total of characters in the string. This number should match the count in the ultimately determined root of the tree. Our frequencies are
and . The two smallest frequencies are for and , both equal to , so we'll create a tree with them. The root node will contain the sum of the counts of its descendants, in
this case . The left node will be the first character encountered, , and the right will contain . Next we have items with a character count of : the tree we just created, the character and the character . The tree came first, so it will go on the left of our new root node. will go on the right. Repeat until the tree is complete, then fill in the 's and 's for the edges. The finished graph looks like:
                                         Input characters are only present in the leaves. Internal nodes have a character value of φ (NULL). We can determine that our values for characters are:
A-0
B - 111 C - 1100 D - 1101 R - 10
Our Huffman encoded string is:
ABRACADABRA
0 111 10 0 1100 0 1101 0 111 10 0 or
01111001100011010111100
To avoid ambiguity, Huffman encoding is a prefix free encoding technique. No codeword appears as a prefix of any other codeword.
To decode the encoded string, follow the zeros and ones to a leaf and return the character there.
You are given pointer to the root of the Huffman tree and a binary coded string to decode. You need to print the decoded string.
  
 Function Description
Complete the function decode_huff in the editor below. It must return the decoded string. decode_huff has the following parameters:
root: a reference to the root node of the Huffman tree s: a Huffman encoded string
Input Format
There is one line of input containing the plain string, . Background code creates the Huffman tree then passes the head node and the encoded string to the function.
Constraints
Output Format
Output the decoded string on a single line.
Sample Input
s="1001011"
Sample Output
ABACA
Explanation
S="1001011"
Processing the string from left to right.
S[0]='1' : we move to the right child of the root. We encounter a leaf node with value 'A'. We add 'A' to the decoded string. We move back to the root.
S[1]='0' : we move to the left child.
S[2]='0' : we move to the left child. We encounter a leaf node with value 'B'. We add 'B' to the decoded string. We move back to the root.
S[3] = '1' : we move to the right child of the root. We encounter a leaf node with value 'A'. We add 'A' to the decoded string. We move back to the root.
S[4]='0' : we move to the left child.
S[5]='1' : we move to the right child. We encounter a leaf node with value C'. We add 'C' to the decoded string. We move back to the root.
S[6] = '1' : we move to the right child of the root. We encounter a leaf node with value 'A'. We add 'A' to the decoded string. We move back to the root.
               
  Decoded String = "ABACA"

 */

import java.util.*;

abstract class NodeHD implements Comparable<NodeHD> {
    
  	public  int frequency; // the frequency of this tree
    public  char data;
    public  NodeHD left, right; 
    public NodeHD(int freq) { 
    	frequency = freq;
   	}
 
    // compares on the frequency
    public int compareTo(NodeHD tree) {
        return frequency - tree.frequency;
    }
  
}
 
class HuffmanLeaf extends NodeHD {
    
    public HuffmanLeaf(int freq, char val) {
        super(freq);
        data = val;
    }
  
}
 
class HuffmanNode extends NodeHD {
    
    public HuffmanNode(NodeHD l, NodeHD r) {
        super(l.frequency + r.frequency);
        left = l;
        right = r;
    }

}

class Decoding {

/*  
	class NodeHD
		public  int frequency; // the frequency of this tree
    	public  char data;
    	public  NodeHD left, right;
    
*/
	PriorityQueue<NodeHD> nodeTraverse = new PriorityQueue<NodeHD>();
	
	public void traverse(char frequency) {  //NodeHD
		NodeHD topNode = nodeTraverse.peek();
		
		if ( topNode == null )
			return;  // null
		
		if ( frequency == '0' ) {
			if ( topNode.left != null ) {
				nodeTraverse.add(topNode.left);
				//return topNode.left;
			}
		}else { 
			if ( topNode.right != null ) {
				nodeTraverse.add(topNode.right);
				//return topNode.right;
			}
		}
	}
	
	public char checkData() {
		NodeHD topNode = nodeTraverse.peek();
		//System.out.println( (int) topNode.data + " " + topNode.frequency );
		//if( topNode != null )
			//if ( topNode.left == null && topNode.right == null )
	    //if ( (int)topNode.data != 0 )
	    return topNode.data;
		
		//return '0';
	}
	
    void decode(String s, NodeHD root) {

    	nodeTraverse.add( root );
    	for( char c: s.toCharArray() ) {
    		//System.out.print( "c - " + c + "\n");
    		traverse( c );
    		char value = checkData();
    		//System.out.print( "value - " + value + "\n");
    		if ( (int)value != 0 ) {   //( value != ' ') This does not work for char
    			System.out.print( value  );
    			nodeTraverse.clear();
    			nodeTraverse.add(root);
    		}/*else {
    			System.out.print( "null value" + "\n");
    		}*/
    	}

    }

}

public class HackerRankHuffmanDecoding {
  	// input is an array of frequencies, indexed by character code
    
  	public static NodeHD buildTree(int[] charFreqs) {
      
        PriorityQueue<NodeHD> trees = new PriorityQueue<NodeHD>();
        // initially, we have a forest of leaves
        // one for each non-empty character
        for (int i = 0; i < charFreqs.length; i++)
            if (charFreqs[i] > 0)
                trees.offer(new HuffmanLeaf(charFreqs[i], (char)i));
 
        assert trees.size() > 0;
        // loop until there is only one tree left
        while (trees.size() > 1) {
            // two trees with least frequency
            NodeHD a = trees.poll();
            NodeHD b = trees.poll();
 
            // put into new node and re-insert into queue
            trees.offer(new HuffmanNode(a, b));
        }
        return trees.poll();
    }
  
    public static Map<Character,String> mapA=new HashMap<Character ,String>();
  
    public static void printCodes(NodeHD tree, StringBuffer prefix) {
        assert tree != null;
      
        if (tree instanceof HuffmanLeaf) {
            HuffmanLeaf leaf = (HuffmanLeaf)tree;
 
            // print out character, frequency, and code for this leaf (which is just the prefix)
            //System.out.println(leaf.data + "\t" + leaf.frequency + "\t" + prefix);
            mapA.put(leaf.data,prefix.toString());

        } else if (tree instanceof HuffmanNode) {
            HuffmanNode node = (HuffmanNode)tree;
 
            // traverse left
            prefix.append('0');
            printCodes(node.left, prefix);
            prefix.deleteCharAt(prefix.length()-1);
 
            // traverse right
            prefix.append('1');
            printCodes(node.right, prefix);
            prefix.deleteCharAt(prefix.length()-1);
        }
    }
 
    public static void inOrder( NodeHD root ) {
		 if ( root == null )
			 return;
		 
		 inOrder( root.left );
		 System.out.print( root.data );  //+ " " 
		 inOrder( root.right );
	}
    
    @SuppressWarnings("resource")
	public static void main(String[] args) {
    	try {
			System.setIn(new FileInputStream("C:/Pushkar/STS39Workspace/GeneralLearning/HackerRankHuffmanDecoding.txt"));
			///Users/goyalpushkar/Documents/STSworkspace/GeneralLearning/HackerRankLowestCommonAncestor.txt
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner input = new Scanner(System.in);
        String test= input.next();
        
        // we will assume that all our characters will have
        // code less than 256, for simplicity
        int[] charFreqs = new int[256];
      
        // read each character and record the frequencies
        for (char c : test.toCharArray())
            charFreqs[c]++;
 
        // build tree
        NodeHD tree = buildTree(charFreqs);
 
        // print out results
        printCodes(tree, new StringBuffer());
        StringBuffer s = new StringBuffer();
       
        for(int i = 0; i < test.length(); i++) {
          
          char c = test.charAt(i);
          s.append(mapA.get(c));
          
        }
        System.out.println( "In Order");
        inOrder( tree );
        System.out.print( "\n" );
        System.out.println(s);
        Decoding d = new Decoding();
        d.decode(s.toString(),tree);
        input.close();
    }
}
