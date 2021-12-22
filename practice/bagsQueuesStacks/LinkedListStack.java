package com.practice.bagsQueuesStacks;

import java.util.Iterator;

import com.practice.basic.StdIn;
import com.practice.basic.StdOut;

/*
 * ALGORITHM 1.2 Pushdown stack (linked-list implementation)
 * This generic Stack implementation is based on a linked-list data structure. It can be used to create
   stacks containing any type of data.
    
   This use of linked lists achieves our optimum design goals:
	■ It can be used for any type of data.
	■ The space required is always proportional to the size of the collection.
	■ The time per operation is always independent of the size of the collection.
	This implementation is a prototype for many algorithm implementations that we consider. 
 */
public class LinkedListStack<Item> implements Iterable<Item>
{
	 private Node first; // top of stack (most recently added node)
	 private int N;      // number of items
	 
	 private class Node
	 {  // nested class to define nodes
		 Item item;
		 Node next; 
	 }
     
	 public boolean isEmpty() {  return first == null; }  // Or: N == 0.
     public int size()        {  return N; }
     
     public void push(Item item)
     {  // Add item to top of stack.
        Node oldfirst = first;
        first = new Node();
        first.item = item;
        first.next = oldfirst;
        N++;
	 }
     
     public Item pop()
     {  // Remove item from top of stack.
        Item item = first.item;
        first = first.next;
        N--;
        return item;
	}
     
	@Override
	public Iterator<Item> iterator() {
		return new linkedListIterator();
	}
	    
	public class linkedListIterator implements Iterator<Item>{

		private Node current = first;

		public boolean hasNext() {
			return current != null;  
		}

		public void remove() { }
		
		public Item next() {
			Item item = current.item;
			current = current.next;
			return item;
		}
		
	}
     
	public static void main(String[] args)
	{  // Create a stack and push/pop strings as directed on StdIn.
		LinkedListStack<String> s = new LinkedListStack<String>();
	     while (!StdIn.isEmpty())
	     {
	        String item = StdIn.readString();
	        if (!item.equals("-"))
	             s.push(item);
	        else if (!s.isEmpty()) StdOut.print(s.pop() + " ");
	     }
	     StdOut.println("(" + s.size() + " left on stack)");
    }

}

