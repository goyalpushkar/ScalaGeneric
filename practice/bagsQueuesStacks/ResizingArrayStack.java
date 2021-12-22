package com.practice.bagsQueuesStacks;

import java.util.Iterator;

/*
 * ALGORITHM 1.1 Pushdown (LIFO) stack (resizing array implementation)
 * This generic, iterable implementation of our Stack API is a model for collection ADTs that keep
   items in an array. It resizes the array to keep the array size within a constant factor of the 
   stack size.
   
   In the context of the study of algorithms, This algorithm (Algorithm 1.1) is significant because 
   it almost (but not quite) achieves optimum performance goals for any collection implementation:
	■ Each operation should require time independent of the collection size.
	■ The space used should always be within a constant factor of the collection size. 
	The flaw in ResizingArrayStack is that some push and pop operations require resiz- ing: this takes
	 time proportional to the size of the stack. Next, we consider a way to correct this flaw, using 
	 a fundamentally different way to structure data.
 */
public class ResizingArrayStack<Item> implements Iterable<Item>
{
    @SuppressWarnings("unchecked")
	private Item[] a = (Item[]) new Object[1];  // stack items
    private int N = 0;                          // number of items
    
    public boolean isEmpty()  {  return N == 0; }
    public int size()         {  return N;      }
    
    private void resize(int max)
    {  // Move stack to a new array of size max.
       Item[] temp = (Item[]) new Object[max];
       for (int i = 0; i < N; i++)
          temp[i] = a[i];
       a = temp;
    }
    
    public void push(Item item)
    {  // Add item to top of stack.
       if (N == a.length) resize(2*a.length);
       a[N++] = item;
    }
    
    public Item pop()
    {  // Remove item from top of stack.
       Item item = a[--N];
       a[N] = null;  // Avoid loitering (see text).
       if (N > 0 && N == a.length/4) resize(a.length/2);
       return item;
    }
    
    public Iterator<Item> iterator()
     {  return new ReverseArrayIterator();  }
    
    private class ReverseArrayIterator implements Iterator<Item>
    {  // Support LIFO iteration.
       private int i = N;
       public boolean hasNext() {  return i > 0;   }
       public    Item next()    {  return a[--i];  }
       public    void remove()  {                  }
    }
    
}
