package com.practice.abstractTypes;

import com.practice.basic.StdOut;
import com.practice.basic.StdRandom;

public class Counter implements Comparable<Counter> {

	public final String name;
	private int count = 0;

	public Counter(String name) {
		super();
		this.name = name;
	}

	void increment(){
		count = count + 1;
	}
	
	int tally(){
		return count;
	}
	
	@Override
	public String toString() {
		return count + " " + name;
	}
	
	/**
     * Compares this counter to the specified counter.
     *
     * @param  that the other counter
     * @return {@code 0} if the value of this counter equals
     *         the value of that counter; a negative integer if
     *         the value of this counter is less than the value of
     *         that counter; and a positive integer if the value
     *         of this counter is greater than the value of that
     *         counter
     */
    @Override
    public int compareTo(Counter that) {
        if      (this.count < that.count) return -1;
        else if (this.count > that.count) return +1;
        else                              return  0;
    }


    /**
     * Reads two command-line integers n and trials; creates n counters;
     * increments trials counters at random; and prints results.
     *
     * @param args the command-line arguments
     */
    public static void main(String[] args) { 
        int n = Integer.parseInt(args[0]);
        int trials = Integer.parseInt(args[1]);

        // create n counters
        Counter[] hits = new Counter[n];
        for (int i = 0; i < n; i++) {
            hits[i] = new Counter("counter" + i);
        }

        // increment trials counters at random
        for (int t = 0; t < trials; t++) {
            hits[StdRandom.uniform(n)].increment();
        }

        // print results
        for (int i = 0; i < n; i++) {
            StdOut.println(hits[i]);
        }
    } 
	
}
