package com.practice.MergeSort;

import com.practice.basic.StdOut;

public class AbstractMerge {

	private static Comparable[] aux;
	
	/*
	 * This method merges by fi rst copying into the auxiliary array aux[] then merging back to a[]. In the
		merge (the second for loop), there are four conditions: left half exhausted (take from the right), right
		half exhausted (take from the left), current key on right less than current key on left (take from the
		right), and current key on right greater than or equal to current key on left (take from the left).
	 */
	public static void merge(Comparable[] a, int lo, int mid, int hi)
	{ // Merge a[lo..mid] with a[mid+1..hi].
		int i = lo, j = mid+1;		
		
		for (int k = lo; k <= hi; k++) // Copy a[lo..hi] to aux[lo..hi].
			aux[k] = a[k];
		
		for (int k = lo; k <= hi; k++) // Merge back to a[lo..hi].
			if (i > mid) a[k] = aux[j++];
			else if (j > hi ) a[k] = aux[i++];
			else if (less(aux[j], aux[i])) a[k] = aux[j++];
			else a[k] = aux[i++];
	}
	
	private static boolean less(Comparable v, Comparable w)
	{ return v.compareTo(w) < 0; }
	
	private static void exch(Comparable[] a, int i, int j)
	{ Comparable t = a[i]; a[i] = a[j]; a[j] = t; }
	
	private static void show(Comparable[] a)
	{ // Print the array, on a single line.
		for (int i = 0; i < a.length; i++)
		StdOut.print(a[i] + " ");
		StdOut.println();
	}
	
	public static boolean isSorted(Comparable[] a)
	{ // Test whether the array entries are in order.
		for (int i = 1; i < a.length; i++)
		if (less(a[i], a[i-1])) return false;
		return true;
	}
	
	// Top Down Merge Sort
	/* To sort a subarray a[lo..hi] we divide it into two parts: a[lo..mid] and a[mid+1..hi], sort them
		independently (via recursive calls), and merge the resulting ordered subarrays to produce the result
	 */
	public static void topDownSort(Comparable[] a)
	{
		aux = new Comparable[a.length]; // Allocate space just once.
		sort(a, 0, a.length - 1);
	}
	
	private static void sort(Comparable[] a, int lo, int hi)
	{ // Sort a[lo..hi].
		if (hi <= lo) return;
		int mid = lo + (hi - lo)/2;
		sort(a, lo, mid); // Sort left half.
		sort(a, mid+1, hi); // Sort right half.
		merge(a, lo, mid, hi); // Merge results
	}
	
	//Bottom Up Merge Sort
	public static void bottomUpSort(Comparable[] a)
	{ // Do lg N passes of pairwise merges.
		int N = a.length;
		aux = new Comparable[N];
		for (int sz = 1; sz < N; sz = sz+sz) // sz: subarray size
			for (int lo = 0; lo < N-sz; lo += sz+sz) // lo: subarray index
				merge(a, lo, lo+sz-1, Math.min(lo+sz+sz-1, N-1));
	}
	
}
