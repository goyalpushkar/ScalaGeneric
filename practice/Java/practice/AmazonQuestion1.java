package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import com.cswg.Util.UtilCommon;

public class AmazonQuestion1 {

	public static double distance( int xCoor, int yCoor ) {
		  return Math.sqrt( Math.pow(xCoor, 2) + Math.pow(yCoor, 2) );
	}
	
    /*public static void quickSort( int[] sortedArray, int startIndex, int endIndex ) {
            if ( startIndex >= endIndex )
                return;
               
            int pivotValue = sortedArray[ (startIndex / 2) + (endIndex / 2) ];
            
            //println( startIndex + " -- " + endIndex + " --> " + pivotValue)
            val pivotIndex = partition(sortedArray, startIndex, endIndex, pivotValue)
            quickSort( sortedArray, startIndex, pivotIndex - 1 )
            quickSort( sortedArray, pivotIndex, endIndex )

        }
        
        static void exchange( int[] sortedArray, int firstIndex, int nextIndex ) {
             
            int temp = sortedArray[firstIndex];
            sortedArray[ sortedArray[nextIndex] ];
            sortedArray[temp];
        }
        
        static void partition( sortedArray: Array[Int], startIndex: Int, endIndex: Int, pivot: Int ): Int = {
            
            var leftSide = startIndex
            var rightSide = endIndex
            
            while( leftSide <= rightSide ){
              while( sortedArray.apply(leftSide) < pivot )
                 leftSide = leftSide.+(1)
  
              while ( sortedArray.apply(rightSide) > pivot )
                 rightSide = rightSide.-(1)               
               
              if ( leftSide <= rightSide ){
                 exchange( sortedArray, leftSide, rightSide)
                 leftSide = leftSide.+(1)
                 rightSide = rightSide.-(1)    
              }
            }
            return leftSide
        }*/
        
	/*public static void merger(Map<List<Integer>, Double> array, int left, int middle, int right ){
	      
		  int n1 = middle - left + 1;
		  int n2 = right - middle;
		  
		  int[] leftArr = new int[n1];
		  int[] rightArr = new int[n2];
				  
		  
		  for( int i=0; i<left; i++ ){
			  leftArr[i] = array.get(left + 1);
		  }
		  
	}
	
	public static void mergeSort( Map<List<Integer>, Double> array, int left, int right ){
	
		 if ( left < right ){
			 
			 int middle = left/2 + right/2;
			 
			 mergeSort( array, left, middle);
			 mergeSort( array, middle+1, right);
			 
			 merge( array, left, middle, right);
			 
		 }
	}
	*/
	public static List<List<Integer>> nearestVegetarian( int totalRest, List<List<Integer>> allocations, int numRestaurents ){
		
		Map<List<Integer>, Double> distanceMatrix = new HashMap<List<Integer>, Double>();
		for( List<Integer> point: allocations ){
			double distanceFromUser = distance( point.get(0), point.get(1) );
			distanceMatrix.put( point, distanceFromUser);
		}
		
		distanceMatrix.forEach( (key, value) -> System.out.println( key + " -> " + value ));
		
		//mergeSort( distanceMatrix, 0, distanceMatrix.size() );
	     List<Map.Entry<List<Integer>, Double>> newList = new LinkedList<Map.Entry<List<Integer>, Double>>( distanceMatrix.entrySet());
	     
	     Collections.sort(newList, new Comparator<Map.Entry<List<Integer>, Double> > () {
	    	  public int compare( Map.Entry<List<Integer>, Double> val1, Map.Entry<List<Integer>, Double> val2) {
	    		  return val1.getValue().compareTo(val2.getValue());
	    	  }
	     });
	     
	    List<List<Integer>> returnList = new ArrayList<List<Integer>>();
		for( int i=0; i <numRestaurents; i++ ){
			returnList.add( newList.get(i).getKey());
		}
		
		return returnList;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(UtilCommon.fileLocation);
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation + "AmazonQuestion1.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        int totalRestaurents = scanner.nextInt();
        System.out.println(totalRestaurents);
        scanner.nextLine();
        List<List<Integer>> locations = new ArrayList<List<Integer>>();
        for( int i = 0; i<totalRestaurents; i++){
        	//System.out.println( scanner.nextLine().split(" ") );
        	String[] arrItems = scanner.nextLine().split(" ");
        	//System.out.println(arrItems[0].trim() + " " + arrItems[1].trim() );
        	List<Integer> points = new ArrayList<Integer>();
        	
        	points.add( Integer.parseInt( arrItems[0].trim() ) );
        	points.add( Integer.parseInt( arrItems[1].trim() ) );
        	//System.out.println("Points Added");
        	locations.add( points );
        }
        
        for( List<Integer> points: locations ){
        	System.out.println( points.get(0) + " " +  points.get(1) );
        }
        
        int numRestaurents = scanner.nextInt();
        System.out.println(totalRestaurents);
        List<List<Integer>> returnVal = nearestVegetarian( totalRestaurents, locations, numRestaurents);
        
        for( List<Integer> points: returnVal ){
        	System.out.println( points.get(0) + " " +  points.get(1) );
        }
        
        scanner.close();
        
	}
	
}
