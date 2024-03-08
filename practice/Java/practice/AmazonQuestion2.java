package com.cswg.practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

import com.cswg.Util.UtilCommon;

public class AmazonQuestion2 {

	public static List<List<Integer>> optimal( int deviceCap, List<List<Integer>> forGroundApp, List<List<Integer>> backGroundApp){
		
		backGroundApp.sort( new Comparator<List<Integer>> () {
			                 public int compare(List<Integer> val1, List<Integer> val2 ){
			                	 return -1 * val1.get(0).compareTo( val2.get(0));
			                 }
		});
		
		List<List<Integer>> newList = new ArrayList<List<Integer>>();
		for( int index=0; index< forGroundApp.size(); index++ ){
			int maxCap = ( deviceCap - forGroundApp.get(index).get(1) );
			for( int backIndex = 0; backIndex < backGroundApp.size(); backIndex++ ){
				 if( backGroundApp.get(backIndex).get(1) > maxCap ) {
					 continue;
				 }else{
					 List<Integer> points = new ArrayList<Integer>();
					 points.add( forGroundApp.get(index).get(0) );
					 points.add( backGroundApp.get(backIndex).get(0) );
					 newList.add( points );
					 break;
				 }
			}
		}
		
		return newList;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		System.out.println(UtilCommon.fileLocation);
        try {
			System.setIn(new FileInputStream( UtilCommon.fileLocation + "AmazonQuestion2.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Scanner scanner = new Scanner(System.in);
        int deviceCapacity = scanner.nextInt();
        System.out.println(deviceCapacity);
        int foregroundApp = scanner.nextInt();
        System.out.println(foregroundApp);
        scanner.nextLine();
        List<List<Integer>> foregroundMemory = new ArrayList<List<Integer>>();
        for( int i = 0; i<foregroundApp; i++){
        	//System.out.println( scanner.nextLine().split(" ") );
        	String[] arrItems = scanner.nextLine().split(" ");
        	//System.out.println(arrItems[0].trim() + " " + arrItems[1].trim() );
        	List<Integer> points = new ArrayList<Integer>();
        	
        	points.add( Integer.parseInt( arrItems[0].trim() ) );
        	points.add( Integer.parseInt( arrItems[1].trim() ) );
        	//System.out.println("Points Added");
        	foregroundMemory.add( points );
        }
        
        //scanner.nextLine();
        int backgroundApp = scanner.nextInt();
        System.out.println(backgroundApp);
        scanner.nextLine();
        List<List<Integer>> backgroundMemory = new ArrayList<List<Integer>>();
        for( int i = 0; i<backgroundApp; i++){
        	//System.out.println( scanner.nextLine().split(" ") );
        	String[] arrItems = scanner.nextLine().split(" ");
        	//System.out.println(arrItems[0].trim() + " " + arrItems[1].trim() );
        	List<Integer> points = new ArrayList<Integer>();
        	
        	points.add( Integer.parseInt( arrItems[0].trim() ) );
        	points.add( Integer.parseInt( arrItems[1].trim() ) );
        	//System.out.println("Points Added");
        	backgroundMemory.add( points );
        }
        
        for( List<Integer> points: foregroundMemory ){
        	System.out.println( points.get(0) + " " +  points.get(1) );
        }
        
        for( List<Integer> points: backgroundMemory ){
        	System.out.println( points.get(0) + " " +  points.get(1) );
        }
        
        //int numRestaurents = scanner.nextInt();
        List<List<Integer>> returnVal = optimal( deviceCapacity, foregroundMemory, backgroundMemory);
        System.out.println( "Returned Value ");
        for( List<Integer> points: returnVal ){
        	System.out.println( points.get(0) + " " +  points.get(1) );
        }
        
        scanner.close();
        
	}

}
