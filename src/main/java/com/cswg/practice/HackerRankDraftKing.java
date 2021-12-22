package com.cswg.practice;

import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;

public class HackerRankDraftKing {

	   public static void main(String[] args) {
	        ConsoleProcessor processor = new ConsoleProcessor();
	        processor.processAllLines();
	    }
}

class ConsoleProcessor {

    public OrgChart orgChart = new OrgChart();

    public void processAllLines() {
        Scanner in = new Scanner(System.in);
        String line = in.nextLine();

        Integer numLines = 0;

        try {
           numLines = Integer.valueOf(line.trim());
        } catch (NumberFormatException ex) {
            ex.printStackTrace();
        }

        for (int i = 0; i < numLines; i++) {
            processLine(in.nextLine());
        }

        in.close();
    }

    protected void processLine(String line) {
        String[] parsedCommand = line.split(",");

        // ignore empty lines
        if (parsedCommand.length == 0) {
            return;
        }

        switch (parsedCommand[0]) {
            case "add":
                orgChart.add(parsedCommand[1], parsedCommand[2], parsedCommand[3]);
                break;
            case "print":
                orgChart.print();
                break;
            case "remove":
                orgChart.remove(parsedCommand[1]);
                break;
            case "move":
                orgChart.move(parsedCommand[1], parsedCommand[2]);
                break;
            case "count":
                System.out.println(orgChart.count(parsedCommand[1]));
                break;
        }
    }
}

class OrgChart {

	//Set<String> managers = new HashSet<String>();
	//Set<String> employees = new HashSet<String>();
	//Map<String, String> employees = new HashMap<String, String>();
	Map<String, String> manager = new HashMap<String, String>();
    Map<String, LinkedHashMap<String, String>> reportees = new HashMap< String, LinkedHashMap<String, String>>();
    LinkedHashMap<String, String> emptyList = new LinkedHashMap<String, String>();
    
    public void add(String id, String name, String managerId)
    {
    	String newManagerId = managerId;
    	if ( !(reportees.containsKey(managerId) ) ){
    	   newManagerId  = "-1";
    	}
    	if ( !manager.containsKey(id) ){
    		//Add employee
    		//employees.put(id, name);
    		
    		//Add employee under his manger
    		LinkedHashMap<String, String> managerReportee = reportees.getOrDefault(newManagerId, emptyList);
    		managerReportee.put(id, name);
    		reportees.put(newManagerId, managerReportee);
    		
    		//Add employee and his manger
    		manager.put(id, newManagerId);
    		return;
    	}
    		
        //throw new UnsupportedOperationException();
    }

    public void printTree( String root, int level) { //, LinkedHashMap<String, String> reporteeList
		for ( int i = 0; i < level; i ++ ){
			System.out.println("\t");
		}
		if ( root != "-1" ){
		   System.out.println( reportees.get(root) + " " + "[" + root + "]" );
		   System.out.println("\n");
		}
		LinkedHashMap<String, String> directReportee = reportees.getOrDefault(root, emptyList);
		
		for( String empId :  directReportee.keySet() ){
			printTree( empId, level + 1);
		}
		
    }
    
    
    public void print()
    {
    	
    	printTree( "-1", 0);// reportees.get("-1")
    	
        //throw new UnsupportedOperationException();
    }

    public void remove(String employeeId)
    {
    	if ( manager.containsKey(employeeId) ){
    		String managerId = manager.getOrDefault(employeeId, "-1");
    		
    		//Remove this employee from managers reportee and Move Reportees of removed employee to manager
    		LinkedHashMap<String, String> reporteeList = reportees.getOrDefault(employeeId, emptyList);
    		LinkedHashMap<String, String> managersReporteeList = reportees.getOrDefault(managerId, emptyList);
    		managersReporteeList.remove(employeeId);
    		managersReporteeList.putAll(reporteeList);
    		reportees.put(managerId, managersReporteeList);//managersReporteeList
    		
    		//Remove employee
    		manager.remove(employeeId);
    	}
    	
        //throw new UnsupportedOperationException();
    }

    public void move(String employeeId, String newManagerId)
    {
    	if ( manager.containsKey(employeeId) ){
    		if ( manager.containsKey(newManagerId) ){
    			LinkedHashMap<String, String> reporteeList = reportees.getOrDefault(newManagerId, emptyList);
    			LinkedHashMap<String, String> currentManagerReportee = reportees.getOrDefault( manager.get(employeeId), emptyList );
    			reporteeList.put(employeeId, currentManagerReportee.get(employeeId));
    			reportees.put(newManagerId, reporteeList);
    		}else{
    			return;
    		}
    	}
        //throw new UnsupportedOperationException();
    }

    public int count(String employeeId)
    {
    	
    	int totalCount = reportees.getOrDefault(employeeId, emptyList).size();
    	LinkedHashMap<String, String> reporteeList = reportees.getOrDefault(employeeId, emptyList);
    	for( String empId :  reporteeList.keySet() ){
    		totalCount = totalCount + reportees.getOrDefault(empId, emptyList).size();
    	}
    	
    	return totalCount;
        //throw new UnsupportedOperationException();
    }
}

