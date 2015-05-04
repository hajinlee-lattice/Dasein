package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;


public class ScoringMapperPredictUtil {
	
	public static void Evaluate(Set<String> modelIDs) throws NumberFormatException, IOException {
		// spawn python 
		StringBuilder sb = new StringBuilder();
		for (String modelID : modelIDs) {
			sb.append(modelID +" ");
		} 
		Process p = Runtime.getRuntime().exec("python /Users/ygao/Documents/workspace/ledp/le-dataplatform/src/test/python/test2.py " + sb.toString());
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
		
		String line = null;
		try {
		    while ((line = in.readLine()) != null) {
		        System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws NumberFormatException, IOException {
		Set<String> set = new HashSet<String>();
		set.add("id1");
		set.add("id2");
		set.add("id3");
		Evaluate(set);
	}

}
