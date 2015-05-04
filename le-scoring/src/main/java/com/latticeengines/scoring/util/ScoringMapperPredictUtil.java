package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;



public class ScoringMapperPredictUtil {
	
	private static final String CALIBRATION = "Calibration";
	private static final String CALIBRATION_MAXIMUMSCORE = "MaximumScore";
	private static final String CALIBRATION_MINIMUMSCORE = "MinimumScore";
	private static final String CALIBRATION_PROBABILITY = "Probability";
	private static final String AVERAGE_PROBABILTY = "AverageProbability";
	private static final String BUCKETS = "Buckets";
	private static final String BUCKETS_TYPE = "Type";
	private static final String BUCKETS_MAXIMUMSCORE = "Maximum";
	private static final String BUCKETS_MINIMUMSCORE = "Minimum";
	private static final String BUCKETS_NAME = "Name";
	private static final String PERCENTILE_BUCKETS = "PercentileBuckets";
	private static final String PERCENTILE_BUCKETS_PERCENTILE = "Percentile";
	private static final String PERCENTILE_BUCKETS_MINIMUMSCORE = "MinimumScore";
	private static final String PERCENTILE_BUCKETS_MAXIMUMSCORE = "MaximumScore";
    private static final String SCORING_OUTPUT_PREFIX = "scoringoutputfile-";
	
	public static void Evaluate(HashMap<String, JSONObject> models, HashMap<String, Integer> modelNumberMap, int threshold) throws NumberFormatException, IOException {
		// spawn python 
		Set<String> modelIDs = models.keySet();
		StringBuilder sb = new StringBuilder();
		for (String modelID : modelIDs) {
			sb.append(modelID +" ");
		} 
		Process p = Runtime.getRuntime().exec("python /Users/ygao/Documents/workspace/ledp/le-dataplatform/src/test/python/test2.py " + sb.toString());
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
		
		readScoreFiles(modelNumberMap, models, threshold);
		
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
	
	private static void readScoreFiles(HashMap<String, Integer> modelNumberMap, HashMap<String, JSONObject> models, int threshold) {
		Set<String> modelIDs = modelNumberMap.keySet();
		for(String id: modelIDs) {
			HashMap<String, Double> scores = new HashMap<String, Double>();
			HashMap<String, ModelEvaluationResult> results = new HashMap<String, ModelEvaluationResult>();
			int value = modelNumberMap.get(id);
			JSONObject model = models.get(id);
			int remain = value/threshold;
			for (int i = 0; i < remain; i++) {
				readScoreFile(id, i, scores);
			}
			Set<String> keySet = scores.keySet();
			for (String key : keySet) {
				ModelEvaluationResult result = getResult(id, model, scores.get(key));
				results.put(key, result);
			}
			
		}
	}
	
	private static void readScoreFile(String modelID, int index, HashMap<String, Double> scores) {
		String fileName = modelID + SCORING_OUTPUT_PREFIX + index + ".txt";
		File f = new File(fileName);
		if (!f.exists()) {
			new Exception ("Output file" + fileName + "does not exit!");
		}
		try {
			List<String> lines = FileUtils.readLines(f);
			for (String line : lines) {
				String[] splitLine = line.split(",");
				if (splitLine.length != 2) {
					new Exception ("Scoring output file in incorrect format");
				}
				scores.put(splitLine[0], Double.parseDouble(splitLine[1]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static ModelEvaluationResult getResult(String id, JSONObject model, Double score) {
		Double probability = null;
		
		// perform calibration
		JSONArray calibrationRanges = (JSONArray) model.get(CALIBRATION);
		if (calibrationRanges != null) {
			for (int i = 0; i < calibrationRanges.size(); i++) {
				JSONObject range = (JSONObject) calibrationRanges.get(i);
				if (betweenBounds(score, (Double)range.get(CALIBRATION_MINIMUMSCORE), (Double)range.get(CALIBRATION_MAXIMUMSCORE))) {
					probability = (Double) range.get(CALIBRATION_PROBABILITY);
					break;
				}
			}
		}
		
		Double averageProbability = (Double) model.get(AVERAGE_PROBABILTY);
		Double lift = averageProbability != null && averageProbability != 0 ? probability/averageProbability : null;
		
		// perform bucketing 
		String bucket = null;
		JSONArray bucketRanges = (JSONArray) model.get(BUCKETS);
		if (bucketRanges != null) {
			for (int i = 0; i < bucketRanges.size(); i++) {
				Double value = probability;
				JSONObject range = (JSONObject) bucketRanges.get(i);
				// need to double check with Haitao/Ron about uncalibration
				if (value == null) {
					value = score;
				}
				// need to double check with Haitao/Ron about uncalibration
				if ((Integer)range.get(BUCKETS_TYPE) == 1) {
					value = lift;
				}
				
				if (value != null && betweenBounds(value, (Double)range.get(BUCKETS_MINIMUMSCORE), (Double)range.get(BUCKETS_MAXIMUMSCORE))) {
					bucket = (String) range.get(BUCKETS_NAME);
					break;
				}
			}
		}
		
		// bucket into percentiles
		
		return null;
		
	}
	
    private static boolean betweenBounds(double value, Double lowerInclusive, Double upperExclusive)
    {
        return (lowerInclusive == null || value >= lowerInclusive) && 
            (upperExclusive == null || value < upperExclusive);
    }
	
	public static void main(String[] args) throws NumberFormatException, IOException {
		Set<String> set = new HashSet<String>();
		set.add("id1");
		set.add("id2");
		set.add("id3");
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put("id1", 1);
		map.put("id2", 2);
		map.put("id3", 3);
		//Evaluate(set, map, 1);
	}

}
