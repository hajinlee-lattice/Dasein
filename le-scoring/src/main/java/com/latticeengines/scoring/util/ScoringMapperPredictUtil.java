package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.hadoop.fs.Path;

import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;



public class ScoringMapperPredictUtil {
	
	private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
	
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
    
    // TODO debugging purposes
	private static final String absolutePath = "/Users/ygao/test/e2e/";
    private static final int THRESHOLD = 10000;
	
	public static void evaluate(HashMap<String, JSONObject> models, HashMap<String, Integer> modelNumberMap, int threshold) {
		// spawn python 
		Set<String> modelIDs = models.keySet();
		StringBuilder sb = new StringBuilder();
		for (String modelID : modelIDs) {
			sb.append(modelID +" ");
		} 
		//Process p = Runtime.getRuntime().exec("python /Users/ygao/Documents/workspace/ledp/le-dataplatform/src/test/python/test2.py " + sb.toString());
		log.info("python " + "scoring.py " + sb.toString());
		File pyFile = new File("scoring.py");
		if (!pyFile.exists()) {
			new Exception("scoring.py does not exist!");
		}
		
		Process p = null;
		try {
			p = Runtime.getRuntime().exec("python " + "scoring.py " + sb.toString());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		String line = null;
		try {
			while (( line = in.readLine()) != null ) {
				log.info(line);
			}
			in.close();
			while (( line = err.readLine()) != null ) {
				log.info(line);
			}
			err.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		log.info("come to the evaluate function");
		readScoreFiles(modelNumberMap, models, threshold);
	}
	
	private static void readScoreFiles(HashMap<String, Integer> modelNumberMap, HashMap<String, JSONObject> models, int threshold) {
		log.info("come to the readScoreFiles function");
		Set<String> modelIDs = modelNumberMap.keySet();
		for(String id: modelIDs) {
			log.info("id is " + id);
			HashMap<String, Double> scores = new HashMap<String, Double>();
			HashMap<String, ModelEvaluationResult> results = new HashMap<String, ModelEvaluationResult>();
			int value = modelNumberMap.get(id);
			JSONObject model = models.get(id);
			int remain = value/threshold;
			int i = 0;
			do {
				readScoreFile(id, i, scores);
				i++;
			} while (i < remain);
			Set<String> keySet = scores.keySet();
			for (String key : keySet) {
				ModelEvaluationResult result = getResult(id, model, scores.get(key));
				results.put(key, result);
			}
			
		}
	}
	
	private static void readScoreFile(String modelID, int index, HashMap<String, Double> scores) {
		log.info("come to the readScoreFile function");
		//TODO change it to relative path
		//String fileName = absolutePath + modelID + SCORING_OUTPUT_PREFIX + index + ".txt";
		String fileName = modelID + SCORING_OUTPUT_PREFIX + index + ".txt";
		File f = new File(fileName);
		if (!f.exists()) {
			new Exception ("Output file" + fileName + "does not exist!");
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
	
	private static ModelEvaluationResult getResult(String modelID, JSONObject model, Double score) {		
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
				// TODO need to double check with Haitao/Ron about uncalibration
				if (value == null) {
					value = score;
				}
				// TODO need to double check with Haitao/Ron about uncalibration
				if ((Long)range.get(BUCKETS_TYPE) == 1) {
					value = lift;
				}
				
				if (value != null && betweenBounds(value, (Double)range.get(BUCKETS_MINIMUMSCORE), (Double)range.get(BUCKETS_MAXIMUMSCORE))) {
					bucket = (String) range.get(BUCKETS_NAME);
					break;
				}
			}
		}
		
		// bucket into percentiles
		Long percentile = null;
		JSONArray percentileRanges = (JSONArray) model.get(PERCENTILE_BUCKETS);
		if (percentileRanges != null) {
			Double topPercentileMaxScore = 0.0;
			Double bottomPercentileMinScore = 1.0;
			Long topPercentile = (long) 100;
			Long bottomPercentile = (long) 1;
			boolean foundTopPercentileMaxScore = false;
			boolean foundbottomPercentileMinScore = false;
			for (int i = 0; i < percentileRanges.size(); i++) {
				JSONObject range = (JSONObject) percentileRanges.get(i);
				Double min = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
				Double max = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
				Long percent = (Long) range.get(PERCENTILE_BUCKETS_PERCENTILE);
				if (max > topPercentileMaxScore) {
					topPercentileMaxScore = max;
					topPercentile = percent;
					foundTopPercentileMaxScore = true;
				}
				if (min < bottomPercentileMinScore) {
					bottomPercentileMinScore = min;
					bottomPercentile = percent;
					foundbottomPercentileMinScore = true;
				}
			}
			
			if (foundTopPercentileMaxScore && score >= topPercentileMaxScore) {
				percentile = topPercentile;
			} else if (foundbottomPercentileMinScore && score <= bottomPercentileMinScore) {
				percentile = bottomPercentile;
			} else {
				for (int i = 0; i < percentileRanges.size(); i++) {
					JSONObject range = (JSONObject) percentileRanges.get(i);
					Double min = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
					Double max = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
					Long percent = (Long) range.get(PERCENTILE_BUCKETS_PERCENTILE);
					if (betweenBounds(score, min, max)) {
						percentile = percent;
						break;
					}
				}
			}
			
		}
		
		Long integerScore = (long) (probability != null ? Math.round(probability * 100) : Math.round(score * 100));
		ModelEvaluationResult result = new ModelEvaluationResult(score, integerScore, bucket, modelID, probability, lift, percentile);
		log.info("result is " + result);
		return result;
		
	}
	
    private static boolean betweenBounds(double value, Double lowerInclusive, Double upperExclusive)
    {
        return (lowerInclusive == null || value >= lowerInclusive) && 
            (upperExclusive == null || value < upperExclusive);
    }
	
	public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        HashMap<String, Integer> modelNumberMap = new HashMap<String, Integer>();
        
        Path p = new Path(absolutePath + "87ecf8cd-fe45-45f7-89d1-612235631fc1");
        //ScoringMapperTransformUtil.parseModelFiles(models, p, modelNumberMap);
        //evaluate(models, modelNumberMap, THRESHOLD);
		log.info("python " + absolutePath + "test2.py " + "87ecf8cd-fe45-45f7-89d1-612235631fc1");
		Process p1 = Runtime.getRuntime().exec("python " + absolutePath + "test2.py " + "87ecf8cd-fe45-45f7-89d1-612235631fc1");
		BufferedReader in = new BufferedReader(new InputStreamReader(p1.getInputStream()));
		BufferedReader err = new BufferedReader(new InputStreamReader(p1.getErrorStream()));
		String line = null;
		while (( line = in.readLine()) != null ) {
			System.out.println(line);
		}
		in.close();
		while (( line = err.readLine()) != null ) {
			System.out.println(line);
		}
		err.close();
		p1.waitFor();
		log.info("come to the evaluate function");
	}

}
