package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.Gson;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;



public class ScoringMapperPredictUtil {
	
	private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
	
	private static final String CALIBRATION = "Calibration";
	private static final String CALIBRATION_MAXIMUMSCORE = "MaximumScore";
	private static final String CALIBRATION_MINIMUMSCORE = "MinimumScore";
	private static final String CALIBRATION_PROBABILITY = "Probability";
	private static final String AVERAGE_PROBABILITY = "AverageProbability";
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
	
	public static void evaluate(HashMap<String, JSONObject> models, HashMap<String, ArrayList<String>> leadInputRecordMap, String outputPath, int threshold) {
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
		readScoreFiles(leadInputRecordMap, models, outputPath, threshold);
	}
	
	private static void readScoreFiles(HashMap<String, ArrayList<String>> leadInputRecordMap, HashMap<String, JSONObject> models, String outputPath, int threshold) {
		log.info("come to the readScoreFiles function");
		Set<String> modelIDs = leadInputRecordMap.keySet();
		// list of HashMap<leadID: score>
		ArrayList<ModelEvaluationResult> resultList = new ArrayList<ModelEvaluationResult>();
		for(String id: modelIDs) {
			log.info("id is " + id);
			// key: leadID, value: raw score
			HashMap<String, Float> scores = new HashMap<String, Float>();
			// key: leadID, value: manipulated scores (raw, probability, etc)
			//HashMap<String, ModelEvaluationResult> results = new HashMap<String, ModelEvaluationResult>();
			int value = leadInputRecordMap.get(id).size();
			JSONObject model = models.get(id);
			int remain = value/threshold;
			int i = 0;
			do {
				readScoreFile(id, i, scores);
				i++;
			} 
			while (i < remain);
			Set<String> keySet = scores.keySet();
			for (String key : keySet) {
				ModelEvaluationResult result = getResult(id, model, scores.get(key));
				resultList.add(result);
				//results.put(key, result);
			}
		}
		writeToOutputFile(resultList, outputPath);
	}
	
	private static void readScoreFile(String modelID, int index, HashMap<String, Float> scores) {
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
				scores.put(splitLine[0], Float.parseFloat(splitLine[1]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private static void writeToOutputFile(ArrayList<ModelEvaluationResult> resultList, String outputPath)
	{
		log.info("in the function of writeToOutputFILE");
		String fileName = "/Users/ygao/Downloads/text.avro";
		File outputFile = new File(fileName);
  		DatumWriter<ModelEvaluationResult> userDatumWriter = new 
  SpecificDatumWriter<ModelEvaluationResult>();
          DataFileWriter<ModelEvaluationResult> dataFileWriter = new 
  DataFileWriter<ModelEvaluationResult>(userDatumWriter);
          
		Gson gson = new Gson();
		for (int i = 0; i < resultList.size(); i++) {
			ModelEvaluationResult result = resultList.get(i);
        	String json = gson.toJson(result);
        	System.out.println(json);
            try {
            	if (i == 0)
            		dataFileWriter.create(result.getSchema(), outputFile);
	            dataFileWriter.append(result);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}        	
		}
        try {
			dataFileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			HdfsUtils.copyLocalToHdfs(new Configuration(), "/Users/ygao/Downloads/text.avro", outputPath + "/test.avro");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static ModelEvaluationResult getResult(String modelID, JSONObject model, float score) {		
		Float probability = null;
		
		// perform calibration
		JSONArray calibrationRanges = (JSONArray) model.get(CALIBRATION);
		if (calibrationRanges != null) {
			for (int i = 0; i < calibrationRanges.size(); i++) {
				JSONObject range = (JSONObject) calibrationRanges.get(i);
				Double lowerBoundObj = (Double) range.get(CALIBRATION_MINIMUMSCORE);
				Double upperBoundObj = (Double) range.get(CALIBRATION_MAXIMUMSCORE);
				Float lowerBound = lowerBoundObj != null? (lowerBoundObj).floatValue() : null;
				Float upperBound = upperBoundObj != null? (upperBoundObj).floatValue() : null;
				if (betweenBounds(score, lowerBound, upperBound)) {
					Double probabilityObj = (Double) range.get(CALIBRATION_PROBABILITY);
					probability = probabilityObj != null ? (float) probabilityObj.floatValue() : null;
					break;
				}
			}
		}
		
		Double averageProbabilityObj = (Double) model.get(AVERAGE_PROBABILITY);
		Float averageProbability = averageProbabilityObj != null ? averageProbabilityObj.floatValue() : null;
		Float lift = averageProbability != null && averageProbability != 0 ? (float)probability/averageProbability : null;
		
		// perform bucketing 
		String bucket = null;
		JSONArray bucketRanges = (JSONArray) model.get(BUCKETS);
		if (bucketRanges != null) {
			for (int i = 0; i < bucketRanges.size(); i++) {
				Float value = probability;
				JSONObject range = (JSONObject) bucketRanges.get(i);
				// TODO need to Float check with Haitao/Ron about uncalibration
				if (value == null) {
					value = score;
				}
				// "0 - Probability, 1 - Lift"
				if (((Long)range.get(BUCKETS_TYPE)).intValue() == 1) {
					value = lift;
				}
				Double lowerBoundObj = (Double) range.get(BUCKETS_MINIMUMSCORE);
				Double upperBoundObj = (Double) range.get(BUCKETS_MAXIMUMSCORE);
				Float lowerBound = lowerBoundObj != null? (lowerBoundObj).floatValue() : null;
				Float upperBound = upperBoundObj != null? (upperBoundObj).floatValue() : null;
				if (value != null && betweenBounds(value, lowerBound, upperBound)) {
					bucket = (String) range.get(BUCKETS_NAME);
					break;
				}
			}
		}
		
		// bucket into percentiles
		Integer percentile = null;
		JSONArray percentileRanges = (JSONArray) model.get(PERCENTILE_BUCKETS);
		if (percentileRanges != null) {
			float topPercentileMaxScore = (float) 0.0;
			float bottomPercentileMinScore = (float) 1.0;
			Integer topPercentile = 100;
			Integer bottomPercentile = 1;
			boolean foundTopPercentileMaxScore = false;
			boolean foundbottomPercentileMinScore = false;
			for (int i = 0; i < percentileRanges.size(); i++) {
				JSONObject range = (JSONObject) percentileRanges.get(i);
				Double lowerBoundObj = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
				Double upperBoundObj = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
				Float min = lowerBoundObj != null ? lowerBoundObj.floatValue() : null;
				Float max = upperBoundObj != null ? upperBoundObj.floatValue() : null;
				Integer percent = ((Long) range.get(PERCENTILE_BUCKETS_PERCENTILE)).intValue();
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
					Double lowerBoundObj = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
					Double upperBoundObj = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
					Float min = lowerBoundObj != null ? lowerBoundObj.floatValue() : Float.MIN_VALUE;
					Float max = upperBoundObj != null ? upperBoundObj.floatValue() : null;
					Integer percent = ((Long) range.get(PERCENTILE_BUCKETS_PERCENTILE)).intValue();
					if (betweenBounds(score, min, max)) {
						percentile = percent;
						break;
					}
				}
			}
			
		}
		
		Integer integerScore = (int) (probability != null ? Math.round(probability * 100) : Math.round(score * 100));
		ModelEvaluationResult result = new ModelEvaluationResult(bucket, lift, modelID, percentile, probability, score, integerScore);
		log.info("result is " + result);
		return result;
		
	}
	
    private static boolean betweenBounds(float value, Float lowerInclusive, Float upperExclusive)
    {
        return (lowerInclusive == null || value >= lowerInclusive) && 
            (upperExclusive == null || value < upperExclusive);
    }
   
	
    @Autowired
    private static Configuration yarnConfiguration;
    
	public static void main(String[] args) throws Exception {
		
		
		String local = "/Users/ygao/Downloads/text.avro";

		String hdfs = "/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/test" + "/test.avro";
		List<GenericRecord> list = AvroUtils.getData(new Configuration(), new Path(hdfs));
		for (GenericRecord ele : list) {
			System.out.println(ele.toString());
		}
		
		
		/*
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        
        Path p = new Path(absolutePath + "87ecf8cd-fe45-45f7-89d1-612235631fc1");
        ScoringMapperTransformUtil.parseModelFiles(models, p);
        //evaluate(models, modelNumberMap, THRESHOLD);
        Set<String> keys = models.keySet(); 
        for (String key : keys) {
        
        	JSONObject model = models.get(key);
        	getResult("87ecf8cd-fe45-45f7-89d1-612235631fc1", model, 0.0042960797f);
        }
        */
	}
	
}
