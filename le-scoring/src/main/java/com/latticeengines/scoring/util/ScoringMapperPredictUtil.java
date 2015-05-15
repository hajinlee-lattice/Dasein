package com.latticeengines.scoring.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
	
	public static ArrayList<ModelEvaluationResult> evaluate(HashMap<String, JSONObject> models, HashMap<String, ArrayList<String>> leadInputRecordMap, HashMap<String, String> modelIdMap, String[] requestID, String outputPath, int threshold) {
		ArrayList<ModelEvaluationResult> resultList= null;
		// spawn python 
		Set<String> modelIDs = models.keySet();
		StringBuilder sb = new StringBuilder();
		for (String modelID : modelIDs) {
			sb.append(modelID +" ");
		} 
		
		log.info("/usr/local/bin/python2.7 " + "scoring.py " + sb.toString());
		File pyFile = new File("scoring.py");
		if (!pyFile.exists()) {
			new Exception("scoring.py does not exist!");
		}
		
		Process p = null;
		try {
			p = Runtime.getRuntime().exec("/usr/local/bin/python2.7 " + "scoring.py " + sb.toString());
		} catch (IOException e1) {
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
			e.printStackTrace();
		}
		
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		resultList = readScoreFiles(leadInputRecordMap, models, modelIdMap, requestID, outputPath, threshold);
		return resultList;
	}
	

	public static void writeToOutputFile(ArrayList<ModelEvaluationResult> resultList, Configuration yarnConfiguration, String outputPath)
	{
		if (resultList == null) {
			new Exception("resultList is null");
		}
		if (yarnConfiguration == null) {
			new Exception("yarnConfiguration is null");
		}
		String fileName = UUID.randomUUID() + ".avro";
		File outputFile = new File(fileName);
  		DatumWriter<ModelEvaluationResult> userDatumWriter = new SpecificDatumWriter<ModelEvaluationResult>();
        DataFileWriter<ModelEvaluationResult> dataFileWriter = new DataFileWriter<ModelEvaluationResult>(userDatumWriter);
        
		for (int i = 0; i < resultList.size(); i++) {
			ModelEvaluationResult result = resultList.get(i);
            try {
            	if (i == 0)
            		dataFileWriter.create(result.getSchema(), outputFile);
	            dataFileWriter.append(result);
			} catch (IOException e) {
				e.printStackTrace();
			}        	
		}
        try {
			dataFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        try {
			HdfsUtils.copyLocalToHdfs(yarnConfiguration, fileName, outputPath + "/" + fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static ArrayList<ModelEvaluationResult> readScoreFiles(HashMap<String, ArrayList<String>> leadInputRecordMap, HashMap<String, JSONObject> models, HashMap<String, String> modelIdMap, String[] requestID, String outputPath, int threshold) {
		Set<String> modelIDs = leadInputRecordMap.keySet();
		// list of HashMap<leadID: score>
		ArrayList<ModelEvaluationResult> resultList = new ArrayList<ModelEvaluationResult>();
		for(String id: modelIDs) {
			log.info("id is " + id);
			// key: leadID, value: raw score
			HashMap<String, Double> scores = new HashMap<String, Double>();
			int value = leadInputRecordMap.get(id).size();
			JSONObject model = models.get(id);
			int remain = value/threshold;
			for (int i = 0; i <= remain; i++) {
				readScoreFile(id, i, scores);
			}
			Set<String> keySet = scores.keySet();
			for (String key : keySet) {
				ModelEvaluationResult result = getResult(modelIdMap, id, key, requestID[0], model, scores.get(key));
				resultList.add(result);
			}
		}
		return resultList;
	}
	
	private static void readScoreFile(String modelID, int index, HashMap<String, Double> scores) {
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
	
	private static ModelEvaluationResult getResult(HashMap<String, String> modelIdMap, String modelID, String leadID, String requestID, JSONObject model, double score) {		
		Double probability = null;
		
		// perform calibration
		JSONArray calibrationRanges = (JSONArray) model.get(CALIBRATION);
		if (calibrationRanges != null) {
			for (int i = 0; i < calibrationRanges.size(); i++) {
				JSONObject range = (JSONObject) calibrationRanges.get(i);
				Double lowerBound = (Double) range.get(CALIBRATION_MINIMUMSCORE);
				Double upperBound = (Double) range.get(CALIBRATION_MAXIMUMSCORE);
				if (betweenBounds(score, lowerBound, upperBound)) {
					Double probabilityObj = (Double) range.get(CALIBRATION_PROBABILITY);
					probability = probabilityObj != null ? (Double) probabilityObj.doubleValue() : null;
					break;
				}
			}
		}
		
		Double averageProbability = (Double) model.get(AVERAGE_PROBABILITY);
		Double lift = averageProbability != null && averageProbability != 0 ? (double)probability/averageProbability : null;
		
		// perform bucketing 
		String bucket = null;
		JSONArray bucketRanges = (JSONArray) model.get(BUCKETS);
		if (bucketRanges != null) {
			for (int i = 0; i < bucketRanges.size(); i++) {
				Double value = probability;
				JSONObject range = (JSONObject) bucketRanges.get(i);
				// TODO need to Double check with Haitao/Ron about uncalibration
				if (value == null) {
					value = score;
				}
				// "0 - Probability, 1 - Lift"
				if (((Long)range.get(BUCKETS_TYPE)).intValue() == 1) {
					value = lift;
				}
				Double lowerBound = (Double) range.get(BUCKETS_MINIMUMSCORE);
				Double upperBound = (Double) range.get(BUCKETS_MAXIMUMSCORE);
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
			Double topPercentileMaxScore = (Double) 0.0;
			Double bottomPercentileMinScore = (Double) 1.0;
			Integer topPercentile = 100;
			Integer bottomPercentile = 1;
			boolean foundTopPercentileMaxScore = false;
			boolean foundbottomPercentileMinScore = false;
			for (int i = 0; i < percentileRanges.size(); i++) {
				JSONObject range = (JSONObject) percentileRanges.get(i);
				Double min = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
				Double max = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
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
					Double min = (Double) range.get(PERCENTILE_BUCKETS_MINIMUMSCORE);
					Double max = (Double) range.get(PERCENTILE_BUCKETS_MAXIMUMSCORE);
					Integer percent = ((Long) range.get(PERCENTILE_BUCKETS_PERCENTILE)).intValue();
					if (betweenBounds(score, min, max)) {
						percentile = percent;
						break;
					}
				}
			}
			
		}
		
		Integer integerScore = (int) (probability != null ? Math.round(probability * 100) : Math.round(score * 100));
		String modelName = modelIdMap.get(modelID);
		ModelEvaluationResult result = new ModelEvaluationResult(leadID, bucket, lift, modelName, percentile, probability, score, requestID, integerScore);
		log.info("result is " + result);
		return result;
		
	}
	
    private static boolean betweenBounds(double value, Double lowerInclusive, Double upperExclusive)
    {
        return (lowerInclusive == null || value >= lowerInclusive) && 
            (upperExclusive == null || value < upperExclusive);
    }
    
	public static void main(String[] args) throws Exception {

		String hdfs = "/user/s-analytics/customers/Nutanix/scoring/TestLeadsTable/scores" + "/44c1d69a-88ad-4e3f-8710-d7c736c5d32a.avro";
		List<GenericRecord> list = AvroUtils.getData(new Configuration(), new Path(hdfs));
		for (GenericRecord ele : list) {
			System.out.println(ele.toString());
		}
	}
}
