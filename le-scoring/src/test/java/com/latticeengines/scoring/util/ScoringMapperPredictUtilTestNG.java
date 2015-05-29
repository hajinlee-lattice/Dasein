package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringMapperPredictUtilTestNG extends ScoringFunctionalTestNGBase {
	
	private static final double EPS = 1e-6;
	private static final String modelID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";
	
	//@Test(groups = "funtional")
	public void compareFinalResults() {
		// read input lead files
		
		// trigger the scoring
		
		// compare the results
		// compareEvaluationResults()
	}
	
	// compare the results
	//@Test(groups = "unit")
	public void compareEvaluationResults() {
		boolean evaluationIsSame = false;

		List<GenericRecord> newlist = null;
		List<GenericRecord> oldlist = null;
		
		// load new scores from HDFS	
		newlist = loadHDFSAvroFiles(new Configuration(), 
				"/user/s-analytics/customers/Nutanix/scoring/ScoringCommandProcessorTestNG_LeadsTable/scores/");
				
		// load existing scores from testing file
	    URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/results/"
	    		+ "2Checkout_relaunch_Q_PLS_Scoring_Incremental_1336210_2015-05-15_005244-result-part-m-00000.avro");
		String fileName = url.getFile();
		System.out.println(fileName);
		oldlist = loadLocalAvroFiles(fileName);
	
		//compareJsonResults(str1, str2)
		evaluationIsSame = compareJsonResults(newlist, oldlist);
		assertTrue(evaluationIsSame);
		//return evaluationIsSame;
	}
	
	private  ArrayList<GenericRecord> loadHDFSAvroFiles(Configuration configuration, String hdfsDir) {
		ArrayList<GenericRecord> newlist = new ArrayList<GenericRecord>();
		List<String> files = null;
		try {
			files = HdfsUtils.getFilesForDir(configuration, hdfsDir);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(String file : files) {
			try {
				List<GenericRecord> list = AvroUtils.getData(configuration, new Path(file));
				newlist.addAll(list);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return newlist;
	}
	
	private  List<GenericRecord> loadLocalAvroFiles(String localDir) {
		List<GenericRecord> newlist = new ArrayList<GenericRecord>();
		File localAvroFile = new File(localDir);
		FileReader<GenericRecord> reader;
		GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<>();
        try {
			reader = DataFileReader.openReader(localAvroFile, fileReader);
            for (GenericRecord datum : reader) {
            	newlist.add(datum);
            }
		} catch (IOException e) {
			e.printStackTrace();
		}  
		return newlist;
	}
	
	public boolean compareJsonResults(List<GenericRecord> newResults, List<GenericRecord> oldResults) {
		boolean resultsAreSame = true;
		if (newResults.size() != oldResults.size()) {
			resultsAreSame = false;
		} else {
			HashMap<String, GenericRecord> resultMap = new HashMap<String, GenericRecord>();
			for (int i = 0; i < newResults.size(); i++) {
				GenericRecord newResult = newResults.get(i);
				String key = newResult.get("LeadID").toString() + newResult.get("Play_Display_Name").toString();
				resultMap.put(key, newResult);
			}
			
			for (int i = 0; i < oldResults.size(); i++) {
				GenericRecord oldResult = oldResults.get(i);
				String key = oldResult.get("LeadID").toString() + oldResult.get("Play_Display_Name").toString();
				if (!resultMap.containsKey(key)) {
					resultsAreSame = false;
					break;
				} else {
					if (!compareTwoRecord(resultMap.get(key), oldResult)) {
						resultsAreSame = false;
						break;
					}
				}
			}
		}
		return resultsAreSame;
	}
	
	private boolean compareTwoRecord(GenericRecord newRecord, GenericRecord oldRecord) {
		boolean recordsAreSame = true;
		// TODO based on Schema related methods
		String[] columns = {"Bucket_Display_Name", "Lift", "Percentile", "Probability", "RawScore", "Score"};
		for (String column : columns) {
			switch (column) {
				case "Bucket_Display_Name": 
					if (!newRecord.get(column).equals(oldRecord.get(column))) {
						System.out.println("come to the " + column);
						recordsAreSame = false;
					}
					break;
				case "Lift": 
					if (!newRecord.get(column).equals(oldRecord.get(column))) {
						System.out.println("come to the " + column);
						recordsAreSame = false;
					}
					break;
				case "Percentile": 
					if (!newRecord.get(column).equals(oldRecord.get(column))) {
						System.out.println("come to the " + column);
						recordsAreSame = false;
					}
					break;
				case "Probability": 
					if (!newRecord.get(column).equals(oldRecord.get(column))) {
						System.out.println("come to the " + column);
						recordsAreSame = false;
					}
					break;
				case "RawScore": 
					if (Math.abs((Double)newRecord.get(column)-(Double)oldRecord.get(column)) > EPS) {
						System.out.println("come to the " + column);
						recordsAreSame = false;
					}
					break;
				case "Score": 
					if (!newRecord.get(column).equals(oldRecord.get(column))) {
						System.out.println("come to the " + column);
						recordsAreSame = false;
					}	
					break;
			}
		}
		return recordsAreSame;
	}

	// compare the results
	//@Test(groups = "unit")
	public void testProcessScoreFiles() {
		// copy over the score.txt file to the current directory
		 URL scoreUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/results/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.jsonscoringoutputfile-0.txt");
		 File dest = new File(System.getProperty("user.dir") + "/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.jsonscoringoutputfile-0.txt");
		 try {
			FileUtils.copyURLToFile(scoreUrl, dest);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		// make up a leadInputRecordMap
		HashMap<String, ArrayList<String>> leadInputRecordMap = new HashMap<String, ArrayList<String>>();
		ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			list.add(i+"");
		}
		
		leadInputRecordMap.put(modelID, list);
		
		// parseModelFile
	    URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
		String fileName = url.getFile();
		Path path = new Path(fileName);
		HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
		ScoringMapperTransformUtil.parseModelFiles(models, path);
		
		// make up a modelIdMap
		HashMap<String, String> modelIdMap = new HashMap<String, String>();
		modelIdMap.put(modelID, modelID);
		
		ArrayList<ModelEvaluationResult> resultList = null;
		resultList = ScoringMapperPredictUtil.processScoreFiles(leadInputRecordMap, models, modelIdMap, 1000);
		
		ArrayList<ModelEvaluationResult> expectedResultList = new ArrayList<ModelEvaluationResult>();
		ModelEvaluationResult result1 = new ModelEvaluationResult("18f446f1-747b-461e-9160-c995c3876ed4", "Highest", 4.88519256666, modelID, 100, 0.05822784810126582, 0.0777755757027, 6);
		ModelEvaluationResult result2 = new ModelEvaluationResult("47358ca2-a549-4765-a7f7-a7637a565343", "Highest", 4.88519256666, modelID, 100, 0.05822784810126582, 0.0394015516631, 6);
		ModelEvaluationResult result3 = new ModelEvaluationResult("4821a01c-5a4c-4633-9122-5d050c064d43", "Highest", 2.65499596014, modelID, 98, 0.03164556962025317, 0.0267911548364, 3);
		ModelEvaluationResult result4 = new ModelEvaluationResult("50d2fcf4-3dbb-46cf-80a3-c1ac96106b07", "Highest", 2.12399676811, modelID, 95, 0.02531645569620253, 0.0242481348343, 3);
		ModelEvaluationResult result5 = new ModelEvaluationResult("510c48cd-4672-4b91-ad3b-3f904b100913", "High", 1.76999730676, modelID, 91, 0.02109704641350211, 0.022526096794, 2);
		ModelEvaluationResult result6 = new ModelEvaluationResult("936a6661-c745-4922-a32b-bde68ada894d", "High", 1.69919741449, modelID, 84, 0.020253164556962026, 0.0189333568863, 2);
		ModelEvaluationResult result7 = new ModelEvaluationResult("93d16654-72db-4ca5-adb5-64e12ef54215", "High", 1.5778261706, modelID, 81, 0.018806509945750453, 0.0175358841357, 2);
		ModelEvaluationResult result8 = new ModelEvaluationResult("baf39fe9-a184-4a83-9399-45208560dbe4", "High", 1.48679773768, modelID, 75, 0.017721518987341773, 0.0148266449465, 2);
		ModelEvaluationResult result9 = new ModelEvaluationResult("cd7de65c-b2af-42a5-85af-491cf8503747", "Medium", 1.15302681698, modelID, 65, 0.013743218806509945, 0.01244724204371, 1);
		ModelEvaluationResult result10 = new ModelEvaluationResult("fd6be1aa-95aa-45b2-adbb-3125a01acf84", "Medium", 1.06199838406, modelID, 62, 0.012658227848101266, 0.01185827291902, 1);
		expectedResultList.add(result1);
		expectedResultList.add(result2);
		expectedResultList.add(result3);
		expectedResultList.add(result4);
		expectedResultList.add(result5);
		expectedResultList.add(result6);
		expectedResultList.add(result7);
		expectedResultList.add(result8);
		expectedResultList.add(result9);
		expectedResultList.add(result10);
		assertTrue(resultListsAreSame(expectedResultList, resultList));
		
		// delete the score.txt file to the current directory
		dest.delete();
	}
	
	private boolean resultListsAreSame(ArrayList<ModelEvaluationResult> list1, ArrayList<ModelEvaluationResult> list2) {
		boolean isSame = true;
		for (int i = 0; i < list1.size(); i++) {
			ModelEvaluationResult result1 = list1.get(i);
			boolean hasMatchingResult = false;
			for (int j = 0; j < list2.size(); j++) {
				ModelEvaluationResult result2 = list2.get(j);
				if (resultIsSame(result1, result2)) {
					hasMatchingResult = true;
					break;
				}
			}
			if (!hasMatchingResult) {
				System.out.println(result1 + " does not have matching result");
				isSame = false;
				break;
			}
		}
		return isSame;
	}	

	@Test(groups = "unit")
	public void testWriteToOutputFile() throws IllegalArgumentException, Exception {
		ArrayList<ModelEvaluationResult> expectedResultList = new ArrayList<ModelEvaluationResult>();
		ModelEvaluationResult result1 = new ModelEvaluationResult("18f446f1-747b-461e-9160-c995c3876ed4", "Highest", 4.88519256666, modelID, 100, 0.05822784810126582, 0.0777755757027, 6);
		ModelEvaluationResult result2 = new ModelEvaluationResult("47358ca2-a549-4765-a7f7-a7637a565343", "Highest", 4.88519256666, modelID, 100, 0.05822784810126582, 0.0394015516631, 6);
		ModelEvaluationResult result3 = new ModelEvaluationResult("4821a01c-5a4c-4633-9122-5d050c064d43", "Highest", 2.65499596014, modelID, 98, 0.03164556962025317, 0.0267911548364, 3);
		ModelEvaluationResult result4 = new ModelEvaluationResult("50d2fcf4-3dbb-46cf-80a3-c1ac96106b07", "Highest", 2.12399676811, modelID, 95, 0.02531645569620253, 0.0242481348343, 3);
		ModelEvaluationResult result5 = new ModelEvaluationResult("510c48cd-4672-4b91-ad3b-3f904b100913", "High", 1.76999730676, modelID, 91, 0.02109704641350211, 0.022526096794, 2);
		expectedResultList.add(result1);
		expectedResultList.add(result2);
		expectedResultList.add(result3);
		expectedResultList.add(result4);
		expectedResultList.add(result5);
		
		// create a temp folder 
		String tempOutputPath = "/user/s-analytics/customers/ScoringMapperPredictUtilTestNG/scoring";
		try {
			if (HdfsUtils.fileExists(new Configuration(), tempOutputPath)) {
				HdfsUtils.rmdir(new Configuration(), tempOutputPath);
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
			
		ScoringMapperPredictUtil.writeToOutputFile(expectedResultList, new Configuration(), tempOutputPath);
		
		// Deserialize 
        FileSystem fs = FileSystem.get(new Configuration());
        List<String> fileList = null;
        try {
			fileList = HdfsUtils.getFilesForDir(new Configuration(), tempOutputPath);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
        assertTrue(fileList.size()== 1, "The fileList should only have one element.");
        InputStream is = fs.open(new Path(fileList.get(0)));
        File file = new File("temp.avro");
        FileUtils.copyInputStreamToFile(is, file);
        
        
        SpecificDatumReader<ModelEvaluationResult> reader = new SpecificDatumReader<ModelEvaluationResult>(ModelEvaluationResult.class);
		DataFileReader<ModelEvaluationResult> dataFileReader = new DataFileReader<ModelEvaluationResult>(file, reader);
		ArrayList<ModelEvaluationResult> generatedResultList = new ArrayList<ModelEvaluationResult>();
		ModelEvaluationResult result = null;
		System.out.println("print out the ModelEvaluationResults");
		while (dataFileReader.hasNext()) {
			result = dataFileReader.next();
			System.out.println(result);
			generatedResultList.add(result);
		}
		assertTrue(expectedResultList.size() == generatedResultList.size(), "The resultLists should have the same size.");
		assertTrue(resultListsAreSame(expectedResultList, generatedResultList), "The resultLists should have be the same.");
		
		// delete the temp folder and the temp file
		dataFileReader.close();
		try {
			HdfsUtils.rmdir(new Configuration(), tempOutputPath);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private boolean resultIsSame(ModelEvaluationResult result1, ModelEvaluationResult result2) {
		double eps = 1e-6;
		boolean isSame = true;
		if (!compareTwoCharSequences(result1.getLeadID(), result2.getLeadID())) {
			isSame = false;
		} 
		if (!compareTwoCharSequences(result1.getBucketDisplayName(), result2.getBucketDisplayName())) {
			isSame = false;
		} 
		if ((result1.getLift() - result2.getLift()) >= eps ) {
			isSame = false;
		} 
		if (!compareTwoCharSequences(result1.getPlayDisplayName(), result2.getPlayDisplayName())) {
			isSame = false;
		} 
		if (result1.getPercentile() != result2.getPercentile()) {
			isSame = false;
		} 
		if ((result1.getProbability() - result2.getProbability()) >= eps ) {
			isSame = false;
		} 
		if ((result1.getRawScore() - result2.getRawScore()) >= eps ) {
			isSame = false;
		} 
		if (result1.getScore() != result2.getScore()) {
			isSame = false;
		} 
		return isSame;
	}
	
	private boolean compareTwoCharSequences(CharSequence charSequence1, CharSequence charSequence2) {
		boolean same = true;
		if (charSequence1.length() != charSequence2.length()) {
			same = false;
		} else {
			for (int i = 0; i < charSequence1.length(); i++) {
				if (charSequence1.charAt(i) != charSequence1.charAt(i)) {
						same = false;
				}
			}
		}
		return same;
	}

	
	
}
