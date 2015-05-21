package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.util.ScoringMapperTransformUtil;

public class ScoringMapperTransformUtilTestNG {
	
    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
	private final static String DATA_PATH = "com/latticeengines/scoring/data/";
	private final static String MODEL_SUPPORTED_FILE_PATH = "com/latticeengines/scoring/models/supportedFiles/";
	private final static String MODEL_ID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";
	private final static String TEST_RECORD = "{\"LeadID\": \"837394\", \"ModelingID\": 113880, \"PercentileModel\": null, "
			+ "\"FundingFiscalYear\": 123456789, \"BusinessFirmographicsParentEmployees\": 24, \"C_Job_Role1\": \"\", "
			+ "\"BusinessSocialPresence\": \"True\", \"Model_GUID\": \"2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json\"}";

	//@Test(groups = "unit")
	public void testParseDatatypeFile() throws IOException {
	    URL url = ClassLoader.getSystemResource(DATA_PATH + "datatype.avsc");
		String fileName = url.getFile();
		Path path = new Path(fileName);
		JSONObject datatypeObj = ScoringMapperTransformUtil.parseDatatypeFile(path);
		assertTrue(datatypeObj.size() == 7);
		assertTrue(datatypeObj.get("ModelingID").equals(new Long(1)));
	}
	
	//@Test(groups = "unit")
	public void testParseModelFiles() {
		String[] targetFiles = {"encoder.py", "pipeline.py", "pipelinefwk.py", "pipelinesteps.py", "scoringengine.py", "STPipelineBinary.p"};
	    URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
		String fileName = url.getFile();
		Path path = new Path(fileName);
		
		HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
		ScoringMapperTransformUtil.parseModelFiles(models, path);
		assertTrue(models.size() == 1);
		//assertTrue(models.containsRightContents("2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json"));
		
		for (int i = 0; i < targetFiles.length; i++) {
			System.out.println("Current target file is " + targetFiles[i]);
			assertTrue(compareFiles(targetFiles[i]));
		}
	
	}
	
	private boolean compareFiles(String fileName) {
		boolean filesAreSame = false;
		File newFile = new File(MODEL_ID + fileName);		
		URL url = ClassLoader.getSystemResource(MODEL_SUPPORTED_FILE_PATH + fileName);			
		File oldFile = new File(url.getFile());
		filesAreSame = compareFilesLineByLine(newFile, oldFile);
		return filesAreSame;
	}
	
	private boolean compareFilesLineByLine(File file1, File file2) {
		boolean filesAreSame = true;
		try {
			LineNumberReader reader1 = new LineNumberReader(new FileReader(file1));
			LineNumberReader reader2 = new LineNumberReader(new FileReader(file2));
	        String line1 = reader1.readLine();
	        String line2 = reader2.readLine();
	        while (line1 != null && line2 != null)
	        {
	            if (!line1.equals(line2))
	            {
	            	System.out.println("File \"" + file1 + "\" and file \"" +
	            			file2 + "\" differ at line " + reader1.getLineNumber() +
	                        ":" + "\n" + line1 + "\n" + line2);
	            	filesAreSame = false;
	            	break;
	            }
	            line1 = reader1.readLine();
	            line2 = reader2.readLine();
	        }
	        if ((line1 == null && line2 != null) || (line1 != null && line2 == null)) {
	        	filesAreSame = false;
	        }
		} catch (IOException e) {
			e.printStackTrace();
		}
		return filesAreSame;
	}
	
	//@Test(groups = "unit")
	public void testManipulateLeadFile() {
		
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
    	HashMap<String, ArrayList<String>> leadInputRecordMap = new HashMap<String, ArrayList<String>>();
    	HashMap<String, String> modelIdMap = new HashMap<String, String>();
    	
	    URL url = ClassLoader.getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
		String fileName = url.getFile();
		Path path = new Path(fileName);
    	ScoringMapperTransformUtil.parseModelFiles(models, path);
		ScoringMapperTransformUtil.manipulateLeadFile(leadInputRecordMap, models, modelIdMap, TEST_RECORD);
		
		String modelID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";
		ArrayList<String> recordList = leadInputRecordMap.get(modelID);
		System.out.println("haha");
		assertTrue(leadInputRecordMap.size() == 1);
		System.out.println("jaja");
		assertTrue(recordList.size() == 1);
		System.out.println("hoho");
		String record = recordList.get(0);
		JSONParser parser = new JSONParser();
		try {
			JSONObject j = (JSONObject) parser.parse(record);
			assertTrue(j.get("key").equals("837394"));
			System.out.println("lele");
			JSONArray arr = (JSONArray) j.get("value"); 
			// model.json file has 194 columns for metadata
			assertTrue(arr.size() == 194);
			System.out.println("hehe");
			assertTrue(containsRightContents(arr));
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	private boolean containsRightContents(JSONArray arr) {
		boolean result = true;
		for (int i = 0; i < arr.size() && result; i++) {
			System.out.println("the current index is " + i);
			JSONObject obj = (JSONObject) arr.get(i);
			String key = (String)obj.get("Key");
			switch (key) {
				case "PercentileModel":
					if (!((String)((JSONObject)obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("String|")) {
						result = false;
					} 
					break;
				case "FundingFiscalYear":
					if (!((String)((JSONObject)obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("Float|'123456789'")) {
						result = false;
					} 
					break;
				case "BusinessFirmographicsParentEmployees":
					if (!((String)((JSONObject)obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("Float|'24'")) {
						result = false;
					} 
					break;
				case "C_Job_Role1":
					if (!((String)((JSONObject)obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("String|''")) {
						result = false;
					} 
					break;
				case "BusinessSocialPresence":
					if (!((String)((JSONObject)obj.get("Value")).get(LEAD_SERIALIZE_TYPE_KEY)).equals("String|'True'")) {
						result = false;
					} 
					break;
				default:
						break;				
			}
		}
		return result;
	}
	
}
