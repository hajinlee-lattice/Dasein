package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperTransformUtil {
	
	private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

	//TODO
	private static final String absolutePath = "/Users/ygao/test/e2e/";
	
    //private static final int THRESHOLD = 10000;
    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    private static final String LEAD_RECORD_LEAD_ID_COLUMN = "LeadID";
    private static final String LEAD_RECORD_MODEL_ID_COLUMN = "Play_Display_Name";
    private static final String LEAD_RECORD_REQUEST_ID_COLUMN = "Request_ID";
    private static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    private static final String MODEL = "Model";
    private static final String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    private static final String MODEL_SCRIPT = "Script";
    private static final String SCORING_SCRIPT_NAME = "scoringengine.py";
    
    public static void parseModelFiles(HashMap<String, JSONObject> models, Path path) {
		try {
			log.info("come to the parseModelFiles function");
	        FileReader reader;
			reader = new FileReader(path.toString());
	        JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
            // use the GUID to identify a model
            String modelID = path.getName();
            models.put(modelID, jsonObject);
            decodeSupportedFiles(modelID, (JSONObject)jsonObject.get(MODEL));
            writeScoringScript(modelID, (JSONObject)jsonObject.get(MODEL));
            log.info("length is " + models.size());
            log.info("modelName is " + jsonObject.get("Name"));
		} catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ParseException ex) {
            ex.printStackTrace();
        } catch (NullPointerException ex) {
            ex.printStackTrace();
        }
    }
    
    public static JSONObject parseDatatypeFile(Path path) {
    	JSONObject datatypeObject = null;
    	String fileName = path.getName();
    	String content = null;
		try {
			content = FileUtils.readFileToString(new File(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	JSONParser jsonParser = new JSONParser();
    	try {
			datatypeObject = (JSONObject) jsonParser.parse(content);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	return datatypeObject;
    }
    
	public static void writeScoringScript(String modelID, JSONObject modelObject) {
		String scriptContent = (String) modelObject.get(MODEL_SCRIPT);
		//String absolutePath = "/Users/ygao/Documents/workspace/ledp/le-dataplatform/src/test/python/";
		//String fileName = absolutePath + modelID + SCORING_SCRIPT_NAME;
		String fileName = modelID + SCORING_SCRIPT_NAME;
		try {
			File file = new File(fileName);
			FileUtils.writeStringToFile(file, scriptContent);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    			
	private static void decodeSupportedFiles(String modelID, JSONObject modelObject) {
		JSONArray compressedSupportedFiles = (JSONArray) modelObject.get(MODEL_COMPRESSED_SUPPORT_Files);
		for (int i = 0; i < compressedSupportedFiles.size(); i++) {
			JSONObject compressedFile = (JSONObject) compressedSupportedFiles.get(i);
			//String absolutePath = "/Users/ygao/Documents/workspace/ledp/le-dataplatform/src/test/python/";
			//String compressedFileName = absolutePath + modelID + compressedFile.get("Key");
			String compressedFileName = modelID + compressedFile.get("Key");
			decodeBase64ThenDecompressToFile((String)compressedFile.get("Value"), compressedFileName);
		}
		
	}
	
	private static void decodeBase64ThenDecompressToFile(String value, String fileName)
	{
		FileOutputStream stream;
		try {
			stream = new FileOutputStream(fileName);
			GZIPInputStream gzis = 
		    		new GZIPInputStream(new Base64InputStream(IOUtils.toInputStream(value)));
			IOUtils.copy(gzis, stream);
	        gzis.close();
	        stream.close();
	    } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
	public static void writeModelsToFile(HashMap<String, JSONObject> models, String filePath) {
		try {
			File file = new File(filePath);
			if (file.exists()) {
				file.delete();
			}
			BufferedWriter bw = new BufferedWriter (new FileWriter(filePath, true));
			Set<String> modelIDSet = models.keySet();
			for (String modelID : modelIDSet) {
				bw.write(modelID);
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    public static void  manipulateLeadFile(HashMap<String, ArrayList<String>> leadInputRecordMap, HashMap<String, JSONObject> models, HashMap<String, Integer> leadNumber, String[] requestID, String record) {
    	JSONParser jsonParser = new JSONParser();
    	JSONObject leadJsonObject = null;
		try {
			leadJsonObject = (JSONObject) jsonParser.parse(record);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	String modelID = identifyModelID(leadJsonObject, models);
    	String formattedRecord = transformToJsonString(leadJsonObject, models, leadNumber, requestID, modelID);
    	if (leadInputRecordMap.containsKey(modelID)) {
    		leadInputRecordMap.get(modelID).add(formattedRecord);
    	} else {
    		ArrayList<String> leadInput = new ArrayList<String>();
    		leadInput.add(formattedRecord);
    		leadInputRecordMap.put(modelID, leadInput);
    	}
    }
    
    private static String identifyModelID(JSONObject leadJsonObject, HashMap<String, JSONObject> models) {
    	
		// TODO unify this with Haitao about the columnName of ModelID, and also the type of the ModelID
		//String modelIDVal= (String) leadJsonObject.get(LEAD_RECORD_MODEL_ID_COLUMN);
		//debugging
		String modelIDVal= "87ecf8cd-fe45-45f7-89d1-612235631fc1";
		String modelID = null;
    	Set<String> modelIDSet = models.keySet();
    	for (String ID : modelIDSet) {
    		if (modelIDVal.contains(ID)) {
    			modelID = ID;
    			break;
    		}
    	}
    	if (modelID == null) {
    		new Exception("ModelID in avro files do not match any of the models");
    	}
    	return modelID;
    }
    
    public static String transformToJsonString(JSONObject leadJsonObject, HashMap<String, JSONObject> models, HashMap<String, Integer> leadNumber, String[] requestID, String modelID) {
    	
    	String formattedRecord = null;
		//get the metadata from the specific model json file
		JSONArray metadata = (JSONArray) models.get(modelID).get(INPUT_COLUMN_METADATA);
		//log.info("metadata is " + metadata.toString());
		
		// parse the avro file since it is in json format
		//TODO unify with Haitao about the name of the columnID and the type
		JSONObject jsonObj = new JSONObject();
		String leadID = (String) leadJsonObject.get(LEAD_RECORD_LEAD_ID_COLUMN);
		if (!leadNumber.containsKey(leadID)) {
			leadNumber.put(leadID, 1);
		} else {
			int i = leadNumber.get(leadID);
			leadNumber.put(leadID, ++i);
		}
		//log.info("The lead id is " + leadID);
		//String currentRequestID = (String) leadJsonObject.get(LEAD_RECORD_REQUEST_ID_COLUMN);
		String currentRequestID = "default";
		//log.info("The lead id is " + currentRequestID);
		if (requestID[0] == null) {
			requestID[0] = currentRequestID;
		} else {
			if (requestID[0] != currentRequestID) {
				new Exception("requestID does not match");
			}
		}
		JSONArray jsonArray = new JSONArray();  
		jsonObj.put("value", jsonArray); 
		jsonObj.put("key", leadID);
		
		Set<String> keySet = leadJsonObject.keySet();
		for (int i = 0; i < metadata.size(); i++) {
			JSONObject columnObj = new JSONObject();
			JSONObject serializedValueAndTypeObj = new JSONObject();
			columnObj.put("Value", serializedValueAndTypeObj);
			String type = null;
			//get key
			JSONObject obj = (JSONObject) metadata.get(i);
			String key = (String) obj.get("Name");
			columnObj.put("Key", key);
			type = (Long) obj.get("ValueType") == 0 ? "Float" : "String";
			String typeAndValue = type + "|\'" + leadJsonObject.get(key) + "\'";
			serializedValueAndTypeObj.put(LEAD_SERIALIZE_TYPE_KEY, typeAndValue);		
			jsonArray.add(columnObj);
		}
		formattedRecord = jsonObj.toString() + "\n";
		return formattedRecord;
    }
    
    public static void writeToLeadInputFiles(HashMap<String, ArrayList<String>> leadInputRecordMap, int threshold) {
    	if (leadInputRecordMap == null) {
    		new Exception("leadInputRecordMap is null");
    	}
     	Set<String> modelIDs = leadInputRecordMap.keySet();
    	for (String modelID : modelIDs) {
    		writeToLeadInputFile(leadInputRecordMap.get(modelID), modelID, threshold);
    	}
    }
    
    private static void writeToLeadInputFile(ArrayList<String> leadInputRecords, String modelID, int threshold) {
    	log.info("debugging writeToLeadInputFile");
    	log.info("for model " + modelID + ", there are " + leadInputRecords.size() + " leads");
    	
    	int indexOfFile = 0;
    	int count = 0;
    	//create an intial input stream
    	try {
	    	String leadInputFileName = modelID + "-" + indexOfFile;
	    	log.info("Filename is " + leadInputFileName);
			File file = new File(leadInputFileName);
			BufferedWriter bw = null;
			bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
	    	for (int i = 0; i < leadInputRecords.size(); i++) {
	    		count++;
	    		bw.write(leadInputRecords.get(i));
	    		if (count == threshold) {
	    			// reach the point of writing the current bw to file
	    			bw.flush();
	    			bw.close();
	    			count = 0;
	    			indexOfFile++;
	    			leadInputFileName = modelID + "-" + indexOfFile;
	    	    	log.info("Filename is " + leadInputFileName);
	    			file = new File(leadInputFileName);
	    			bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
	    		}
	    	}
	    	if (count != 0) {
				bw.flush();
				bw.close();
	    	}
    	} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
}
