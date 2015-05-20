package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
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

import static java.nio.file.StandardCopyOption.*;

import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperTransformUtil {
	
	private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);
	
    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    private static final String LEAD_RECORD_LEAD_ID_COLUMN = "LeadID";
    private static final String LEAD_RECORD_MODEL_ID_COLUMN = "model_ID";
    private static final String LEAD_RECORD_REQUEST_ID_COLUMN = "request_ID";
    private static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    private static final String MODEL = "Model";
    private static final String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    private static final String MODEL_SCRIPT = "Script";
    private static final String SCORING_SCRIPT_NAME = "scoringengine.py";
    
    // debug for the e2e
    private static final String absolutePath = "/Users/ygao/test/e2e/";
    
    public static void parseModelFiles(HashMap<String, JSONObject> models, Path path) {
		try {
	        FileReader reader;
			reader = new FileReader(path.toString());
	        JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
            // use the GUID to identify a model.  It is a contact that when mapper localizes the model, it changes its name to be the 
            // modelGUID
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
    	String content = null;
		try {
			content = FileUtils.readFileToString(new File(path.toString()));
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
		
		// debug for the e2e
		String fileName = modelID + SCORING_SCRIPT_NAME;
		//String fileName = absolutePath + modelID + SCORING_SCRIPT_NAME;
		log.info("fileName is " + fileName);
		try {
			File file = new File(fileName);
			FileUtils.writeStringToFile(file, scriptContent);
			
			File dest = new File(absolutePath+fileName);
			log.info("Trying to write scoring script file to e2e folder, and the fileName is " + fileName);
			Files.copy(file.toPath(), dest.toPath(), REPLACE_EXISTING);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    			
	private static void decodeSupportedFiles(String modelID, JSONObject modelObject) {
		JSONArray compressedSupportedFiles = (JSONArray) modelObject.get(MODEL_COMPRESSED_SUPPORT_Files);
		for (int i = 0; i < compressedSupportedFiles.size(); i++) {
			JSONObject compressedFile = (JSONObject) compressedSupportedFiles.get(i);

			// debug for the e2e
			String compressedFileName = modelID + compressedFile.get("Key");
			//String compressedFileName = absolutePath + modelID + compressedFile.get("Key");
			log.info("compressedFileName is " + compressedFileName);
			decodeBase64ThenDecompressToFile((String)compressedFile.get("Value"), compressedFileName);
		}
		
	}
	
	private static void decodeBase64ThenDecompressToFile(String value, String fileName)
	{
		FileOutputStream stream;
		try {
			stream = new FileOutputStream(fileName);
			InputStream gzis = 
		    		new GZIPInputStream(new Base64InputStream(IOUtils.toInputStream(value)));
			IOUtils.copy(gzis, stream);
	        gzis.close();
	        stream.close();
			// debug for the e2e
			File dest = new File(absolutePath+fileName);
			File file = new File(fileName);
			log.info("Trying to decompress file to e2e folder, and the fileName is " + fileName);
			log.info("Trying to decompress file to e2e folder, and the dest is " + dest);
			Files.copy(file.toPath(), dest.toPath(), REPLACE_EXISTING);
	        
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
    
    public static void  manipulateLeadFile(HashMap<String, ArrayList<String>> leadInputRecordMap, HashMap<String, JSONObject> models, HashMap<String, String> modelIdMap, HashMap<String, Integer> leadNumber, String record) {
    	JSONParser jsonParser = new JSONParser();
    	JSONObject leadJsonObject = null;
		try {
			leadJsonObject = (JSONObject) jsonParser.parse(record);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	String modelID = identifyModelID(leadJsonObject, models, modelIdMap);
    	log.info("after identifying, the modelID is " + modelID);
    	
    	// debug for the e2e
    	String leadID = (String)leadJsonObject.get("LeadID");
    	//if (!leadID.equals("1006549")) return;
    	
    	String formattedRecord = transformToJsonString(leadJsonObject, models, leadNumber, modelID);
    	if (leadInputRecordMap.containsKey(modelID)) {
    		leadInputRecordMap.get(modelID).add(formattedRecord);
    	} else {
    		ArrayList<String> leadInput = new ArrayList<String>();
    		leadInput.add(formattedRecord);
    		leadInputRecordMap.put(modelID, leadInput);
    	}
    }
    
    private static String identifyModelID(JSONObject leadJsonObject, HashMap<String, JSONObject> models, HashMap<String, String> modelIdMap) {
    	
		String modelIDVal= (String) leadJsonObject.get(LEAD_RECORD_MODEL_ID_COLUMN);
		log.info("the modelVal is " + modelIDVal);
		String modelID = null;
    	Set<String> modelIDSet = models.keySet();

    	for (String ID : modelIDSet) {
    		if (modelIDVal.contains(ID)) {
    			modelID = ID;
    			log.info("modelID is found! " + modelID);
    			break;
    		}
    	}
    	if (modelID == null) {
    		new Exception("ModelID in avro files do not match any of the models");
    	}
    	//update the mapping from model GUID to model name
    	if (!modelIdMap.containsKey(modelID)) {
    		modelIdMap.put(modelID, modelIDVal);
    	}
    	return modelID;
    }
    
    public static String transformToJsonString(JSONObject leadJsonObject, HashMap<String, JSONObject> models, HashMap<String, Integer> leadNumber, String modelID) {
    	String formattedRecord = null;
    	
    	if (models == null) {
    		new Exception("model is null");
    	} else if (models.get(modelID) == null) {
    		new Exception("models.get(modelID) is null");
    	} else if (models.get(modelID).get(INPUT_COLUMN_METADATA) == null) {
    		new Exception("models.get(modelID).get(INPUT_COLUMN_METADATA) is null");
    	}
    	
		JSONArray metadata = (JSONArray) models.get(modelID).get(INPUT_COLUMN_METADATA);
		
		// parse the avro file since it is in json format
		JSONObject jsonObj = new JSONObject();
		String leadID = (String) leadJsonObject.get(LEAD_RECORD_LEAD_ID_COLUMN);
		if (!leadNumber.containsKey(leadID)) {
			leadNumber.put(leadID, 1);
		} else {
			int i = leadNumber.get(leadID);
			leadNumber.put(leadID, ++i);
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
			// should treat sqoop null as empty
			String typeAndValue = "";
			if (leadJsonObject.get(key) != null) {
				String value = leadJsonObject.get(key).toString();
				typeAndValue = String.format("%s|\'%s\'", type, value);
			} else {
				typeAndValue = String.format("%s|", type);
			}
			serializedValueAndTypeObj.put(LEAD_SERIALIZE_TYPE_KEY, typeAndValue);		
			jsonArray.add(columnObj);
		}
		formattedRecord = jsonObj.toString() + "\n";
		// debug for the e2e
		log.info("The formattedRecord is " + formattedRecord);
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
    	log.info("for model " + modelID + ", there are " + leadInputRecords.size() + " leads");
    	
    	int indexOfFile = 0;
    	int count = 0;
    	try {
			// debug for the e2e
    		String leadInputFileName = modelID + "-" + indexOfFile;
	    	//String leadInputFileName = absolutePath + modelID + "-" + indexOfFile;
	    	log.info("Filename is " + leadInputFileName);
			File file = new File(leadInputFileName);
			BufferedWriter bw = null;
			bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
	    	for (int i = 0; i < leadInputRecords.size(); i++) {
	    		count++;
	    		bw.write(leadInputRecords.get(i));
	    		if (count == threshold) {
	    			bw.flush();
	    			bw.close();
	    			count = 0;
	    			indexOfFile++;
	    			// debug for the e2e
	    			File dest = new File(absolutePath+leadInputFileName);
	    			Files.copy(file.toPath(), dest.toPath(), REPLACE_EXISTING);
	    			
	    			leadInputFileName = modelID + "-" + indexOfFile;
	    			//leadInputFileName = absolutePath + modelID + "-" + indexOfFile;
	    	    	log.info("Filename is " + leadInputFileName);
	    			file = new File(leadInputFileName);
	    			bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
	    		}
	    	}
	    	if (count != 0) {
				bw.flush();
				bw.close();
				// debug for the e2e
    			File dest = new File(absolutePath+leadInputFileName);
    			Files.copy(file.toPath(), dest.toPath(), REPLACE_EXISTING);
	    	}
    	} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
	public static void main(String[] args) throws Exception {
		String type = "Float";
		String value = "123.00";
		
		String typeAndValue = type + "|\'" + value + "\'";
		String trpeAndValue2 = String.format("%s|\'%s\'", type, value);
		if (typeAndValue.equals(trpeAndValue2)) {
			System.out.println("jaja");
		}
		
		
//		HashMap<String, ArrayList<String>> leadInputRecordMap = new HashMap<String, ArrayList<String>>();
//		ArrayList<String> records1 = new ArrayList<String>();
//		records1.add("value11\n");
//		records1.add("value12\n");
//		records1.add("value13\n");
//		leadInputRecordMap.put("model1", records1);
//		ArrayList<String> records2 = new ArrayList<String>();
//		records2.add("value21\n");
//		records2.add("value22\n");
//		records2.add("value23\n");
//		leadInputRecordMap.put("model2", records2);
//		writeToLeadInputFiles(leadInputRecordMap, 2);
	}
	
}
