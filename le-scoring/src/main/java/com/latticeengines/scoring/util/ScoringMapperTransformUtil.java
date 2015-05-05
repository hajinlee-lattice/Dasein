package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
    private static final String LEAD_RECORD_MODEL_ID_COLUMN = "ModelID";
    private static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    private static final String MODEL = "Model";
    private static final String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    private static final String MODEL_SCRIPT = "Script";
    private static final String SCORING_SCRIPT_NAME = "scoringengine.py";
    
    public static void parseModelFiles(HashMap<String, JSONObject> models, Path path, HashMap<String, Integer> modelNumberMap) {
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
            modelNumberMap.put(modelID, 0);
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
	
    public static void manipulateLeadFile(HashMap<String, Integer> modelNumberMap, String record, HashMap<String, JSONObject> models, int threshold) {
    	// find the column which contains the modelID   	
    	try {
    		JSONParser jsonParser = new JSONParser();
			JSONObject leadJsonObject = (JSONObject) jsonParser.parse(record);
			// TODO unify this with Haitao about the columnName of ModelID, and also the type of the ModelID
			//String modelIDVal= (String) leadJsonObject.get(LEAD_RECORD_MODEL_ID_COLUMN);
			//debugging
			String modelIDVal= "87ecf8cd-fe45-45f7-89d1-612235631fc1";
			String modelID = identifyModelID(modelIDVal, modelNumberMap);
			int currentNum = modelNumberMap.get(modelID);
			modelNumberMap.put(modelID, ++currentNum);
			String leadInputFileName = modelID + "-" + (currentNum/threshold);
			//debug
			//String absolutePath = "/Users/ygao/Documents/workspace/ledp/le-dataplatform/src/test/python/";
			//leadInputFileName = absolutePath +leadInputFileName;
			
			//log.info("leadInputFileName name is " + leadInputFileName);
			File file = new File(leadInputFileName);
			if (!file.exists()) {
					file.createNewFile();
			}		
			//get the metadata from the specific model json file
			JSONArray metadata = (JSONArray) models.get(modelID).get(INPUT_COLUMN_METADATA);
			//log.info("metadata is " + metadata.toString());
			transformJsonFile(file, leadJsonObject, metadata);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
    }
    
    private static String identifyModelID(String modelIDVal, HashMap<String, Integer> modelNumberMap) {
    	String modelID = null;
    	Set<String> modelIDSet = modelNumberMap.keySet();
    	for (String ID : modelIDSet) {
    		if (modelIDVal.contains(ID)) {
    			modelID = ID;
    			//log.info("Find the model " + ID);
    			break;
    		}
    	}
    	if (modelID == null) {
    		new Exception("ModelID in avro files do not match any of the models");
    	}
    	return modelID;
    }
    
    private static void transformJsonFile(File file, JSONObject leadJsonObject, JSONArray metadata) {
    	
    	FileWriter fw;
		try {
			fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			// parse the avro file since it is in json format
			//TODO unify with Haitao about the name of the columnID and the type
			JSONObject jsonObj = new JSONObject();
			String columnID = (String) leadJsonObject.get(LEAD_RECORD_LEAD_ID_COLUMN);
			//log.info("The lead comlumn id is " + columnID);
			JSONArray jsonArray = new JSONArray();  
			jsonObj.put("value", jsonArray); 
			jsonObj.put("key", columnID); 
			
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
			
			//log.info("The file it writes to is " + jsonObj.toString());
			bw.write(jsonObj.toString() + "\n");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

    }
	
}
