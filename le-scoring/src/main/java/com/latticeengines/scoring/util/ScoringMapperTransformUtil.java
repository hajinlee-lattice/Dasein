package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperTransformUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    private static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    private static final String LEAD_RECORD_LEAD_ID_COLUMN = "LeadID";
    private static final String LEAD_RECORD_MODEL_ID_COLUMN = "Model_GUID";
    private static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    private static final String MODEL = "Model";
    private static final String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    private static final String MODEL_SCRIPT = "Script";
    private static final String SCORING_SCRIPT_NAME = "scoringengine.py";

    public static HashSet<String> preprocessLeads(ArrayList<String> leadList) throws ParseException, IOException,
            InterruptedException {

        HashSet<String> toReturn = new HashSet<String>();
        HashSet<String> leadAndModelHash = new HashSet<String>();
        JSONParser jsonParser = new JSONParser();
        JSONObject leadJsonObject = null;

        for (String lead : leadList) {
            leadJsonObject = (JSONObject) jsonParser.parse(lead);
            String leadId = String.valueOf(leadJsonObject.get(LEAD_RECORD_LEAD_ID_COLUMN));
            String modelId = String.valueOf(leadJsonObject.get(LEAD_RECORD_MODEL_ID_COLUMN));
            if (leadId == null) {
                throw new LedpException(LedpCode.LEDP_20003);
            }
            if (modelId == null) {
                throw new LedpException(LedpCode.LEDP_20004);
            }
            String key = leadId + modelId;
            if (leadAndModelHash.contains(key)) {
                throw new LedpException(LedpCode.LEDP_20005, new String[] { leadId, modelId });
            } else {
                leadAndModelHash.add(leadId + modelId);
            }
            toReturn.add(modelId);
        }
        return toReturn;
    }

    public static void parseModelFiles(HashMap<String, JSONObject> models, Path path) throws IOException, ParseException {

        FileReader reader;
        reader = new FileReader(path.toString());
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
        // use the GUID to identify a model. It is a contact that when 
        // mapper localizes the model, it changes its name to be the modelGUID
        String modelGuid = path.getName();
        models.put(modelGuid, jsonObject);
        decodeSupportedFiles(modelGuid, (JSONObject) jsonObject.get(MODEL));
        writeScoringScript(modelGuid, (JSONObject) jsonObject.get(MODEL));
        log.info("length is " + models.size());
        log.info("modelName is " + jsonObject.get("Name"));
    }

    public static JSONObject parseDatatypeFile(Path path) throws IOException, ParseException {

        JSONObject datatypeObject = null;
        String content = null;
        content = FileUtils.readFileToString(new File(path.toString()));
        JSONParser jsonParser = new JSONParser();
        datatypeObject = (JSONObject) jsonParser.parse(content);
        return datatypeObject;
    }

    public static void writeScoringScript(String modelGuid, JSONObject modelObject) throws IOException {

        String scriptContent = String.valueOf(modelObject.get(MODEL_SCRIPT));
        String fileName = modelGuid + SCORING_SCRIPT_NAME;
        log.info("fileName is " + fileName);
        File file = new File(fileName);
        FileUtils.writeStringToFile(file, scriptContent);
    }

    private static void decodeSupportedFiles(String modelGuid, JSONObject modelObject) throws IOException {
        
        JSONArray compressedSupportedFiles = (JSONArray) modelObject.get(MODEL_COMPRESSED_SUPPORT_Files);
        for (int i = 0; i < compressedSupportedFiles.size(); i++) {
            JSONObject compressedFile = (JSONObject) compressedSupportedFiles.get(i);
            String compressedFileName = modelGuid + compressedFile.get("Key");
            log.info("compressedFileName is " + compressedFileName);
            decodeBase64ThenDecompressToFile(String.valueOf(compressedFile.get("Value")), compressedFileName);
        }

    }

    private static void decodeBase64ThenDecompressToFile(String value, String fileName) throws IOException {

        FileOutputStream stream;
        stream = new FileOutputStream(fileName);
        InputStream gzis = new GZIPInputStream(new Base64InputStream(IOUtils.toInputStream(value)));
        IOUtils.copy(gzis, stream);
        gzis.close();
        stream.close();
    }

    public static void manipulateLeadFile(HashMap<String, ArrayList<String>> leadInputRecordMap,
            HashMap<String, JSONObject> models, HashMap<String, String> modelIdMap, String record) throws ParseException {

        JSONParser jsonParser = new JSONParser();
        JSONObject leadJsonObject = null;
        leadJsonObject = (JSONObject) jsonParser.parse(record);

        String modelGuid = identifyModelGuid(leadJsonObject, modelIdMap);

        String formattedRecord = transformToJsonString(leadJsonObject, models, modelGuid);
        if (leadInputRecordMap.containsKey(modelGuid)) {
            leadInputRecordMap.get(modelGuid).add(formattedRecord);
        } else {
            ArrayList<String> leadInput = new ArrayList<String>();
            leadInput.add(formattedRecord);
            leadInputRecordMap.put(modelGuid, leadInput);
        }
    }

    private static String identifyModelGuid(JSONObject leadJsonObject, HashMap<String, String> modelIdMap) {
        String modelId = String.valueOf(leadJsonObject.get(LEAD_RECORD_MODEL_ID_COLUMN));
        String guid = null;
        Set<String> modelGuidSet = modelIdMap.keySet();

        for (String modelGuid : modelGuidSet) {
            if (modelId.contains(modelGuid)) {
                guid = modelGuid;
                break;
            }
        }
        
        return guid;
    }

    @SuppressWarnings("unchecked")
    public static String transformToJsonString(JSONObject leadJsonObject, HashMap<String, JSONObject> models,
            String modelGuid) {
        String formattedRecord = null;
        JSONArray metadata = (JSONArray) models.get(modelGuid).get(INPUT_COLUMN_METADATA);

        // parse the avro file since it is in json format
        JSONObject jsonObj = new JSONObject();
        String leadId = String.valueOf(leadJsonObject.get(LEAD_RECORD_LEAD_ID_COLUMN));

        JSONArray jsonArray = new JSONArray();
        jsonObj.put("value", jsonArray);
        jsonObj.put("key", leadId);

        for (int i = 0; i < metadata.size(); i++) {
            JSONObject columnObj = new JSONObject();
            JSONObject serializedValueAndTypeObj = new JSONObject();
            columnObj.put("Value", serializedValueAndTypeObj);
            String type = null;
            // get key
            JSONObject obj = (JSONObject) metadata.get(i);
            String key = String.valueOf(obj.get("Name"));
            columnObj.put("Key", key);
            type = (Long) obj.get("ValueType") == 0 ? "Float" : "String";
            // should treat sqoop null as empty
            String typeAndValue = "";
            if (leadJsonObject.get(key) != null) {
                String value = String.valueOf(leadJsonObject.get(key));
                String processedValue = processBitValue(type, value);
                typeAndValue = String.format("%s|\'%s\'", type, processedValue);
            } else {
                typeAndValue = String.format("%s|", type);
            }
            serializedValueAndTypeObj.put(LEAD_SERIALIZE_TYPE_KEY, typeAndValue);
            jsonArray.add(columnObj);
        }
        formattedRecord = jsonObj.toString() + "\n";
        return formattedRecord;
    }

    static String processBitValue(String type, String value) {
        String toReturn = value;
        if (type.equals("Float")) {
            switch (value.toUpperCase()) {
            case "TRUE":
                toReturn = "1";
                break;
            case "FALSE":
                toReturn = "0";
                break;
            default:
                break;
            }
        }
        return toReturn;
    }
    
    public static void writeToLeadInputFiles(HashMap<String, ArrayList<String>> leadInputRecordMap, long threshold) throws IOException {
        log.info("threshold is " + threshold);
        if (leadInputRecordMap == null) {
            new Exception("leadInputRecordMap is null");
        }
        Set<String> modelGuidSet = leadInputRecordMap.keySet();
        for (String modelGuid : modelGuidSet) {
            writeToLeadInputFile(leadInputRecordMap.get(modelGuid), modelGuid, threshold);
        }
    }

    private static void writeToLeadInputFile(ArrayList<String> leadInputRecords, String modelGuid, long threshold) throws IOException {
        log.info("for model " + modelGuid + ", there are " + leadInputRecords.size() + " leads");

        int indexOfFile = 0;
        int count = 0;
        String leadInputFileName = modelGuid + "-" + indexOfFile;
        log.info("Filename is " + leadInputFileName);
        File file = new File(leadInputFileName);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), "UTF8"));
        for (int i = 0; i < leadInputRecords.size(); i++) {
            count++;
            bw.write(leadInputRecords.get(i));
            if (count == threshold) {
                bw.flush();
                bw.close();
                count = 0;
                indexOfFile++;

                leadInputFileName = modelGuid + "-" + indexOfFile;
                log.info("Filename is " + leadInputFileName);
                file = new File(leadInputFileName);
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), "UTF8"));
            }
        }
        if (count != 0) {
            bw.flush();
            bw.close();
        }
    }

    public static void main(String[] args) throws Exception {
//        String type = "Float";
//        String value = "'123.00'wx";
//
//        String typeAndValue = type + "|\'" + value + "\'";
//        String trpeAndValue2 = String.format("%s|\'%s\'", type, value);
//        System.out.println(value);
//        if (typeAndValue.equals(trpeAndValue2)) {
//            System.out.println("jaja");
//        }
        
        
        /*
         *  Decompose the model.json
         *
         */
        //HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        File modelFile = new File("/Users/ygao/Downloads/leoMKTOTenant_PLSModel_2015-06-10_04-16_model.json");
        String modelStr = FileUtils.readFileToString(modelFile);
        JSONObject modelObject;
        JSONParser parser = new JSONParser();
        modelObject = (JSONObject) parser.parse((modelStr));
        decodeSupportedFiles("e2e", (JSONObject) modelObject.get(MODEL));
        writeScoringScript("e2e", (JSONObject) modelObject.get(MODEL));
    }


}
