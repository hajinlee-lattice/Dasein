package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperTransformUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    public static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    public static final String LEAD_RECORD_LEAD_ID_COLUMN = "LeadID";
    public static final String LEAD_RECORD_MODEL_ID_COLUMN = "Model_GUID";
    public static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    public static final String MODEL = "Model";
    public static final String MODEL_NAME = "Name";
    public static final String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    public static final String MODEL_SCRIPT = "Script";
    public static final String SCORING_SCRIPT_NAME = "scoringengine.py";

    public static LocalizedFiles processLocalizedFiles(Path[] paths) throws IOException, ParseException {
        JSONObject datatype = null;
        // key: modelGuid, value: model contents
        // Note that not every model in the map might be used.
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        LocalizedFiles localizedFiles = new LocalizedFiles();
        boolean scoringScriptProvided = false;
        boolean datatypeFileProvided = false;

        for (Path p : paths) {
            log.info("files" + p);
            log.info(p.getName());
            if (p.getName().equals("datatype.avsc")) {
                datatypeFileProvided = true;
                datatype = parseDatatypeFile(p);
            } else if (p.getName().equals("scoring.py")) {
                scoringScriptProvided = true;
            } else {
                String modelGuid = p.getName();
                JSONObject modelJsonObj = parseModelFiles(p);
                models.put(modelGuid, modelJsonObj);
            }
        }

        log.info("Has localized in total " + models.size() + " models.");
        ScoringMapperValidateUtil.validateLocalizedFiles(scoringScriptProvided, datatypeFileProvided, models);

        localizedFiles.setDatatype(datatype);
        localizedFiles.setModels(models);
        return localizedFiles;
    }

    @VisibleForTesting
    static JSONObject parseDatatypeFile(Path path) throws IOException, ParseException {

        JSONObject datatypeObject = null;
        String content = null;
        content = FileUtils.readFileToString(new File(path.toString()));
        JSONParser jsonParser = new JSONParser();
        datatypeObject = (JSONObject) jsonParser.parse(content);
        return datatypeObject;
    }

    @VisibleForTesting
    static JSONObject parseModelFiles(Path path) throws IOException, ParseException {

        FileReader reader;
        reader = new FileReader(path.toString());
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
        // use the modelGuid to identify a model. It is a contact that when
        // mapper localizes the model, it changes its name to be the modelGuid
        String modelGuid = path.getName();
        decodeSupportedFiles(modelGuid, (JSONObject) jsonObject.get(MODEL));
        writeScoringScript(modelGuid, (JSONObject) jsonObject.get(MODEL));
        log.info("modelName is " + jsonObject.get(MODEL_NAME));
        return jsonObject;
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

    private static void writeScoringScript(String modelGuid, JSONObject modelObject) throws IOException {

        String scriptContent = String.valueOf(modelObject.get(MODEL_SCRIPT));
        String fileName = modelGuid + SCORING_SCRIPT_NAME;
        log.info("fileName is " + fileName);
        File file = new File(fileName);
        FileUtils.writeStringToFile(file, scriptContent);
    }

    private static void decodeBase64ThenDecompressToFile(String value, String fileName) throws IOException {

        FileOutputStream stream;
        stream = new FileOutputStream(fileName);
        InputStream gzis = new GZIPInputStream(new Base64InputStream(IOUtils.toInputStream(value)));
        IOUtils.copy(gzis, stream);
        gzis.close();
        stream.close();
    }

    public static ModelAndLeadInfo prepareLeadsForScoring(
            Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context,
            LocalizedFiles localizedFiles, long leadFileThreshold) throws IOException, InterruptedException,
            ParseException {

        if (context == null || localizedFiles == null) {
            throw new NullPointerException("context and localizedFiles cannot be null.");
        }

        if (leadFileThreshold <= 0) {
            throw new IllegalArgumentException("leadFileThreshold must be positive.");
        }

        ModelAndLeadInfo modelAndLeadInfo = new ModelAndLeadInfo();
        // key: modelGuid, value: model information containing modelId and lead
        // number associated with that model
        Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndLeadInfo.ModelInfo>();

        // key: leadFileName, value: the bufferwriter the file connecting
        Map<String, BufferedWriter> leadFileBufferMap = new HashMap<String, BufferedWriter>();

        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L);
        log.info("Before transformation, system has used " + usedMemory);

        int leadNumber = 0;
        while (context.nextKeyValue()) {
            leadNumber++;
            transformAndWriteLead(context.getCurrentKey().datum().toString(), modelInfoMap, leadFileBufferMap,
                    localizedFiles, leadFileThreshold);
        }
        Set<String> keySet = leadFileBufferMap.keySet();
        for (String key : keySet) {
            leadFileBufferMap.get(key).close();
        }

        modelAndLeadInfo.setModelInfoMap(modelInfoMap);
        modelAndLeadInfo.setTotalleadNumber(leadNumber);
        ScoringMapperValidateUtil.validateTransformation(modelAndLeadInfo);

        return modelAndLeadInfo;
    }

    @VisibleForTesting
    static void transformAndWriteLead(String leadString, Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap,
            Map<String, BufferedWriter> leadFilebufferMap, LocalizedFiles localizedFiles, long leadFileThreshold)
            throws ParseException, IOException {
        JSONParser parser = new JSONParser();
        JSONObject leadJsonObject = (JSONObject) parser.parse(leadString);

        Set<String> modelGuidSet = localizedFiles.getModels().keySet();
        String modelId = String.valueOf(leadJsonObject.get(LEAD_RECORD_MODEL_ID_COLUMN));
        String modelGuid = identifyModelGuid(modelId, modelGuidSet);
        // first step validation, to see whether the model is provided.
        if (modelGuid.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20007, new String[] { modelId });
        }

        JSONObject modelContents = localizedFiles.getModels().get(modelGuid);
        String leadFileName = "";
        BufferedWriter bw = null;
        // second step validation, to see if the metadata is valid
        if (!modelInfoMap.containsKey(modelGuid)) { // this model is new, and
                                                    // needs to be validated
            ScoringMapperValidateUtil.validateDatatype(localizedFiles.getDatatype(), modelContents, modelId);
            // if the validation passes, update the modelIdMap
            ModelAndLeadInfo.ModelInfo modelInfo = new ModelAndLeadInfo.ModelInfo(modelId, 1L);
            modelInfoMap.put(modelGuid, modelInfo);

            leadFileName = modelGuid + "-0";
            log.info("the leadFileName is " + leadFileName);
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(leadFileName), true), "UTF8"));
            leadFilebufferMap.put(leadFileName, bw);
        } else {
            long currentLeadNum = modelInfoMap.get(modelGuid).getLeadNumber() + 1;
            modelInfoMap.get(modelGuid).setLeadNumber(currentLeadNum);
            long indexOfFile = currentLeadNum / leadFileThreshold;
            StringBuilder leadFileBuilder = new StringBuilder();
            leadFileBuilder.append(modelGuid).append('-').append(indexOfFile);
            leadFileName = leadFileBuilder.toString();
            log.info("the leadFileName is " + leadFileName);
            if (!leadFilebufferMap.containsKey(leadFileName)) {
                // create new stream
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(leadFileName), true),
                        "UTF8"));
                leadFilebufferMap.put(leadFileName, bw);
                // close the previous stream
                StringBuilder formerLeadFileBuilder = new StringBuilder();
                formerLeadFileBuilder.append(modelGuid).append('-').append(indexOfFile - 1);
                String formerLeadFileName = formerLeadFileBuilder.toString();
                log.info("the formerLeadFileName is " + formerLeadFileName);
                if (leadFilebufferMap.containsKey(formerLeadFileName)) {
                    BufferedWriter formerLeadFileBw = leadFilebufferMap.get(formerLeadFileName);
                    formerLeadFileBw.close();
                }
            } else {
                bw = leadFilebufferMap.get(leadFileName);
            }
        }

        String transformedLead = transformLead(leadJsonObject, modelContents);
        writeLeadToFile(transformedLead, bw);

        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L);
        log.info("During the transformation, system has used " + usedMemory);
        return;
    }

    private static String identifyModelGuid(String modelId, Set<String> modelGuidSet) {
        String guid = "";
        for (String modelGuid : modelGuidSet) {
            if (modelId.contains(modelGuid)) {
                guid = modelGuid;
                break;
            }
        }
        return guid;
    }

    public static String transformLead(JSONObject leadJsonObject, JSONObject modelJsonObject) {
        JSONArray metadata = (JSONArray) modelJsonObject.get(INPUT_COLUMN_METADATA);

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
        return jsonObj.toString();
    }

    @VisibleForTesting
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

    private static void writeLeadToFile(String jsonFormattedLead, BufferedWriter bw) throws IOException {
        bw.write(jsonFormattedLead);
        bw.write('\n');
    }

    public static void main(String[] args) throws Exception {

        File modelFile = new File("/Users/ygao/Downloads/leoMKTOTenant_PLSModel_2015-06-10_04-16_model.json");
        String modelStr = FileUtils.readFileToString(modelFile);
        JSONObject modelObject;
        JSONParser parser = new JSONParser();
        modelObject = (JSONObject) parser.parse((modelStr));
        decodeSupportedFiles("e2e", (JSONObject) modelObject.get(MODEL));
        writeScoringScript("e2e", (JSONObject) modelObject.get(MODEL));
    }

}
