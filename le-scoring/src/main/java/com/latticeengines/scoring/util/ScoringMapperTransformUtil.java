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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;

public class ScoringMapperTransformUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    public static LocalizedFiles processLocalizedFiles(Path[] paths) throws IOException {
        JsonNode datatype = null;
        // key: modelGuid, value: model contents
        // Note that not every model in the map might be used.
        HashMap<String, JsonNode> models = new HashMap<String, JsonNode>();
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
            } else if (!p.getName().endsWith(".jar")) {
                String modelGuid = p.getName();
                JsonNode modelJsonObj = parseModelFiles(p);
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
    static JsonNode parseDatatypeFile(Path path) throws IOException {
        String content = FileUtils.readFileToString(new File(path.toString()));
        JsonNode datatypeJsonNode = new ObjectMapper().readTree(content);
        return datatypeJsonNode;
    }

    @VisibleForTesting
    static JsonNode parseModelFiles(Path path) throws IOException {

        FileReader reader;
        reader = new FileReader(path.toString());
        JsonNode jsonObject = new ObjectMapper().readTree(reader);
        // use the modelGuid to identify a model. It is a contact that when
        // mapper localizes the model, it changes its name to be the modelGuid
        String modelGuid = path.getName();
        decodeSupportedFiles(modelGuid, jsonObject.get(ScoringDaemonService.MODEL));
        writeScoringScript(modelGuid, jsonObject.get(ScoringDaemonService.MODEL));
        log.info("modelName is " + jsonObject.get(ScoringDaemonService.MODEL_NAME));
        return jsonObject;
    }

    private static void decodeSupportedFiles(String modelGuid, JsonNode modelObject) throws IOException {

        ArrayNode compressedSupportedFiles = (ArrayNode) modelObject
                .get(ScoringDaemonService.MODEL_COMPRESSED_SUPPORT_Files);
        for (int i = 0; i < compressedSupportedFiles.size(); i++) {
            JsonNode compressedFile = compressedSupportedFiles.get(i);
            String compressedFileName = modelGuid + compressedFile.get("Key").asText();
            log.info("compressedFileName is " + compressedFileName);
            decodeBase64ThenDecompressToFile(compressedFile.get("Value").asText(), compressedFileName);
        }

    }

    private static void writeScoringScript(String modelGuid, JsonNode modelObject) throws IOException {

        String scriptContent = modelObject.get(ScoringDaemonService.MODEL_SCRIPT).asText();
        String fileName = modelGuid + ScoringDaemonService.SCORING_SCRIPT_NAME;
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
            LocalizedFiles localizedFiles, long leadFileThreshold) throws IOException, InterruptedException {

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

        int leadNumber = 0;
        ObjectMapper mapper = new ObjectMapper();
        while (context.nextKeyValue()) {
            leadNumber++;
            String record = context.getCurrentKey().datum().toString();
            JsonNode jsonNode = mapper.readTree(record);
            String modelGuid = jsonNode.get(ScoringDaemonService.MODEL_GUID).asText();
            transformAndWriteLead(jsonNode, modelInfoMap, leadFileBufferMap, localizedFiles, leadFileThreshold,
                    modelGuid);
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
    static void transformAndWriteLead(JsonNode jsonNode, Map<String, ModelAndLeadInfo.ModelInfo> modelInfoMap,
            Map<String, BufferedWriter> leadFilebufferMap, LocalizedFiles localizedFiles, long leadFileThreshold,
            String modelGuid) throws IOException {
        // first step validation, to see whether the leadId is provided.
        if (!jsonNode.has(ScoringDaemonService.UNIQUE_KEY_COLUMN)
                || jsonNode.get(ScoringDaemonService.UNIQUE_KEY_COLUMN).isNull()) {
            throw new LedpException(LedpCode.LEDP_20003);
        }

        String uuid = UuidUtils.extractUuid(modelGuid);
        JsonNode modelContents = localizedFiles.getModels().get(uuid);
        String leadFileName = "";
        BufferedWriter bw = null;
        // second step validation, to see if the metadata is valid

        if (!modelInfoMap.containsKey(uuid)) { // this model is new, and
                                               // needs to be validated
            ScoringMapperValidateUtil.validateDatatype(localizedFiles.getDatatype(), modelContents, modelGuid);
            // if the validation passes, update the modelIdMap
            ModelAndLeadInfo.ModelInfo modelInfo = new ModelAndLeadInfo.ModelInfo(modelGuid, 1L);
            modelInfoMap.put(uuid, modelInfo);

            leadFileName = uuid + "-0";
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(leadFileName), true), "UTF8"));
            leadFilebufferMap.put(leadFileName, bw);
        } else {
            long currentLeadNum = modelInfoMap.get(uuid).getLeadNumber() + 1;
            modelInfoMap.get(uuid).setLeadNumber(currentLeadNum);
            long indexOfFile = currentLeadNum / leadFileThreshold;
            StringBuilder leadFileBuilder = new StringBuilder();
            leadFileBuilder.append(uuid).append('-').append(indexOfFile);
            leadFileName = leadFileBuilder.toString();
            if (!leadFilebufferMap.containsKey(leadFileName)) {
                // create new stream
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(leadFileName), true),
                        "UTF8"));
                leadFilebufferMap.put(leadFileName, bw);
                // close the previous stream
                StringBuilder formerLeadFileBuilder = new StringBuilder();
                formerLeadFileBuilder.append(uuid).append('-').append(indexOfFile - 1);
                String formerLeadFileName = formerLeadFileBuilder.toString();
                if (leadFilebufferMap.containsKey(formerLeadFileName)) {
                    BufferedWriter formerLeadFileBw = leadFilebufferMap.get(formerLeadFileName);
                    formerLeadFileBw.close();
                }
            } else {
                bw = leadFilebufferMap.get(leadFileName);
            }
        }

        String transformedLead = transformLead(jsonNode, modelContents);
        writeLeadToFile(transformedLead, bw);

        return;
    }

    public static String transformLead(JsonNode jsonNode, JsonNode modelJsonObject) {
        ArrayNode metadata = (ArrayNode) modelJsonObject.get(ScoringDaemonService.INPUT_COLUMN_METADATA);

        // parse the avro file since it is in json format
        ObjectNode jsonObj = new ObjectMapper().createObjectNode();
        String leadId = jsonNode.get(ScoringDaemonService.UNIQUE_KEY_COLUMN).asText();

        ArrayNode jsonArray = jsonObj.putArray("value");
        jsonObj.put("key", leadId);

        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < metadata.size(); i++) {
            ObjectNode columnObj = mapper.createObjectNode();
            ObjectNode serializedValueAndTypeObj = mapper.createObjectNode();
            columnObj.set("Value", serializedValueAndTypeObj);
            String type = null;
            // get key
            JsonNode obj = metadata.get(i);
            String key = obj.get("Name").asText();
            columnObj.put("Key", key);
            type = obj.get("ValueType").asLong() == 0 ? "Float" : "String";
            // should treat sqoop null as empty
            String typeAndValue = "";
            if (jsonNode.has(key) && !jsonNode.get(key).isNull()) {
                String value = jsonNode.get(key).asText();
                String processedValue = processBitValue(type, value);
                typeAndValue = String.format("%s|\'%s\'", type, processedValue);
            } else {
                typeAndValue = String.format("%s|", type);
            }
            serializedValueAndTypeObj.put(ScoringDaemonService.LEAD_SERIALIZE_TYPE_KEY, typeAndValue);
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
        JsonNode modelObject = new ObjectMapper().readTree(modelStr);
        decodeSupportedFiles("e2e", modelObject.get(ScoringDaemonService.MODEL));
        writeScoringScript("e2e", modelObject.get(ScoringDaemonService.MODEL));
    }

}
