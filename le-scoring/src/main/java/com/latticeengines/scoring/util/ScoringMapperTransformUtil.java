package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

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
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;

public class ScoringMapperTransformUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    public static HashMap<String, JsonNode> processLocalizedFiles(Path[] paths) throws IOException {
        // key: uuid, value: model contents
        // Note that not every model in the map might be used.
        HashMap<String, JsonNode> models = new HashMap<String, JsonNode>();
        boolean scoringScriptProvided = false;

        for (Path p : paths) {
            log.info("files" + p);
            log.info(p.getName());

            if (p.getName().equals("scoring.py")) {
                scoringScriptProvided = true;
            } else if (!p.getName().endsWith(".jar")) {
                String uuid = p.getName();
                JsonNode modelJsonObj = parseFileContentToJsonNode(p);
                // use the uuid to identify a model. It is a contact that when
                // mapper localizes the model, it changes its name to be the
                // uuid
                decodeSupportedFilesToFile(uuid, modelJsonObj.get(ScoringDaemonService.MODEL));
                writeScoringScript(uuid, modelJsonObj.get(ScoringDaemonService.MODEL));
                log.info("modelName is " + modelJsonObj.get(ScoringDaemonService.MODEL_NAME));
                models.put(uuid, modelJsonObj);
            }
        }

        log.info("Has localized in total " + models.size() + " models.");
        ScoringMapperValidateUtil.validateLocalizedFiles(scoringScriptProvided, models);

        return models;
    }

    @VisibleForTesting
    static JsonNode parseFileContentToJsonNode(Path path) throws IOException {
        String content = FileUtils.readFileToString(new File(path.toString()));
        JsonNode jsonNode = new ObjectMapper().readTree(content);
        return jsonNode;
    }

    @VisibleForTesting
    static void decodeSupportedFilesToFile(String uuid, JsonNode modelObject) throws IOException {

        ArrayNode compressedSupportedFiles = (ArrayNode) modelObject
                .get(ScoringDaemonService.MODEL_COMPRESSED_SUPPORT_Files);
        for (JsonNode compressedFile : compressedSupportedFiles) {
            String compressedFileName = uuid + compressedFile.get("Key").asText();
            log.info("compressedFileName is " + compressedFileName);
            decodeBase64ThenDecompressToFile(compressedFile.get("Value").asText(), compressedFileName);
        }

    }

    @VisibleForTesting
    static void writeScoringScript(String uuid, JsonNode modelObject) throws IOException {

        String scriptContent = modelObject.get(ScoringDaemonService.MODEL_SCRIPT).asText();
        String fileName = uuid + ScoringDaemonService.SCORING_SCRIPT_NAME;
        log.info("fileName is " + fileName);
        File file = new File(fileName);
        FileUtils.writeStringToFile(file, scriptContent);
    }

    private static void decodeBase64ThenDecompressToFile(String value, String fileName) throws IOException {
        try (FileOutputStream stream = new FileOutputStream(fileName)) {
            try (InputStream gzis = new GZIPInputStream(new Base64InputStream(IOUtils.toInputStream(value)))) {
                IOUtils.copy(gzis, stream);
            }
        }
    }

    public static ModelAndRecordInfo prepareRecordsForScoring(
            Mapper<AvroKey<Record>, NullWritable, NullWritable, NullWritable>.Context context, JsonNode dataType,
            HashMap<String, JsonNode> models, long leadFileThreshold) throws IOException, InterruptedException {

        Configuration config = context.getConfiguration();
        ModelAndRecordInfo modelAndLeadInfo = new ModelAndRecordInfo();
        // key: uuid, value: model information containing modelId and record
        // number associated with that model
        Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap = new HashMap<String, ModelAndRecordInfo.ModelInfo>();

        // key: recordFileName, value: the bufferwriter the file connecting
        Map<String, BufferedWriter> recordFileBufferMap = new HashMap<String, BufferedWriter>();

        int recordNumber = 0;
        Collection<String> modelGuids = config.getStringCollection(ScoringProperty.MODEL_GUID.name());
        ObjectMapper mapper = new ObjectMapper();
        String uniqueKeyColumn = config.get(ScoringProperty.UNIQUE_KEY_COLUMN.name());
        while (context.nextKeyValue()) {
            String record = context.getCurrentKey().datum().toString();
            JsonNode jsonNode = mapper.readTree(record);
            if (CollectionUtils.isEmpty(modelGuids)) {
                String modelGuid = jsonNode.get(ScoringDaemonService.MODEL_GUID).asText();
                transformAndWriteRecord(jsonNode, dataType, modelInfoMap, recordFileBufferMap, models, leadFileThreshold,
                        modelGuid, uniqueKeyColumn);
                recordNumber++;
            } else {
                for (String modelGuid : modelGuids) {
                    transformAndWriteRecord(jsonNode, dataType, modelInfoMap, recordFileBufferMap, models,
                            leadFileThreshold, modelGuid, uniqueKeyColumn);
                    recordNumber++;
                }
            }

        }
        Set<String> keySet = recordFileBufferMap.keySet();
        for (String key : keySet) {
            recordFileBufferMap.get(key).close();
        }

        modelAndLeadInfo.setModelInfoMap(modelInfoMap);
        modelAndLeadInfo.setTotalRecordCountr(recordNumber);
        ScoringMapperValidateUtil.validateTransformation(modelAndLeadInfo);

        return modelAndLeadInfo;
    }

    @VisibleForTesting
    static void transformAndWriteRecord(JsonNode jsonNode, JsonNode dataType, Map<String, ModelAndRecordInfo.ModelInfo> modelInfoMap,
            Map<String, BufferedWriter> recordFilebufferMap, HashMap<String, JsonNode> models, long recordFileThreshold,
            String modelGuid, String uniqueKeyColumn) throws IOException {
        // first step validation, to see whether the leadId is provided.
        if (!jsonNode.has(uniqueKeyColumn) || jsonNode.get(uniqueKeyColumn).isNull()) {
            throw new LedpException(LedpCode.LEDP_20003, new String[] { uniqueKeyColumn });
        }

        String uuid = UuidUtils.extractUuid(modelGuid);
        JsonNode modelContents = models.get(uuid);
        String recordFileName = "";
        BufferedWriter bw = null;

        // second step validation, to see if the metadata is valid
        if (!modelInfoMap.containsKey(uuid)) { // this model is new, and
                                               // needs to be validated
            ScoringMapperValidateUtil.validateDatatype(dataType, modelContents, modelGuid);
            // if the validation passes, update the modelIdMap
            ModelAndRecordInfo.ModelInfo modelInfo = new ModelAndRecordInfo.ModelInfo(modelGuid, 1L);
            modelInfoMap.put(uuid, modelInfo);

            recordFileName = uuid + "-0";
            bw = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(new File(recordFileName), true), "UTF8"));
            recordFilebufferMap.put(recordFileName, bw);
        } else {
            long currentLeadNum = modelInfoMap.get(uuid).getRecordCount() + 1;
            modelInfoMap.get(uuid).setRecordCount(currentLeadNum);
            long indexOfFile = currentLeadNum / recordFileThreshold;
            StringBuilder leadFileBuilder = new StringBuilder();
            leadFileBuilder.append(uuid).append('-').append(indexOfFile);
            recordFileName = leadFileBuilder.toString();
            if (!recordFilebufferMap.containsKey(recordFileName)) {
                // create new stream
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(recordFileName), true),
                        "UTF8"));
                recordFilebufferMap.put(recordFileName, bw);
                // close the previous stream
                StringBuilder formerLeadFileBuilder = new StringBuilder();
                formerLeadFileBuilder.append(uuid).append('-').append(indexOfFile - 1);
                String formerLeadFileName = formerLeadFileBuilder.toString();
                if (recordFilebufferMap.containsKey(formerLeadFileName)) {
                    BufferedWriter formerLeadFileBw = recordFilebufferMap.get(formerLeadFileName);
                    formerLeadFileBw.close();
                }
            } else {
                bw = recordFilebufferMap.get(recordFileName);
            }
        }

        String transformedRecord = transformRecord(jsonNode, modelContents, uniqueKeyColumn);
        writeRecordToFile(transformedRecord, bw);

    }

    public static String transformRecord(JsonNode jsonNode, JsonNode modelJsonObject, String uniqueKeyColumn)
            throws UnsupportedEncodingException {
        ArrayNode metadata = (ArrayNode) modelJsonObject.get(ScoringDaemonService.INPUT_COLUMN_METADATA);

        // parse the avro file since it is in json format
        ObjectNode jsonObj = new ObjectMapper().createObjectNode();
        String recordId = jsonNode.get(uniqueKeyColumn).asText();

        ArrayNode jsonArray = jsonObj.putArray("value");
        jsonObj.put("key", StringUtils.byteToHexString(recordId.getBytes("UTF8")));

        ObjectMapper mapper = new ObjectMapper();
        for (JsonNode objKey : metadata) {
            ObjectNode columnObj = mapper.createObjectNode();
            ObjectNode serializedValueAndTypeObj = columnObj.putObject("Value");
            // get key
            String key = objKey.get("Name").asText();
            columnObj.put("Key", key);
            String type = objKey.get("ValueType").asLong() == 0 ? "Float" : "String";
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

    private static void writeRecordToFile(String jsonFormattedLead, BufferedWriter bw) throws IOException {
        bw.write(jsonFormattedLead);
        bw.write('\n');
    }

    public static void main(String[] args) throws Exception {
        File modelFile = new File("/Users/ygao/Downloads/leoMKTOTenant_PLSModel_2015-06-10_04-16_model.json");
        String modelStr = FileUtils.readFileToString(modelFile);
        JsonNode modelObject = new ObjectMapper().readTree(modelStr);
        decodeSupportedFilesToFile("e2e", modelObject.get(ScoringDaemonService.MODEL));
        writeScoringScript("e2e", modelObject.get(ScoringDaemonService.MODEL));
        
    }

}
