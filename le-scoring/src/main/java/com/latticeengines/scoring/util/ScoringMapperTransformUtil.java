package com.latticeengines.scoring.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration.ScoringInputType;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.ScoreContext;

public class ScoringMapperTransformUtil {

    private static final Logger log = LoggerFactory.getLogger(ScoringMapperTransformUtil.class);

    private static Charset charSet = Charset.forName("UTF-8");

    public static Map<String, URI> getModelUris(URI[] uris) throws IOException {
        Map<String, URI> modelUris = new HashMap<>();
        boolean scoringScriptProvided = false;
        for (URI uri : uris) {
            String fragment = uri.getFragment();
            log.info("file: " + uri);
            log.info(fragment);
            if (uri.getPath().endsWith("scoring.py")) {
                scoringScriptProvided = true;
            } else if (uri.getPath().endsWith("pythonlauncher.sh")) {
            } else if (!uri.getPath().endsWith(".jar") && fragment != null && !fragment.endsWith("_scorederivation")) {
                String uuid = fragment;
                modelUris.put(uuid, uri);
            }
        }
        ScoringMapperValidateUtil.validateLocalizedFiles(scoringScriptProvided, modelUris);
        return modelUris;
    }

    public static Map<String, JsonNode> processLocalizedFiles(URI uri) throws IOException {
        // key: uuid, value: model contents
        // Note that not every model in the map might be used.
        Map<String, JsonNode> models = new HashMap<String, JsonNode>();
        String fragment = uri.getFragment();
        String uuid = fragment;
        JsonNode modelJsonObj = parseFileContentToJsonNode(uri);
        // use the uuid to identify a model. It is a contact that when
        // mapper localizes the model, it changes its name to be the
        // uuid
        decodeSupportedFilesToFile(uuid, modelJsonObj.get(ScoringDaemonService.MODEL));
        writeScoringScript(uuid, modelJsonObj.get(ScoringDaemonService.MODEL));
        log.info("modelName is " + modelJsonObj.get(ScoringDaemonService.MODEL_NAME));
        models.put(uuid, modelJsonObj);
        log.info("Has localized " + models.size() + " models.");
        return models;

    }

    @VisibleForTesting
    static JsonNode parseFileContentToJsonNode(URI uri) throws IOException {
        String content = FileUtils.readFileToString(new File(uri.getFragment()), charSet);
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
        FileUtils.writeStringToFile(file, scriptContent, charSet);
    }

    private static void decodeBase64ThenDecompressToFile(String value, String fileName) throws IOException {
        try (FileOutputStream stream = new FileOutputStream(fileName)) {
            try (InputStream gzis = new GZIPInputStream(new Base64InputStream(IOUtils.toInputStream(value, charSet)))) {
                IOUtils.copy(gzis, stream);
            }
        }
    }

    public static void prepareRecordsForScoring(String uuid, ScoreContext context, JsonNode dataType,
            Map<String, JsonNode> models, List<Record> records) throws IOException, InterruptedException {
        for (Record record : records) {
            if (context.type.equals(ScoringInputType.Json.name())) {
                writeToJson(uuid, record, context, dataType, models);
            } else {
                writeToAvro(uuid, record, context);
            }
        }
    }

    private static void writeToAvro(String uuid, Record record, ScoreContext context) throws IOException {
        context.recordNumber++;
        context.modelInfoMap.putIfAbsent(uuid, new ModelAndRecordInfo.ModelInfo(context.uuidToModeId.get(uuid), 0L));
        context.modelInfoMap.get(uuid).setRecordCount(context.modelInfoMap.get(uuid).getRecordCount() + 1L);
        if (context.out == null) {
            context.out = new FileOutputStream(uuid + "-input.avro");
            context.writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
            context.creator = context.writer.create(record.getSchema(), context.out);
        }
        context.creator.append(record);
    }

    private static void writeToJson(String uuid, Record record, ScoreContext context, JsonNode dataType,
            Map<String, JsonNode> models) throws IOException {
        try {
            context.recordNumber++;
            JsonNode jsonNode = context.mapper.readTree(record.toString());
            transformAndWriteRecord(uuid, context, jsonNode, dataType, models);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static void transformAndWriteRecord(String uuid, ScoreContext context, JsonNode jsonNode, JsonNode dataType,
            Map<String, JsonNode> models) throws IOException {
        String modelGuid = context.uuidToModeId.get(uuid);
        // first step validation, to see whether the leadId is provided.
        if (!jsonNode.has(context.uniqueKeyColumn) || jsonNode.get(context.uniqueKeyColumn).isNull()) {
            throw new LedpException(LedpCode.LEDP_20003, new String[] { context.uniqueKeyColumn });
        }

        JsonNode modelContents = models.get(uuid);
        String recordFileName = "";
        BufferedWriter bw = null;

        // second step validation, to see if the metadata is valid
        if (!context.modelInfoMap.containsKey(uuid)) { // this model is new, and
            // needs to be validated
            ScoringMapperValidateUtil.validateDatatype(dataType, modelContents, modelGuid);
            // if the validation passes, update the modelIdMap
            ModelAndRecordInfo.ModelInfo modelInfo = new ModelAndRecordInfo.ModelInfo(modelGuid, 1L);
            context.modelInfoMap.put(uuid, modelInfo);

            recordFileName = uuid + "-0";
            bw = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(new File(recordFileName), true), charSet));
            context.recordFileBufferMap.put(recordFileName, bw);
        } else {
            long currentLeadNum = context.modelInfoMap.get(uuid).getRecordCount() + 1;
            context.modelInfoMap.get(uuid).setRecordCount(currentLeadNum);
            long indexOfFile = currentLeadNum / context.recordFileThreshold;
            StringBuilder leadFileBuilder = new StringBuilder();
            leadFileBuilder.append(uuid).append('-').append(indexOfFile);
            recordFileName = leadFileBuilder.toString();
            if (!context.recordFileBufferMap.containsKey(recordFileName)) {
                // create new stream
                bw = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(new File(recordFileName), true), charSet));
                context.recordFileBufferMap.put(recordFileName, bw);
                // close the previous stream
                StringBuilder formerLeadFileBuilder = new StringBuilder();
                formerLeadFileBuilder.append(uuid).append('-').append(indexOfFile - 1);
                String formerLeadFileName = formerLeadFileBuilder.toString();
                if (context.recordFileBufferMap.containsKey(formerLeadFileName)) {
                    BufferedWriter formerLeadFileBw = context.recordFileBufferMap.get(formerLeadFileName);
                    formerLeadFileBw.close();
                    context.recordFileBufferMap.remove(formerLeadFileName);
                }
            } else {
                bw = context.recordFileBufferMap.get(recordFileName);
            }
        }

        String transformedRecord = transformRecord(jsonNode, modelContents, context.uniqueKeyColumn);
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
        String modelStr = FileUtils.readFileToString(modelFile, charSet);
        JsonNode modelObject = new ObjectMapper().readTree(modelStr);
        decodeSupportedFilesToFile("e2e", modelObject.get(ScoringDaemonService.MODEL));
        writeScoringScript("e2e", modelObject.get(ScoringDaemonService.MODEL));

    }

    public static Map<String, ScoreDerivation> deserializeLocalScoreDerivationFiles(String uuid, URI[] uris)
            throws IOException {
        Map<String, ScoreDerivation> scoreDerivations = new HashMap<>();
        for (URI uri : uris) {
            if (uri.getFragment() != null && uri.getFragment().endsWith("_scorederivation")) {
                String curUuid = org.apache.commons.lang3.StringUtils.substringBeforeLast(uri.getFragment(),
                        "_scorederivation");
                if (uuid.equals(curUuid)) {
                    String content = FileUtils.readFileToString(new File(uri.getFragment()), Charset.forName("UTF-8"));
                    scoreDerivations.put(uuid, JsonUtils.deserialize(content, ScoreDerivation.class));
                    break;
                }
            }
        }
        if (scoreDerivations.isEmpty()) {
            log.warn("Score Derivation Map is empty");
        }
        return scoreDerivations;
    }

}
