package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DiagnosticsJsonAggregator extends ProfilingAggregator {

    public static final String DATA_SUMMARY = "DataSummary";
    public static final String METADATA_SUMMARY = "MetadataSummary";
    public static final String ATTRIBUTE_SUMMARY = "AttributeSummary";

    private ObjectMapper mapper = new ObjectMapper();
    private ObjectNode diagnosticsFile;

    @Override
    void aggregateToLocal(List<String> localPaths) throws Exception {
        diagnosticsFile = mapper.createObjectNode();

        List<JsonNode> diagnosticsFiles = new ArrayList<JsonNode>();
        for (String path : localPaths) {
            String content = FileUtils.readFileToString(new File(path));
            diagnosticsFiles.add(mapper.readTree(content));
        }

        Iterator<Entry<String, JsonNode>> iterator = diagnosticsFiles.get(0).fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> field = iterator.next();
            String key = field.getKey();
            JsonNode value = field.getValue();

            switch (key) {
            case DATA_SUMMARY:
                aggregatDataSummary((ObjectNode) value, diagnosticsFiles);
                break;

            case ATTRIBUTE_SUMMARY:
                aggregatAttributeSummary(value, diagnosticsFiles);
                break;

            default:
                diagnosticsFile.put(key, value);
                break;
            }
        }
        FileUtils.writeStringToFile(new File(getName()), diagnosticsFile.toString());
    }

    private void aggregatDataSummary(ObjectNode firstSummary, List<JsonNode> diagnosticsFiles) throws Exception {
        ObjectNode dataSummary = firstSummary;

        int columnSize = 0;
        for (JsonNode diagnosticsFile : diagnosticsFiles) {
            columnSize += diagnosticsFile.get(DATA_SUMMARY).get("ColumnSize").asInt();
        }
        dataSummary.put("ColumnSize", columnSize);

        diagnosticsFile.put(DATA_SUMMARY, dataSummary);
    }

    private void aggregatAttributeSummary(JsonNode firstSummary, List<JsonNode> diagnosticsFiles) throws Exception {
        ObjectNode attributeSummary = mapper.createObjectNode();

        Iterator<String> keys = firstSummary.fieldNames();
        while (keys.hasNext()) {
            String key = keys.next();
            ArrayNode attributeFieldArray = mapper.createArrayNode();

            for (JsonNode diagnosticsFile : diagnosticsFiles) {
                JsonNode tempArray = diagnosticsFile.get(ATTRIBUTE_SUMMARY).withArray(key);

                for (int i = 0; i < tempArray.size(); i++) {
                    attributeFieldArray.add(tempArray.get(i));
                }
            }
            attributeSummary.put(key, attributeFieldArray);
        }
        diagnosticsFile.put(ATTRIBUTE_SUMMARY, attributeSummary);
    }

    @Override
    public String getName() {
        return FileAggregator.DIAGNOSTICS_JSON;
    }

}
