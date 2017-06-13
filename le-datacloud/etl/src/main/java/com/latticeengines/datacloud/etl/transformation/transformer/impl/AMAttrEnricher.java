package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.AMAttrEnrich;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMAttrEnrichParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMAttrEnrichConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(AMAttrEnrich.TRANSFORMER_NAME)
public class AMAttrEnricher extends AbstractDataflowTransformer<AMAttrEnrichConfig, AMAttrEnrichParameters> {

    @Override
    protected String getDataFlowBeanName() {
        return AMAttrEnrich.BEAN_NAME;
    }

    @Override
    public String getName() {
        return AMAttrEnrich.TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AMAttrEnrichConfig.class;
    }

    @Override
    protected Class<AMAttrEnrichParameters> getDataFlowParametersClass() {
        return AMAttrEnrichParameters.class;
    }

    @Override
    protected void updateParameters(AMAttrEnrichParameters parameters, Source[] baseTemplates, Source targetTemplate,
            AMAttrEnrichConfig config, List<String> baseVersions) {
        parameters.setAmLatticeId(config.getAmLatticeId());
        parameters.setInputLatticeId(config.getInputLatticeId());
        parameters.setNotJoinAM(config.isNotJoinAM());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected Schema getTargetSchema(Table result, AMAttrEnrichParameters parameters, List<Schema> baseAvscSchemas) {
        String extractPath = result.getExtracts().get(0).getPath();
        String glob;
        if (extractPath.endsWith(".avro")) {
            glob = extractPath;
        } else if (extractPath.endsWith(File.pathSeparator)) {
            glob = extractPath + "*.avro";
        } else {
            glob = extractPath + File.separator + "*.avro";
        }
        Schema parsed = AvroUtils.getSchemaFromGlob(yarnConfiguration, glob);
        Set<String> inputAttr = new HashSet(parameters.getInputAttrs());
        ObjectMapper om = new ObjectMapper();
        ObjectNode objectNode;
        try {
            objectNode = om.readValue(parsed.toString(), ObjectNode.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse avro schema of cascading result table.", e);
        }
        ArrayNode fields = (ArrayNode) objectNode.get("fields");
        for (JsonNode jNode : fields) {
            ObjectNode field = (ObjectNode) jNode;
            String fieldName = field.get("name").asText();
            if (inputAttr.contains(fieldName)) {
                field.put(DataCloudConstants.PROFILE_ATTR_SRC,
                        om.valueToTree(DataCloudConstants.PROFILE_ATTR_SRC_CUSTOMER));
            }
        }
        objectNode.put("fields", fields);
        Schema.Parser parser = new Schema.Parser();
        try {
            return parser.parse(om.writeValueAsString(objectNode));
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse modified schema.", e);
        }
    }

}
