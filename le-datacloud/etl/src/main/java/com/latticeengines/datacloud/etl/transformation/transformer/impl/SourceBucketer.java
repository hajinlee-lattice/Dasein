package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_BUCKETER;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.dataflow.transformation.BucketEncode;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BucketEncodeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.BucketedAttribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(SourceBucketer.TRANSFORMER_NAME)
public class SourceBucketer extends AbstractDataflowTransformer<BucketEncodeConfig, BucketEncodeParameters> {

    private static final Logger log = LoggerFactory.getLogger(SourceBucketer.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_BUCKETER;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    protected String getDataFlowBeanName() {
        return BucketEncode.BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<BucketEncodeParameters> getDataFlowParametersClass() {
        return BucketEncodeParameters.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return BucketEncodeConfig.class;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, BucketEncodeParameters parameters,
            BucketEncodeConfig configuration) {
        parameters.srcIdx = 0;
        Source profileSource = step.getBaseSources()[1];
        String profileVersion = step.getBaseVersions().get(1);

        if (!isProfileSource(profileSource, profileVersion)) {
            profileSource = step.getBaseSources()[0];
            profileVersion = step.getBaseVersions().get(0);
            parameters.srcIdx = 1;
            if (!isProfileSource(profileSource, profileVersion)) {
                throw new RuntimeException("Neither base source has the profile schema");
            } else {
                log.info("Resolved the first base source as profile.");
            }
        } else {
            log.info("Resolved the second base source as profile.");
        }

        List<GenericRecord> records;
        if (profileSource instanceof TableSource) {
            Table table = ((TableSource) profileSource).getTable();
            records = new ArrayList<>();
            for (Extract extract: table.getExtracts()) {
                records.addAll(AvroUtils.getDataFromGlob(yarnConfiguration, extract.getPath()));
            }
        } else {
            String avroDir = hdfsPathBuilder.constructSnapshotDir(profileSource.getSourceName(), profileVersion).toString();
            records = AvroUtils.getDataFromGlob(yarnConfiguration, avroDir + "/*.avro");
        }

        parameters.encAttrs = BucketEncodeUtils.encodedAttrs(records);
        parameters.retainAttrs = BucketEncodeUtils.retainFields(records);
        parameters.renameFields = BucketEncodeUtils.renameFields(records);
    }

    private boolean isProfileSource(Source source, String version) {
        String avroPath = TransformerUtils.avroPath(source, version, hdfsPathBuilder);
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, avroPath);
        if (records.hasNext()) {
            GenericRecord record = records.next();
            return BucketEncodeUtils.isProfile(record);
        }
        return false;
    }

    @Override
    protected Schema getTargetSchema(Table result, BucketEncodeParameters parameters, BucketEncodeConfig configuration,
            List<Schema> baseAvscSchemas) {
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
        Map<String, List<BucketedAttribute>> bktAttrMap = bktAttrMap(parameters.encAttrs);
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
            if (bktAttrMap.containsKey(fieldName)) {
                field.set("bucketed_attrs", om.valueToTree(bktAttrMap.get(fieldName)));
            }
        }
        objectNode.set("fields", fields);
        Schema.Parser parser = new Schema.Parser();
        try {
            return parser.parse(om.writeValueAsString(objectNode));
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse modified schema.", e);
        }
    }

    private Map<String, List<BucketedAttribute>> bktAttrMap(List<DCEncodedAttr> encAttrs) {
        Map<String, List<BucketedAttribute>> map = new HashMap<>();
        for (DCEncodedAttr encAtr : encAttrs) {
            String encAttrName = encAtr.getEncAttr();
            List<BucketedAttribute> bktAttrList = new ArrayList<>();
            for (DCBucketedAttr bktAttr : encAtr.getBktAttrs()) {
                bktAttr.setBuckets(bktAttr.getBucketAlgo().generateLabels());
                bktAttr.setDecodedStrategy(null);
                bktAttr.setBucketAlgo(null);
                bktAttr.setSourceAttr(null);
                bktAttrList.add(bktAttr);
            }
            map.put(encAttrName, bktAttrList);
        }
        return map;
    }

}
