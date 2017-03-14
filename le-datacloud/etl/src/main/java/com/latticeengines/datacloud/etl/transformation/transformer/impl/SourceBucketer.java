package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceBucketer.TRANSFORMER_NAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.bucket.BucketEncode;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BucketEncodeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.BucketedAttribute;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(TRANSFORMER_NAME)
public class SourceBucketer extends AbstractDataflowTransformer<BucketEncodeConfig, BucketEncodeParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(SourceBucketer.class);

    public static final String TRANSFORMER_NAME = "sourceBucketer";

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

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
    protected void updateParameters(BucketEncodeParameters parameters, Source[] baseTemplates, Source targetTemplate,
            BucketEncodeConfig config) {
        // TODO: hook up with API configurations, for now hard coded
        // AccountMaster fake bucketing
        parameters.encAttrs = readConfigJson();
        parameters.excludeAttrs = excludeAttrs();
        // legacy code just to rename LatticeID to LatticeAccountId
        parameters.rowIdField = "LatticeID";
        parameters.renameRowIdField = "LatticeAccountId";
    }

    @Override
    protected boolean validateConfig(BucketEncodeConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected Schema getTargetSchema(Table result, BucketEncodeParameters parameters) {
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
                field.put("bucketed_attrs", om.valueToTree(bktAttrMap.get(fieldName)));
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

    private Map<String, List<BucketedAttribute>> bktAttrMap(List<DCEncodedAttr> encAttrs) {
        Map<String, List<BucketedAttribute>> map = new HashMap<>();
        for (DCEncodedAttr encAtr : encAttrs) {
            String encAttrName = encAtr.getEncAttr();
            List<BucketedAttribute> bktAttrList = new ArrayList<>();
            for (DCBucketedAttr bktAttr : encAtr.getBktAttrs()) {
                bktAttr.setDecodedStrategy(null);
                bktAttr.setBucketAlgo(null);
                bktAttrList.add(bktAttr);
            }
            map.put(encAttrName, bktAttrList);
        }
        return map;
    }

    // TODO: to be removed and replaced by avro table driven configuration
    private List<DCEncodedAttr> readConfigJson() {
        // read encoded attrs
        InputStream encAttrsIs = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.json");
        if (encAttrsIs == null) {
            throw new RuntimeException("Failed ot find resource config.json");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<List<DCEncodedAttr>> typeRef = new TypeReference<List<DCEncodedAttr>>() {};
        List<DCEncodedAttr> encAttrs;
        try {
            encAttrs = objectMapper.readValue(encAttrsIs, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse json config.", e);
        }
        return encAttrs;
    }


    private List<String> excludeAttrs() {
        // exclude fields
        List<String> excludeAttrs = new ArrayList<>();

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("exclude.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource PublicDomains.txt");
        }
        Scanner scanner = new Scanner(is);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (StringUtils.isNotEmpty(line)) {
                excludeAttrs.add(line);
            }
        }
        scanner.close();
        return excludeAttrs;
    }

}
