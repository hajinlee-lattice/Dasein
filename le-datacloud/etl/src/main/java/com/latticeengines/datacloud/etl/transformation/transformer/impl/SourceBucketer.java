package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceBucketer.TRANSFORMER_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.bucket.BucketEncode;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BucketEncodeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(TRANSFORMER_NAME)
public class SourceBucketer extends AbstractDataflowTransformer<BucketEncodeConfig, BucketEncodeParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(SourceBucketer.class);

    public static final String TRANSFORMER_NAME = "sourceBucketer";

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

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
        //TODO: hook up with API configurations, for now hard coded AccountMaster fake bucketing
        parameters.encAttrs = readConfigJson();
        parameters.rowIdField = "LatticeID";
    }

    @Override
    protected boolean validateConfig(BucketEncodeConfig config, List<String> sourceNames) {
        return true;
    }

    // TODO: to be removed and replaced by avro table driven configuration
    private List<DCEncodedAttr> readConfigJson() {
        // read encoded attrs
        InputStream encAttrsIs = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("config.json");
        if (encAttrsIs == null) {
            throw new RuntimeException("Failed ot find resource config.json");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<List<DCEncodedAttr>> typeRef = new TypeReference<List<DCEncodedAttr>>() { };
        List<DCEncodedAttr> encAttrs;
        try {
            encAttrs = objectMapper.readValue(encAttrsIs, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse json config.", e);
        }
        return encAttrs;
    }

}
