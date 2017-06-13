package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.StatsCalculator.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STATS_CALCULATOR;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.dataflow.transformation.CalculateStats;
import com.latticeengines.domain.exposed.datacloud.dataflow.CalculateStatsParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BucketEncodeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(TRANSFORMER_NAME)
public class StatsCalculator extends AbstractDataflowTransformer<BucketEncodeConfig, CalculateStatsParameter> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_STATS_CALCULATOR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        return CalculateStats.BEAN_NAME;
    }

    @Override
    protected Class<CalculateStatsParameter> getDataFlowParametersClass() {
        return CalculateStatsParameter.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return BucketEncodeConfig.class;
    }

    @Override
    protected boolean validateConfig(BucketEncodeConfig config, List<String> sourceNames) {
        return true;
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

}
