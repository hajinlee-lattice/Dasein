package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.OrbCacheSeedMarkerFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecondaryDomainMarkerTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Function;

@Component("orbCacheSeedMarkerTransformerFlow")
public class OrbCacheSeedSecondaryDomainMarkerFlow
        extends ConfigurableFlowBase<OrbCacheSeedSecondaryDomainMarkerTransformerConfig> {

    private static final Logger log = LoggerFactory.getLogger(OrbCacheSeedSecondaryDomainMarkerFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        OrbCacheSeedSecondaryDomainMarkerTransformerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> fieldNames = source.getFieldNames();

        for (String fieldName : fieldNames) {
            log.info("Field in input schema " + fieldName);
        }

        List<String> outputFieldNames = new ArrayList<>(fieldNames);
        Function<?> function = new OrbCacheSeedMarkerFunction(config.getMarkerFieldName(), config.getFieldsToCheck());
        FieldList fieldsToApply = new FieldList(config.getFieldsToCheck());
        List<FieldMetadata> targetFields = new ArrayList<>();
        FieldMetadata secondaryDomainMarker = new FieldMetadata(config.getMarkerFieldName(), Boolean.class);
        targetFields.add(secondaryDomainMarker);

        outputFieldNames.add(config.getMarkerFieldName());
        FieldList outputFields = new FieldList(outputFieldNames);
        Node marked = source.apply(function, fieldsToApply, targetFields, outputFields);
        return marked;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return OrbCacheSeedSecondaryDomainMarkerTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "orbCacheSeedMarkerTransformerFlow";
    }

    @Override
    public String getTransformerName() {
        return "orbCacheSeedMarkerTransformer";

    }
}
