package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecondaryDomainCleanupTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("orbCacheSeedCleanedTransformerFlow")
public class OrbCacheSeedSecondaryDomainCleanupFlow
        extends ConfigurableFlowBase<OrbCacheSeedSecondaryDomainCleanupTransformerConfig> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(OrbCacheSeedSecondaryDomainCleanupFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        OrbCacheSeedSecondaryDomainCleanupTransformerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> fieldNames = source.getFieldNames();

        String markerFieldName = config.getMarkerFieldName();

        FieldList filterFieldList = new FieldList(markerFieldName);
        String expression = markerFieldName + " == false";
        Node cleanedNode = source.filter(expression, filterFieldList);

        List<String> retainedFields = new ArrayList<String>();
        for (String fieldName : fieldNames) {
            if (fieldName.equals(markerFieldName)) {
                continue;
            }
            retainedFields.add(fieldName);
        }

        FieldList outputFields = new FieldList(retainedFields);
        cleanedNode = cleanedNode.retain(outputFields);
        return cleanedNode;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return OrbCacheSeedSecondaryDomainCleanupTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "orbCacheSeedCleanedTransformerFlow";
    }

    @Override
    public String getTransformerName() {
        return "orbCacheSeedCleanedTransformer";

    }
}
