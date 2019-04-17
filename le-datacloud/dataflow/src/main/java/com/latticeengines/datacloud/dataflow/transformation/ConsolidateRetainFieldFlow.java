package com.latticeengines.datacloud.dataflow.transformation;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateRetainFieldConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("consolidateRetainFieldDataFlow")
public class ConsolidateRetainFieldFlow extends ConsolidateBaseFlow<ConsolidateRetainFieldConfig> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateRetainFieldFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ConsolidateRetainFieldConfig config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        if (CollectionUtils.isEmpty(config.getFieldsToRetain())) {
            return source;
        }
        source = source.retain(new FieldList(config.getFieldsToRetain()));
        log.info("Consolidate fields to retain=", config.getFieldsToRetain());
        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateRetainFieldConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateRetainFieldDataFlow";
    }

    @Override
    public String getTransformerName() {
        return "consolidateRetainFieldTransformer";

    }
}