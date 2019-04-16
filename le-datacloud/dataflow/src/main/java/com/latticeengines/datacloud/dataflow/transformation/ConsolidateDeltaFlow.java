package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

/**
 * Used by diff step
 */
@Component("consolidateDeltaDataFlow")
public class ConsolidateDeltaFlow extends ConsolidateBaseFlow<ConsolidateDataTransformerConfig> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateDeltaFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDataTransformerConfig config = getTransformerConfig(parameters);

        List<Node> sources = new ArrayList<>();
        List<String> sourceNames = null;
        List<Table> sourceTables = null;

        String masterId = processIdColumns(parameters, config, sources, sourceTables, sourceNames);
        if (sources.size() <= 1) {
            return sources.get(0);
        }
        if (sources.size() != 2) {
            throw new RuntimeException("There should be two tables: input and master table!");
        }
        if (config.isInputLast()) {
            sources = new ArrayList<>(Arrays.asList(sources.get(1), sources.get(0)));
        }
        Node idNode = sources.get(0).retain(new FieldList(masterId));
        Node masterNode = sources.get(1);
        List<String> fieldToRetain = masterNode.getFieldNames();
        Node result = idNode.leftJoin(new FieldList(masterId), masterNode, new FieldList(masterId));
        log.info("Fields to retain=" + fieldToRetain);
        result = result.retain(new FieldList(fieldToRetain));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateDataTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateDeltaDataFlow";
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.TRANSFORMER_CONSOLIDATE_DELTA;

    }
}