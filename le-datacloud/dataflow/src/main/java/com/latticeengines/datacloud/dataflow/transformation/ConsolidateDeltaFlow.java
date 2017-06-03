package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDeltaTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("consolidateDeltaDataFlow")
public class ConsolidateDeltaFlow extends ConfigurableFlowBase<ConsolidateDeltaTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateDeltaTransformerConfig config = getTransformerConfig(parameters);

        List<Node> sources = new ArrayList<>();
        List<String> sourceNames = new ArrayList<>();
        for (int i = 0; i < parameters.getBaseTables().size(); i++) {
            String sourceName = parameters.getBaseTables().get(i);
            sources.add(addSource(sourceName));
            sourceNames.add(sourceName);
        }
        if (sources.size() <= 1) {
            return sources.get(0);
        }
        if (sources.size() != 2) {
            throw new RuntimeException("There should be two tables: input and master table!");
        }

        String srcIdField = config.getSrcIdField();
        Node idNode = sources.get(0).retain(new FieldList(srcIdField));
        Node masterNode = sources.get(1);
        List<String> fieldToRetain = masterNode.getFieldNames();

        Node result = idNode.leftJoin(new FieldList(srcIdField), masterNode, new FieldList(srcIdField));
        result = result.retain(new FieldList(fieldToRetain));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateDeltaTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateDeltaDataFlow";
    }

    @Override
    public String getTransformerName() {
        return "consolidateDeltaTransformer";

    }
}