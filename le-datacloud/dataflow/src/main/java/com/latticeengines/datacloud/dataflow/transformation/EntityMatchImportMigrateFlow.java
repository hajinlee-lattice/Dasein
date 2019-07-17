package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.EntityMatchImportMigrateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(EntityMatchImportMigrateFlow.DATAFLOW_BEAN_NAME)
public class EntityMatchImportMigrateFlow extends ConfigurableFlowBase<EntityMatchImportMigrateConfig> {

    public static final String DATAFLOW_BEAN_NAME = "EntityMatchImportMigrateFlow";
    public static final String TRANSFORMER_NAME = "EntityMatchImportMigrateTransformer";

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return EntityMatchImportMigrateConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        EntityMatchImportMigrateConfig config = getTransformerConfig(parameters);

        Node result = addSource(parameters.getBaseTables().get(0));
        if (MapUtils.isNotEmpty(config.getDuplicateMap())) {
            for (Map.Entry<String, String> entry : config.getDuplicateMap().entrySet()) {
                result = result.addStringColumnFromSource(entry.getValue(), entry.getKey());
            }
        }
        if (MapUtils.isNotEmpty(config.getRenameMap())) {
            List<String> previousNames = new ArrayList<>();
            List<String> newNames = new ArrayList<>();
            for (Map.Entry<String, String> entry : config.getRenameMap().entrySet()) {
                previousNames.add(entry.getKey());
                newNames.add(entry.getValue());
            }
            result = result.rename(new FieldList(previousNames), new FieldList(newNames));
        }
        return result;
    }
}
