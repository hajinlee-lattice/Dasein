package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceFieldSelectionTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("sourceFieldSelectionFlow")
public class SourceFieldSelectionFlow extends ConfigurableFlowBase<SourceFieldSelectionTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceFieldSelectionTransformerConfig config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> newFields = config.getNewFields();
        Map<String, String> renameFieldMap = config.getRenameFieldMap();
        List<String> retainFields = config.getRetainFields();

        if (CollectionUtils.isNotEmpty(newFields)) {
            for (int i = 0; i < newFields.size(); i++) {
                source = source.addColumnWithFixedValue(newFields.get(i), null, String.class);
            }
        }
        if (renameFieldMap != null && renameFieldMap.size() > 0) {
            for (Map.Entry<String, String> entry : renameFieldMap.entrySet()) {
                source = source.rename(new FieldList(entry.getKey()), new FieldList(entry.getValue()));
            }
        }
        if (CollectionUtils.isNotEmpty(retainFields)) {
            source = source.retain(new FieldList(retainFields));
        }

        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceFieldSelectionTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceFieldSelectionFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceFieldSelectionTransformer";

    }
}