package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.TrimFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TrimConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(TrimFlow.DATAFLOW_BEAN_NAME)
public class TrimFlow extends ConfigurableFlowBase<TrimConfig> {

    public static final String DATAFLOW_BEAN_NAME = "TrimFlow";
    public static final String TRANSFORMER_NAME = "TrimTransformer";

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TrimConfig.class;
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
        TrimConfig config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        Set<String> existingFields = source.getFieldNames().stream().collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(config.getTrimColumns())) {
            for (String trimField : config.getTrimColumns()) {
                if (CollectionUtils.isNotEmpty(existingFields) && existingFields.contains(trimField)) {
                    source = source.apply(new TrimFunction(trimField), new FieldList(trimField),
                            new FieldMetadata(trimField, String.class));
                }
            }
        }
        return source;
    }
}
