package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.TransformationFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.source.ColumnCurationParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;

@Component(ColumnCurationOperationFlow.DATAFLOW_BEAN_NAME)
public class ColumnCurationOperationFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, ColumnCurationParameters> {

    public static final String DATAFLOW_BEAN_NAME = "columnBatchOperationFlow";

    public static final String TRANSFORMER_NAME = "columnBatchTransformer";

    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(ColumnCurationParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        for (Calculation operation : parameters.getColumnOperations()) {
            switch (operation) {
            case MOCK_UP:
                source = mockup(source, parameters.getFields(Calculation.MOCK_UP),
                        parameters.getValues(Calculation.MOCK_UP));
                break;
            case DEPRECATED:
                source = depracate(source, parameters.getFields(Calculation.DEPRECATED));
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Column operation %s is not supported", operation.toString()));
            }
        }
        return source;
    }

    private Node depracate(Node source, List<String> depracateFields) {
        if (CollectionUtils.isNotEmpty(depracateFields)) {
            source = source.discard(new FieldList(depracateFields));
        }
        return source;
    }

    private Node mockup(Node source, List<String> mockupFields, List<String> mockupFieldValues) {
        if (CollectionUtils.isNotEmpty(mockupFields)) {
            for (int i = 0; i < mockupFields.size(); i++) {
                source = source.addColumnWithFixedValue(mockupFields.get(i), mockupFieldValues.get(i), String.class);
            }
        }
        return source;
    }
}
