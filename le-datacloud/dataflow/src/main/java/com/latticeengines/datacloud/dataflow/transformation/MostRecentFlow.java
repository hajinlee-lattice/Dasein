package com.latticeengines.datacloud.dataflow.transformation;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.OldDataCleanupFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.MostRecentParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(MostRecentFlow.DATAFLOW_BEAN_NAME)
public class MostRecentFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, MostRecentParameters> {

    public final static String DATAFLOW_BEAN_NAME = "mostRecentFlowTransform";
    public final static String TRANSFORMER_NAME = "mostRecentTransformer";

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
    public Node construct(MostRecentParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        return findMostRecent(source, parameters);
    }

    protected Node findMostRecent(Node source, MostRecentParameters parameters) {
        String[] groupbyFields = parameters.getGroupbyFields();
        String timestampField = parameters.getTimestampField();
        String domainField = parameters.getDomainField();
        if (StringUtils.isNotEmpty(domainField)) {
            source = source.apply(new DomainCleanupFunction(domainField, true), new FieldList(domainField),
                    new FieldMetadata(domainField, String.class));
        }
        source = source.apply(new OldDataCleanupFunction(timestampField, parameters.getEarliest()),
                new FieldList(timestampField), new FieldMetadata(timestampField, Long.class));
        return source.groupByAndLimit(new FieldList(groupbyFields), new FieldList(timestampField), 1, true, true);
    }
}
