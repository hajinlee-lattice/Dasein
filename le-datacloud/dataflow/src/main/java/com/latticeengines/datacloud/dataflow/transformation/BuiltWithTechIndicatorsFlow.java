package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.BitEncodeUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component("builtWithTechIndicatorsFlow")
public class BuiltWithTechIndicatorsFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    private static String[] groupByFields = new String[] { "Domain" };

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node source = addSource("BuiltWithMostRecent");
        List<SourceColumn> sourceColumns = parameters.getColumns();
        Node encoded = BitEncodeUtils.encode(source, groupByFields, sourceColumns);
        return encoded.addTimestamp("Timestamp");
    }

}
