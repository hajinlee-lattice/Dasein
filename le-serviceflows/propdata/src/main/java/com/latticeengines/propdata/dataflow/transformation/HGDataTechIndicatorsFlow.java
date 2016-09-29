package com.latticeengines.propdata.dataflow.transformation;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.propdata.dataflow.common.BitEncodeUtils;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BasicTransformationConfiguration;

@Component("hgDataTechIndicatorsFlow")
public class HGDataTechIndicatorsFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    private static String[] groupByFields = new String[] { "Domain" };

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node source = addSource("HGDataClean");
        List<SourceColumn> sourceColumns = parameters.getColumns();
        Node encoded = BitEncodeUtils.encode(source, groupByFields, sourceColumns);
        return encoded.addTimestamp("Timestamp");
    }

}
