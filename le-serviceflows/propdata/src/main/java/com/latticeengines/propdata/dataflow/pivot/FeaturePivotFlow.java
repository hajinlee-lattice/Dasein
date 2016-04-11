package com.latticeengines.propdata.dataflow.pivot;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.propdata.dataflow.PivotDataFlowParameters;

@Component("featurePivotFlow")
public class FeaturePivotFlow extends PivotFlow {

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        Node node = super.construct(parameters);
        return node.filter("Execution_Full == null || Execution_Full > 0", new FieldList("Execution_Full"));
    }

}
