package com.latticeengines.propdata.dataflow.merge;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.propdata.dataflow.MostRecentDataFlowParameters;

@Component("featureRefreshFlow")
public class FeatureRefreshFlow extends MostRecentFlow {

    @Override
    public Node construct(MostRecentDataFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        source = source.filter("Execution_Full == null || Execution_Full > 0", new FieldList("Execution_Full"));
        return findMostRecent(source, parameters);
    }

}
