package com.latticeengines.propdata.dataflow.common;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.propdata.dataflow.CountFlowParameters;

@Component("countFlow")
public class CountFlow extends TypesafeDataFlowBuilder<CountFlowParameters> {

    public static final String COUNT = "Count";

    @Override
    public Node construct(CountFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        source = source.addRowID("ID");
        source = source.aggregate(new Aggregation("ID", COUNT, Aggregation.AggregationType.COUNT));
        return source;
    }

}
