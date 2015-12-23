package com.latticeengines.propdata.collection.dataflow.pivot;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;

@Component("pivotBaseSource")
public class PivotBaseSource extends TypesafeDataFlowBuilder<PivotDataFlowParameters> {

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        Node source = addSource(parameters.getBaseTableName());
        PivotStrategyImpl pivotStrategy = parameters.getPivotStrategy();
        FieldList groupByField = parameters.getGroupbyFields();
        Node pivot = source.pivot(groupByField, pivotStrategy);
        return pivot.addTimestamp("Timestamp");
    }



}
