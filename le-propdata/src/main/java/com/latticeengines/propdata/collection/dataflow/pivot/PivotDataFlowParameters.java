package com.latticeengines.propdata.collection.dataflow.pivot;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class PivotDataFlowParameters extends DataFlowParameters {

    private PivotStrategyImpl pivotStrategy;
    private DataFlowBuilder.FieldList groupbyFields;
    private String baseTableName;

    public PivotStrategyImpl getPivotStrategy() {
        return pivotStrategy;
    }

    public void setPivotStrategy(PivotStrategyImpl pivotStrategy) {
        this.pivotStrategy = pivotStrategy;
    }

    public DataFlowBuilder.FieldList getGroupbyFields() {
        return groupbyFields;
    }

    public void setGroupbyFields(DataFlowBuilder.FieldList groupbyFields) {
        this.groupbyFields = groupbyFields;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }
}
