package com.latticeengines.propdata.collection.dataflow.pivot;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.pivot.PivotMapper;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class PivotDataFlowParameters extends DataFlowParameters {

    private PivotMapper pivotMapper;
    private DataFlowBuilder.FieldList groupbyFields;

    public PivotMapper getPivotMapper() {
        return pivotMapper;
    }

    public void setPivotMapper(PivotMapper pivotMapper) {
        this.pivotMapper = pivotMapper;
    }

    public DataFlowBuilder.FieldList getGroupbyFields() {
        return groupbyFields;
    }

    public void setGroupbyFields(DataFlowBuilder.FieldList groupbyFields) {
        this.groupbyFields = groupbyFields;
    }
}
