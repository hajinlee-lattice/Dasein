package com.latticeengines.leadprioritization.workflow.steps.pmml;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.UserDefinedType;

public class PivotValuesLookup {

    public Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn;
    public Map<String, List<AbstractMap.Entry<String, String>>> pivotValuesBySourceColumn;
    public Map<String, UserDefinedType> sourceColumnToUserType;
    
    public PivotValuesLookup(Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn ,//
            Map<String, List<AbstractMap.Entry<String, String>>> pivotValuesBySourceColumn, //
            Map<String, UserDefinedType> sourceColumnToUserType) {
        this.pivotValuesByTargetColumn = pivotValuesByTargetColumn;
        this.pivotValuesBySourceColumn = pivotValuesBySourceColumn;
        this.sourceColumnToUserType = sourceColumnToUserType;
    }
    
}
