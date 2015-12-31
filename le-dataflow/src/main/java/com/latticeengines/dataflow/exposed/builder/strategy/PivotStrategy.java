package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotResult;

import cascading.tuple.TupleEntry;

public interface PivotStrategy extends Serializable {

    PivotResult pivot(TupleEntry arguments);

    Map<String, Object> getDefaultValues();

    List<DataFlowBuilder.FieldMetadata> getFieldMetadataList();

    Set<String> getResultColumns();

}
