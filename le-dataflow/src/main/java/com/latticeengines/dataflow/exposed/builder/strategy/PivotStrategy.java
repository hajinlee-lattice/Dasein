package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.tuple.TupleEntry;

import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotResult;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public interface PivotStrategy extends Serializable {

    List<PivotResult> pivot(TupleEntry arguments);

    Map<String, Object> getDefaultValues();

    List<FieldMetadata> getFieldMetadataList();

    Set<String> getResultColumns();

}
