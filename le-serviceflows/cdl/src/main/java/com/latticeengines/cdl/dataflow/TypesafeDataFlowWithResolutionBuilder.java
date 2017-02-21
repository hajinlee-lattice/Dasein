package com.latticeengines.cdl.dataflow;

import java.util.Map;

import org.apache.avro.Schema.Field;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public abstract class TypesafeDataFlowWithResolutionBuilder<T extends DataFlowParameters> extends TypesafeDataFlowBuilder<T> {

    @Override
    protected Node addSource(String sourceTableName, Map<String, Field> allColumns) {
        return super.addSource(sourceTableName, allColumns);
    }
}
