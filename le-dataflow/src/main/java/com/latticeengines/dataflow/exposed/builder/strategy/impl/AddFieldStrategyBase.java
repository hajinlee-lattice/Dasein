package com.latticeengines.dataflow.exposed.builder.strategy.impl;


import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.AddFieldStrategy;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public abstract class AddFieldStrategyBase implements AddFieldStrategy {

    private static final long serialVersionUID = -2476364662562861648L;
    private final String fieldName;
    private final Class<?> fieldClass;

    public AddFieldStrategyBase(String fieldName, Class<?> fieldClass) {
        this.fieldName = fieldName;
        this.fieldClass = fieldClass;
    }

    public abstract Object compute(TupleEntry arguments);

    public DataFlowBuilder.FieldMetadata newField() {
        return new DataFlowBuilder.FieldMetadata(fieldName, fieldClass);
    }

    public Fields argumentSelector() { return Fields.ALL; } // by default choose the who tuple as argument

}
