package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import cascading.tuple.TupleEntry;

public class AddColumnWithFixedValueStrategy extends AddFieldStrategyBase {
    private static final long serialVersionUID = -2625708547865490421L;

    private Object fieldValue;

    public AddColumnWithFixedValueStrategy(String fieldName, Object fieldValue, Class<?> fieldType) {
        super(fieldName, fieldType);
        this.fieldValue = fieldValue;
    }

    @Override
    public Object compute(TupleEntry arguments) {
        return this.fieldValue;
    }

}
