package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import cascading.tuple.TupleEntry;

public class AddStringFieldStrategy extends AddFieldStrategyBase {

    private static final long serialVersionUID = 8111195597091758523L;

    private String source;

    public AddStringFieldStrategy(String fieldName, String source) {
        super(fieldName, String.class);
        this.source = source;
    }

    @Override
    public Object compute(TupleEntry arguments) {
        return arguments.getString(source);
    }
}
