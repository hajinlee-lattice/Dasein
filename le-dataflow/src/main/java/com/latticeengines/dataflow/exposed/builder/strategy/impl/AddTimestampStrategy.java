package com.latticeengines.dataflow.exposed.builder.strategy.impl;


import cascading.tuple.TupleEntry;

public class AddTimestampStrategy extends AddFieldStrategyBase {


    private static final long serialVersionUID = -2625708547865490421L;

    public AddTimestampStrategy(String fieldName) {
        super(fieldName, Long.class);
    }

    @Override
    public Object compute(TupleEntry arguments) {
        return System.currentTimeMillis();
    }

}
