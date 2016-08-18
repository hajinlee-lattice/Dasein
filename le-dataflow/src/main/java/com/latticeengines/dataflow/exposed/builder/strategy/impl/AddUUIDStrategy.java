package com.latticeengines.dataflow.exposed.builder.strategy.impl;


import java.util.UUID;

import cascading.tuple.TupleEntry;

public class AddUUIDStrategy extends AddFieldStrategyBase {

    private static final long serialVersionUID = -8734229375756379118L;

    public AddUUIDStrategy(String fieldName) {
        super(fieldName, String.class);
    }

    @Override
    public Object compute(TupleEntry arguments) {
        return UUID.randomUUID().toString();
    }

}
