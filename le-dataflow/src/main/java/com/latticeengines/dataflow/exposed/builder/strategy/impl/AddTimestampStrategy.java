package com.latticeengines.dataflow.exposed.builder.strategy.impl;


import cascading.tuple.TupleEntry;

public class AddTimestampStrategy extends AddFieldStrategyBase {


    private static final long serialVersionUID = -2625708547865490421L;

    public static int UNIFORM = 0;      // all records have the same timestamp
    public static int SEQUENTIAL = 1;   // each record has a slightly different timestamp

    private int mode;
    private long timestamp;

    public AddTimestampStrategy(String fieldName) {
        this(fieldName, UNIFORM);
    }

    public AddTimestampStrategy(String fieldName, int mode) {
        super(fieldName, Long.class);
        this.timestamp = System.currentTimeMillis();
        this.mode = mode;
    }

    @Override
    public Object compute(TupleEntry arguments) {
        if (mode == UNIFORM) {
            return timestamp;
        } else if (mode == SEQUENTIAL) {
            return System.currentTimeMillis();
        } else {
            throw new UnsupportedOperationException("Unknown timestamp mode: " + this.mode);
        }
    }

}
