package com.latticeengines.dataflow.exposed.builder.strategy.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.tuple.TupleEntry;

public class AddTimestampStrategy extends AddFieldStrategyBase {
    private static final long serialVersionUID = -2625708547865490421L;

    private static final Log log = LogFactory.getLog(AddTimestampStrategy.class);

    public static int UNIFORM = 0; // all records have the same timestamp
    public static int SEQUENTIAL = 1; // each record has a slightly different
                                      // timestamp

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

    public AddTimestampStrategy(String fieldName, Date date) {
        super(fieldName, Long.class);
        this.timestamp = date.getTime();
        log.info("Setting timestamp: " //
                + "Epoch Time - " + date.getTime() //
                + ", date - " + date.toGMTString());
        this.mode = UNIFORM;
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
