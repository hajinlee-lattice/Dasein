package com.latticeengines.flink.framework.sink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;

public abstract class FlinkSink {

    private int numParallelism;
    private String path;
    private DataSet<?> dataSet;

    public FlinkSink(DataSet<?> dataSet, String pathWithFs, int numParallelism) {
        this.path = pathWithFs;
        this.numParallelism = numParallelism;
        this.dataSet = dataSet;
    }

    public DataSink<?> connect() {
        return createSink(dataSet, path).setParallelism(numParallelism);
    }

    protected abstract DataSink<?> createSink(DataSet<?> dataSet, String pathWithFs);

}
