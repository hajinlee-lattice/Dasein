package com.latticeengines.flink.framework.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public abstract class FlinkSource<T> {

    private DataSource<T> source;

    public FlinkSource(ExecutionEnvironment env, String pathWithFs) {
        source = createDataSource(env, pathWithFs);
    }

    public DataSource<T> connect() {
        return source;
    }

    protected abstract DataSource<T> createDataSource(ExecutionEnvironment env, String pathWithFs);

}
