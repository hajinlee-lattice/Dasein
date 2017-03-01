package com.latticeengines.flink.framework.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public abstract class FlinkSource {

    private DataSource<?> source;

    public FlinkSource(ExecutionEnvironment env, String pathWithFs) {
        source = createDataSource(env, pathWithFs);
    }

    public DataSource<?> connect() {
        return source;
    }

    protected abstract DataSource<?> createDataSource(ExecutionEnvironment env, String pathWithFs);

}
