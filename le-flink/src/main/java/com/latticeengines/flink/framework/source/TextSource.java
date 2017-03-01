package com.latticeengines.flink.framework.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class TextSource extends FlinkSource<String> {

    public TextSource(ExecutionEnvironment env, String pathWithFs) {
        super(env, pathWithFs);
    }

    @Override
    protected DataSource<String> createDataSource(ExecutionEnvironment env, String pathWithFs) {
        return env.readTextFile(pathWithFs);
    }

}


