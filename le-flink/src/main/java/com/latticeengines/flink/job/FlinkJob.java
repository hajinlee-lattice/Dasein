package com.latticeengines.flink.job;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Properties;

public abstract class FlinkJob {
    protected abstract void connect(ExecutionEnvironment env, Properties properties);
}
