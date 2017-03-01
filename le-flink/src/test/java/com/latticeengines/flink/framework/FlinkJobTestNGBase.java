package com.latticeengines.flink.framework;

import java.util.Properties;

import org.apache.flink.api.java.ExecutionEnvironment;

import com.latticeengines.flink.FlinkJobProperty;

public abstract class FlinkJobTestNGBase extends FlinkRuntimeTestNGBase {

    private static final String JOB_OUTPUT = "output";

    protected static final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(8);

    private ThreadLocal<String> senarioName = new ThreadLocal<>();

    protected abstract String getJobName();

    protected abstract String getInputResource();

    @Override
    protected String getPackageName() {
        return "job";
    }

    @Deprecated
    @Override
    protected String getTestName() {
        return getJobName();
    }

    private Properties generateProperties() {
        Properties properties = new Properties();
        String fullInput = getResourcePath(getInputResource());
        properties.setProperty(FlinkJobProperty.INPUT, fullInput);
        properties.setProperty(FlinkJobProperty.TARGET_PATH, getOutputDir());
        return properties;
    }

    protected Properties updateProperties(Properties properties) {
        return properties;
    }

}
