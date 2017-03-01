package com.latticeengines.flink.framework;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;

public abstract class FlinkRuntimeTestNGBase {

    private static final String JOB_OUTPUT = "output";

    protected static final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(8);

    private ThreadLocal<String> senarioName = new ThreadLocal<>();

    protected abstract String getTestName();

    protected String getPackageName() {
        return "runtime";
    }

    protected String getInput() {
        return "input" + File.separator + getPackageName() + File.separator + getTestName();
    }

    protected String getResourcePath(String resourceName) {
        URL url = Thread.currentThread().getContextClassLoader()
                .getResource(getPackageName() + File.separator + getTestName() + File.separator + resourceName);
        if (url == null) {
            throw new IllegalArgumentException(
                    "Cannot find resource named " + resourceName + " for job " + getTestName());
        }
        return url.getFile();
    }

    protected void setSenarioName(String senario) {
        senarioName.set(senario);
    }

    protected String getOutputDir() {
        String senario = senarioName.get();
        if (StringUtils.isBlank(senario)) {
            return JOB_OUTPUT + File.separator + getTestName();
        } else {
            return JOB_OUTPUT + File.separator + getTestName() + File.separator + senario;
        }
    }

    private String getCurrentSenarioName() {
        return senarioName.get();
    }

    protected void execute() {
        String fullJobName = getTestName();
        if (StringUtils.isNotBlank(getCurrentSenarioName())) {
            fullJobName = fullJobName + File.separator + getCurrentSenarioName();
        }
        try {
            FileUtils.deleteDirectory(new File(getOutputDir()));
            env.execute(fullJobName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute flink.", e);
        }
    }

}
